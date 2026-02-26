import os
import re
import math
import html
import requests
import certifi
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import (
    Application,
    MessageHandler,
    CommandHandler,
    CallbackQueryHandler,
    filters,
    ContextTypes,
)

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

WIN_MULTIPLIER = 2.0
MIN_CALLS_REQUIRED = 1
MAX_CALL_DELAY_SECONDS = 120

if not TOKEN or not MONGO_URI:
    raise ValueError("Missing TELEGRAM_TOKEN or MONGO_URI environment variables")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["yabai_crypto_bot"]

calls_collection = db["token_calls"]
settings_collection = db["group_settings"]
user_profiles_collection = db["user_profiles"]

CA_REGEX = r"\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b"


def ensure_indexes():
    calls_collection.create_index([
        ("chat_id", ASCENDING),
        ("ca_norm", ASCENDING),
        ("status", ASCENDING),
    ])
    calls_collection.create_index([("chat_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_collection.create_index([("chat_id", ASCENDING), ("caller_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_collection.create_index([("message_id", ASCENDING), ("chat_id", ASCENDING)])

    user_profiles_collection.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
    settings_collection.create_index([("chat_id", ASCENDING)], unique=True)


def utc_now():
    return datetime.now(timezone.utc)


def normalize_ca(ca: str) -> str:
    return ca.strip().lower()


def accepted_call_filter(chat_id: int):
    return {
        "chat_id": chat_id,
        "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
    }


def clamp(value, low, high):
    return max(low, min(high, value))


def short_ca(ca):
    if not ca:
        return "N/A"
    if len(ca) <= 12:
        return ca
    return f"{ca[:6]}...{ca[-4:]}"


def format_return(x_value):
    x_value = float(x_value or 0.0)
    if x_value >= 2.0:
        return f"{x_value:.2f}x"
    pct = (x_value - 1.0) * 100.0
    if abs(pct) < 0.05:
        pct = 0.0
    return f"{pct:.1f}%"


def token_label(symbol, ca):
    symbol = (symbol or "").strip()
    if symbol:
        return f"${symbol.upper()}"
    return short_ca(ca)


def get_dexscreener_batch_meta(cas_list):
    results = {}
    if not cas_list:
        return results

    for i in range(0, len(cas_list), 30):
        chunk = cas_list[i:i + 30]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(chunk)}"
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            if payload and payload.get("pairs"):
                for pair in payload["pairs"]:
                    address = pair.get("baseToken", {}).get("address")
                    symbol = pair.get("baseToken", {}).get("symbol") or ""
                    fdv = pair.get("fdv", 0)
                    if address and fdv and fdv > 0:
                        addr_lower = address.lower()
                        if addr_lower not in results or fdv > results[addr_lower]["fdv"]:
                            results[addr_lower] = {
                                "fdv": float(fdv),
                                "symbol": symbol.upper() if symbol else "",
                            }
        except Exception as exc:
            print(f"DexScreener batch fetch error: {exc}")
    return results


def get_dexscreener_batch(cas_list):
    meta = get_dexscreener_batch_meta(cas_list)
    return {addr: data["fdv"] for addr, data in meta.items()}


def update_user_profile(chat_id, user, event_type, reason=None):
    update_doc = {
        "$setOnInsert": {
            "chat_id": chat_id,
            "user_id": user.id,
            "first_seen": utc_now(),
        },
        "$set": {
            "display_name": user.full_name or user.first_name or "Unknown",
            "username": user.username,
            "updated_at": utc_now(),
        },
    }

    if event_type == "accepted":
        update_doc.setdefault("$inc", {})["accepted_calls"] = 1
        update_doc["$set"]["last_accepted_at"] = utc_now()
    elif event_type == "rejected":
        update_doc.setdefault("$inc", {})["rejected_calls"] = 1
        if reason:
            field = f"reject_reasons.{reason}"
            update_doc.setdefault("$inc", {})[field] = 1

    user_profiles_collection.update_one(
        {"chat_id": chat_id, "user_id": user.id},
        update_doc,
        upsert=True,
    )


def derive_user_metrics(calls):
    returns_now = []
    returns_ath = []
    wins = 0
    profitable_now = 0
    best_x = 0.0

    for call in calls:
        initial = float(call.get("initial_mcap", 0) or 0)
        current = float(call.get("current_mcap", initial) or initial)
        ath = float(max(call.get("ath_mcap", initial) or initial, current))
        if initial <= 0:
            continue

        x_now = current / initial
        x_ath = ath / initial
        ret_now = x_now - 1.0
        ret_ath = x_ath - 1.0

        returns_now.append(ret_now)
        returns_ath.append(ret_ath)
        best_x = max(best_x, x_ath)

        if x_ath >= WIN_MULTIPLIER:
            wins += 1
        if x_now > 1.0:
            profitable_now += 1

    n = len(returns_now)
    if n == 0:
        return {
            "calls": 0,
            "avg_now": 0.0,
            "avg_ath": 0.0,
            "win_rate": 0.0,
            "profitable_rate": 0.0,
            "reputation": 0.0,
            "best_x": 0.0,
            "badges": [],
        }

    avg_now = sum(returns_now) / n
    avg_ath = sum(returns_ath) / n
    win_rate = wins / n
    profitable_rate = profitable_now / n
    profitability = clamp((avg_now + 1.0) / 2.0, 0.0, 1.0)
    upside_norm = clamp((avg_ath + 1.0) / 3.0, 0.0, 1.0)
    sample_conf = clamp(math.log1p(n) / math.log(25), 0.0, 1.0)

    reputation = 100.0 * (
        0.40 * win_rate
        + 0.30 * profitability
        + 0.20 * upside_norm
        + 0.10 * sample_conf
    )
    reputation = clamp(reputation, 0.0, 100.0)

    badges = []
    if best_x >= 100.0:
        badges.append("100x Legend")
    elif best_x >= 25.0:
        badges.append("Moonshot")
    elif best_x >= 10.0:
        badges.append("Sniper")
    if n >= 10 and win_rate >= 0.60:
        badges.append("High Hit Rate")
    if n >= 5 and avg_now > 0:
        badges.append("Profitable")

    return {
        "calls": n,
        "avg_now": avg_now,
        "avg_ath": avg_ath,
        "win_rate": win_rate,
        "profitable_rate": profitable_rate,
        "reputation": reputation,
        "best_x": best_x,
        "badges": badges,
    }


def call_is_duplicate(chat_id, ca_norm):
    existing = calls_collection.find_one(
        {
            "chat_id": chat_id,
            "ca_norm": ca_norm,
            "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
        },
        {"_id": 1},
    )
    return existing is not None


def get_caller_key(call_doc):
    caller_id = call_doc.get("caller_id")
    if caller_id is not None:
        return f"id:{caller_id}"
    legacy_name = (call_doc.get("caller_name") or "unknown").strip().lower()
    return f"legacy:{legacy_name}"


def get_reputation_penalty(chat_id, caller_id):
    if caller_id is None:
        return 0.0
    profile = user_profiles_collection.find_one(
        {"chat_id": chat_id, "user_id": caller_id},
        {"rejected_calls": 1},
    ) or {}
    rejected_calls = int(profile.get("rejected_calls", 0) or 0)
    return min(15.0, rejected_calls * 0.5)


async def toggle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    setting = settings_collection.find_one({"chat_id": chat_id}) or {}

    if not setting.get("alerts", False):
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.effective_message.reply_text("Alerts ON: I will announce every new tracked CA here.")
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.effective_message.reply_text("Alerts OFF: I will track CAs silently here.")


async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message_obj = update.effective_message
    if not message_obj or not message_obj.text:
        return

    text = message_obj.text
    user = update.effective_user
    chat_id = update.effective_chat.id

    found_cas = {normalize_ca(ca) for ca in re.findall(CA_REGEX, text)}
    if not found_cas:
        return

    is_edited = update.edited_message is not None

    msg_time = message_obj.date
    now = utc_now()
    if msg_time.tzinfo is None:
        msg_time = msg_time.replace(tzinfo=timezone.utc)
    delay_seconds = max(0, int((now - msg_time).total_seconds()))

    for ca_norm in found_cas:
        rejection_reason = None

        if is_edited:
            rejection_reason = "edited_message"
        elif delay_seconds > MAX_CALL_DELAY_SECONDS:
            rejection_reason = "late_submission"
        elif call_is_duplicate(chat_id, ca_norm):
            rejection_reason = "duplicate_ca"

        if rejection_reason:
            calls_collection.insert_one(
                {
                    "chat_id": chat_id,
                    "status": "rejected",
                    "reject_reason": rejection_reason,
                    "ca": ca_norm,
                    "ca_norm": ca_norm,
                    "caller_id": user.id,
                    "caller_name": user.full_name or user.first_name or "Unknown",
                    "caller_username": user.username,
                    "message_id": message_obj.message_id,
                    "message_date": msg_time,
                    "timestamp": now,
                    "ingest_delay_seconds": delay_seconds,
                }
            )
            update_user_profile(chat_id, user, "rejected", reason=rejection_reason)
            continue

        batch_data = get_dexscreener_batch_meta([ca_norm])
        token_meta = batch_data.get(ca_norm, {})
        mcap = token_meta.get("fdv")
        symbol = token_meta.get("symbol", "")

        if mcap and mcap > 0:
            call_data = {
                "chat_id": chat_id,
                "status": "accepted",
                "ca": ca_norm,
                "ca_norm": ca_norm,
                "caller_id": user.id,
                "caller_name": user.full_name or user.first_name or "Unknown",
                "caller_username": user.username,
                "initial_mcap": mcap,
                "ath_mcap": mcap,
                "current_mcap": mcap,
                "token_symbol": symbol,
                "timestamp": now,
                "message_id": message_obj.message_id,
                "message_date": msg_time,
                "ingest_delay_seconds": delay_seconds,
            }
            calls_collection.insert_one(call_data)
            update_user_profile(chat_id, user, "accepted")

            setting = settings_collection.find_one({"chat_id": chat_id}) or {}
            if setting.get("alerts", False):
                await message_obj.reply_text(
                    f"New call tracked\nToken: {token_label(symbol, ca_norm)}\nCA: {ca_norm}\nCaller: {user.first_name}\nEntry MCAP: ${mcap:,.2f}"
                )


def _resolve_time_filter(context: ContextTypes.DEFAULT_TYPE):
    query = {}
    time_text = "All Time"
    if not context.args:
        return query, time_text

    time_arg = context.args[0].lower()
    try:
        if time_arg.endswith("d"):
            days = int(time_arg[:-1])
            cutoff = utc_now() - timedelta(days=days)
            query["timestamp"] = {"$gte": cutoff}
            time_text = f"Last {days} Days"
        elif time_arg.endswith("h"):
            hours = int(time_arg[:-1])
            cutoff = utc_now() - timedelta(hours=hours)
            query["timestamp"] = {"$gte": cutoff}
            time_text = f"Last {hours} Hours"
    except ValueError:
        pass

    return query, time_text


def refresh_calls_market_data(calls):
    unique_cas = list({call.get("ca_norm", normalize_ca(call["ca"])) for call in calls if call.get("ca")})
    latest_meta = get_dexscreener_batch_meta(unique_cas)

    for call in calls:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        meta = latest_meta.get(ca_norm, {})
        current_mcap = meta.get("fdv", call.get("current_mcap", call.get("initial_mcap", 0)))
        if not current_mcap:
            continue
        ath = max(float(call.get("ath_mcap", current_mcap)), float(current_mcap))
        update_fields = {"current_mcap": current_mcap, "ath_mcap": ath}
        if meta.get("symbol"):
            update_fields["token_symbol"] = meta["symbol"]
        calls_collection.update_one({"_id": call["_id"]}, {"$set": update_fields})
        call["current_mcap"] = current_mcap
        call["ath_mcap"] = ath
        if meta.get("symbol"):
            call["token_symbol"] = meta["symbol"]


async def _fetch_and_calculate_rankings(update: Update, context: ContextTypes.DEFAULT_TYPE, is_bottom=False):
    chat_id = update.effective_chat.id
    base_filter = accepted_call_filter(chat_id)
    time_filter, time_text = _resolve_time_filter(context)
    query = {**base_filter, **time_filter}

    list_type = "Wall of Shame" if is_bottom else "Leaderboard"
    status_message = await update.effective_message.reply_text(f"Fetching {time_text} {list_type}...")

    all_calls = list(calls_collection.find(query))
    if not all_calls:
        await status_message.edit_text(f"No data for {time_text} in this group")
        return

    refresh_calls_market_data(all_calls)

    user_calls = {}
    for call in all_calls:
        caller_key = get_caller_key(call)
        user_calls.setdefault(caller_key, []).append(call)

    leaderboard_data = []
    for caller_key, calls in user_calls.items():
        metrics = derive_user_metrics(calls)
        if metrics["calls"] < MIN_CALLS_REQUIRED:
            continue

        caller_id = calls[0].get("caller_id")
        caller_name = calls[0].get("caller_name", "Unknown")
        penalty = get_reputation_penalty(chat_id, caller_id)
        score = max(0.0, metrics["reputation"] - penalty)

        leaderboard_data.append(
            {
                "caller_key": caller_key,
                "caller_id": caller_id,
                "name": caller_name,
                "calls": metrics["calls"],
                "avg_ath_x": 1.0 + metrics["avg_ath"],
                "avg_now_x": 1.0 + metrics["avg_now"],
                "best_x": metrics["best_x"],
                "win_rate": metrics["win_rate"] * 100,
                "profitable_rate": metrics["profitable_rate"] * 100,
                "score": score,
            }
        )

    if not leaderboard_data:
        await status_message.edit_text(f"No one has reached the minimum {MIN_CALLS_REQUIRED} calls to be ranked")
        return

    if is_bottom:
        leaderboard_data.sort(key=lambda x: (x["score"], x["avg_now_x"]))
        title = f"Wall of Shame ({time_text})"
    else:
        leaderboard_data.sort(key=lambda x: (x["score"], x["avg_now_x"], x["calls"]), reverse=True)
        title = f"Yabai Callers ({time_text})"

    context.chat_data["leaderboard_title"] = title
    context.chat_data["leaderboard_data"] = leaderboard_data

    await render_leaderboard_page(status_message, context, page=0)


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _fetch_and_calculate_rankings(update, context, is_bottom=False)


async def bottom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    await _fetch_and_calculate_rankings(update, context, is_bottom=True)


async def season(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        context.args = ["30d"]
    await _fetch_and_calculate_rankings(update, context, is_bottom=False)


async def render_leaderboard_page(message_obj, context, page=0):
    data = context.chat_data.get("leaderboard_data", [])
    title = context.chat_data.get("leaderboard_title", "Leaderboard")

    items_per_page = 10
    total_pages = max(1, math.ceil(len(data) / items_per_page))

    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_data = data[start_idx:end_idx]

    lines = [f"🏆 {title} 🏆", f"Page {page + 1}/{total_pages}", ""]

    for idx, row in enumerate(page_data, start=start_idx + 1):
        lines.append(
            f"{idx}. {row['name']} ({row['calls']} calls)\n"
            f"   📈 Avg: {format_return(row['avg_now_x'])} | 🔥 Best: {format_return(row['best_x'])}\n"
            f"   🎯 Win Rate: {row['win_rate']:.1f}%"
        )
        lines.append("")

    text = "\n".join(lines).strip()

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("Prev", callback_data=f"lb_{page-1}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next", callback_data=f"lb_{page+1}"))

    reply_markup = InlineKeyboardMarkup([buttons]) if buttons else None

    try:
        await message_obj.edit_text(text, reply_markup=reply_markup)
    except Exception:
        pass


async def paginate_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[1])
    if "leaderboard_data" in context.chat_data:
        await render_leaderboard_page(query.message, context, page)
    else:
        await query.message.edit_text("Data expired. Run the command again.")


async def caller_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Provide a name or @username. Example: /caller John")
        return

    target = " ".join(context.args).replace("@", "")
    chat_id = update.effective_chat.id

    query = {
        "chat_id": chat_id,
        "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
        "$and": [
            {
                "$or": [
                    {"caller_name": {"$regex": f"^{re.escape(target)}$", "$options": "i"}},
                    {"caller_username": {"$regex": f"^{re.escape(target)}$", "$options": "i"}},
                ]
            }
        ],
    }

    all_user_calls = list(calls_collection.find(query).sort("timestamp", -1))
    if not all_user_calls:
        await update.effective_message.reply_text(f"No calls found for '{target}' in this group")
        return

    refresh_calls_market_data(all_user_calls)
    metrics = derive_user_metrics(all_user_calls)

    recent_calls = all_user_calls[:5]
    actual_name = recent_calls[0].get("caller_name", "Unknown")
    win_pct = metrics["win_rate"] * 100
    recent_cas_norm = [c.get("ca_norm", normalize_ca(c.get("ca", ""))) for c in recent_calls if c.get("ca")]
    recent_meta = get_dexscreener_batch_meta(recent_cas_norm)
    avg_text = format_return(1 + metrics["avg_now"])
    best_text = format_return(metrics["best_x"])

    lines = [
        f"👤 Caller Profile: {html.escape(actual_name)}",
        f"📞 Total Calls: {metrics['calls']}",
        f"📈 Avg: {avg_text} | 🔥 Best: {best_text}",
        f"🎯 Win Rate (>= {WIN_MULTIPLIER:.1f}x): {win_pct:.1f}%",
        f"🏅 Badges: {html.escape(', '.join(metrics['badges']) if metrics['badges'] else 'None')}",
        "",
        "Recent Calls:",
    ]

    for call in recent_calls:
        ca = call.get("ca", "")
        initial = float(call.get("initial_mcap", 0) or 0)
        current = float(call.get("current_mcap", initial) or initial)
        ath = float(call.get("ath_mcap", current) or current)
        if initial <= 0:
            continue
        call_date = call.get("timestamp", utc_now()).strftime("%Y-%m-%d")
        ca_norm = call.get("ca_norm", normalize_ca(ca))
        symbol = recent_meta.get(ca_norm, {}).get("symbol") or call.get("token_symbol", "")
        token = token_label(symbol, ca)
        lines.append(
            f"• {html.escape(token)} ({call_date})\n"
            f"   📈 ATH: {format_return(ath / initial)} | 💰 Now: {format_return(current / initial)}\n"
            f"   <code>{html.escape(ca)}</code>"
        )

    await update.effective_message.reply_text("\n".join(lines), parse_mode="HTML")


async def my_score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    user = update.effective_user

    user_calls = list(
        calls_collection.find(
            {
                "chat_id": chat_id,
                "caller_id": user.id,
                "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
            }
        )
    )

    if not user_calls:
        await update.effective_message.reply_text("You do not have tracked calls yet.")
        return

    refresh_calls_market_data(user_calls)
    metrics = derive_user_metrics(user_calls)

    penalty = get_reputation_penalty(chat_id, user.id)
    win_pct = metrics["win_rate"] * 100
    score = max(0.0, metrics["reputation"] - penalty)

    text = (
        f"📈 Your Performance\n\n"
        f"📞 Total Calls: {metrics['calls']}\n"
        f"📈 Avg: {format_return(1 + metrics['avg_now'])} | 🔥 Best: {format_return(metrics['best_x'])}\n"
        f"🎯 Win Rate (>= {WIN_MULTIPLIER:.1f}x): {win_pct:.1f}%\n"
        f"⭐ Score: {score:.1f}/100\n"
        f"Badges: {', '.join(metrics['badges']) if metrics['badges'] else 'None'}"
    )
    await update.effective_message.reply_text(text)


async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    status_message = await update.effective_message.reply_text("Analyzing group performance...")

    all_calls = list(calls_collection.find(accepted_call_filter(chat_id)))
    if not all_calls:
        await status_message.edit_text("No calls tracked in this group yet")
        return

    refresh_calls_market_data(all_calls)

    total_calls = len(all_calls)
    unique_callers = set(get_caller_key(call) for call in all_calls)

    group_metrics = derive_user_metrics(all_calls)

    best_call = max(
        all_calls,
        key=lambda c: (float(c.get("ath_mcap", 0) or 0) / float(c.get("initial_mcap", 1) or 1)),
        default=None,
    )

    best_text = "N/A"
    if best_call:
        initial = float(best_call.get("initial_mcap", 1) or 1)
        best_x = float(best_call.get("ath_mcap", initial) or initial) / initial
        ca = best_call.get("ca", "")
        ca_norm = best_call.get("ca_norm", normalize_ca(ca))
        best_meta = get_dexscreener_batch_meta([ca_norm]).get(ca_norm, {})
        symbol = best_meta.get("symbol") or best_call.get("token_symbol", "")
        token = token_label(symbol, ca)
        best_caller = best_call.get("caller_name", "Unknown")
        best_text = format_return(best_x)
        best_by_text = f"   └ By {html.escape(best_caller)} ({html.escape(token)})\n   <code>{html.escape(ca)}</code>"
    else:
        best_by_text = "   └ By N/A"

    text = (
        f"📊 Group Performance Overview 📊\n\n"
        f"👥 Total Callers: {len(unique_callers)}\n"
        f"📞 Total Calls Tracked: {total_calls}\n"
        f"🎯 Group Win Rate (>= {WIN_MULTIPLIER:.1f}x): {group_metrics['win_rate'] * 100:.1f}%\n\n"
        f"📈 Group Average: {format_return(1 + group_metrics['avg_now'])}\n"
        f"🔥 Best Call All-Time: {best_text}\n"
        f"{best_by_text}"
    )

    await status_message.edit_text(text, parse_mode="HTML")


async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.effective_message.reply_text("Provide a CA. Example: /stats <CA>")
        return

    ca = normalize_ca(context.args[0])
    chat_id = update.effective_chat.id

    call = calls_collection.find_one(
        {
            "chat_id": chat_id,
            "ca_norm": ca,
            "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
        }
    )

    if not call:
        call = calls_collection.find_one(
            {
                "chat_id": chat_id,
                "ca": ca,
                "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
            }
        )

    if not call:
        await update.effective_message.reply_text("This CA is not tracked in this group yet")
        return

    batch_data = get_dexscreener_batch_meta([ca])
    token_meta = batch_data.get(ca, {})
    current_mcap = token_meta.get("fdv")
    symbol = token_meta.get("symbol") or call.get("token_symbol", "")

    if not current_mcap:
        await update.effective_message.reply_text("Failed to fetch current data from DexScreener")
        return

    ath = max(float(call.get("ath_mcap", current_mcap) or current_mcap), float(current_mcap))
    update_fields = {"current_mcap": current_mcap, "ath_mcap": ath}
    if symbol:
        update_fields["token_symbol"] = symbol
    calls_collection.update_one({"_id": call["_id"]}, {"$set": update_fields})

    initial = float(call.get("initial_mcap", 1) or 1)
    current_x = current_mcap / initial
    ath_x = ath / initial

    text = (
        f"🪙 Token: {html.escape(token_label(symbol, ca))}\n"
        f"<code>{html.escape(ca)}</code>\n\n"
        f"👤 Called by: {html.escape(call.get('caller_name', 'Unknown'))}\n"
        f"📈 ATH: {format_return(ath_x)}\n"
        f"💰 Current: {format_return(current_x)}\n"
        f"🏁 Entry MCAP: ${initial:,.2f}"
    )
    await update.effective_message.reply_text(text, parse_mode="HTML")


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    member = await context.bot.get_chat_member(chat.id, user.id)
    if member.status not in {"administrator", "creator"}:
        await msg.reply_text("Admin only command")
        return

    base = {"chat_id": chat.id}
    accepted = calls_collection.count_documents({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]})
    rejected = calls_collection.count_documents({**base, "status": "rejected"})

    reason_counts = list(
        calls_collection.aggregate(
            [
                {"$match": {**base, "status": "rejected"}},
                {"$group": {"_id": "$reject_reason", "count": {"$sum": 1}}},
                {"$sort": {"count": -1}},
            ]
        )
    )

    suspicious = list(
        user_profiles_collection.find({"chat_id": chat.id})
        .sort("rejected_calls", -1)
        .limit(5)
    )

    delay_pipeline = [
        {
            "$match": {
                **base,
                "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
                "ingest_delay_seconds": {"$exists": True},
            }
        },
        {
            "$group": {
                "_id": None,
                "avg_delay": {"$avg": "$ingest_delay_seconds"},
                "max_delay": {"$max": "$ingest_delay_seconds"},
            }
        },
    ]
    delay_stats = list(calls_collection.aggregate(delay_pipeline))
    avg_delay = delay_stats[0]["avg_delay"] if delay_stats else 0
    max_delay = delay_stats[0]["max_delay"] if delay_stats else 0

    recent_calls = list(calls_collection.find({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]}))
    user_calls = {}
    for call in recent_calls:
        key = get_caller_key(call)
        user_calls.setdefault(key, []).append(call)

    low_performers = []
    for _, user_call_set in user_calls.items():
        m = derive_user_metrics(user_call_set)
        if m["calls"] < 3:
            continue
        low_performers.append(
            {
                "name": user_call_set[0].get("caller_name", "Unknown"),
                "calls": m["calls"],
                "win_rate": m["win_rate"] * 100,
                "avg_now_x": 1 + m["avg_now"],
            }
        )
    low_performers.sort(key=lambda x: (x["win_rate"], x["avg_now_x"]))

    lines = [
        "🛡️ Admin Panel",
        f"Accepted: {accepted} | Rejected: {rejected}",
        f"Acceptance: {(accepted / (accepted + rejected) * 100) if (accepted + rejected) else 0:.1f}%",
        f"Delay avg/max: {avg_delay:.1f}s / {max_delay:.0f}s",
        "",
        "Reject Reasons",
    ]

    if reason_counts:
        for row in reason_counts[:5]:
            lines.append(f"- {row['_id'] or 'unknown'}: {row['count']}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Spam Watchlist")
    if suspicious:
        for row in suspicious:
            name = row.get("display_name", "Unknown")
            rej = row.get("rejected_calls", 0)
            acc = row.get("accepted_calls", 0)
            if rej > 0:
                lines.append(f"- {name}: rejected {rej}, accepted {acc}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("Low Performers (>=3 calls)")
    if low_performers:
        for row in low_performers[:5]:
            lines.append(
                f"- {row['name']}: win {row['win_rate']:.1f}%, avg {format_return(row['avg_now_x'])}, calls {row['calls']}"
            )
    else:
        lines.append("- None")

    await msg.reply_text("\n".join(lines))


def main():
    ensure_indexes()

    app = Application.builder().token(TOKEN).build()

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_ca))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & filters.TEXT & ~filters.COMMAND, track_ca))

    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("bottom", bottom))
    app.add_handler(CommandHandler("season", season))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("togglealerts", toggle_alerts))
    app.add_handler(CommandHandler("caller", caller_profile))
    app.add_handler(CommandHandler("groupstats", group_stats))
    app.add_handler(CommandHandler("myscore", my_score))
    app.add_handler(CommandHandler("adminstats", admin_stats))

    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern=r"^lb_"))

    print("YabaiRankBot running")
    app.run_polling()


if __name__ == "__main__":
    main()
