import os
import re
import math
import html
import json
import asyncio
import requests
import certifi
from urllib.parse import quote
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
HEARTBEAT_INTERVAL_SECONDS = 600
HOT_STREAK_MIN = 4
COLD_STREAK_MIN = 6
STREAK_LOOKBACK = 8
ACTIVE_CALL_WINDOW_HOURS = 1
ALERT_COOLDOWN_HOURS = 4
DIGEST_HOUR_UTC = 12
RUG_ATH_MAX_X = 1.20
RUG_CURRENT_MAX_X = 0.30
RUG_MIN_AGE_HOURS = 12

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


def rank_badge(rank):
    if rank == 1:
        return "🥇"
    if rank == 2:
        return "🥈"
    if rank == 3:
        return "🥉"
    if 4 <= rank <= 10:
        return f"{rank}️⃣"
    return f"{rank}."


def stars_from_pct(pct):
    stars = int(clamp(round(float(pct or 0.0) / 20.0), 0, 5))
    return ("★" * stars) + ("☆" * (5 - stars))


def format_return(x_value):
    if isinstance(x_value, str):
        raw = x_value.strip().lower()
        try:
            if raw.endswith("x"):
                x_value = float(raw[:-1])
            elif raw.endswith("%"):
                x_value = 1.0 + (float(raw[:-1]) / 100.0)
            else:
                x_value = float(raw)
        except ValueError:
            x_value = 0.0
    else:
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


def quickchart_url(chart_config):
    payload = quote(json.dumps(chart_config, separators=(",", ":")))
    return f"https://quickchart.io/chart?c={payload}"


def build_performance_chart_url(title, win_rate_pct, profitable_pct, avg_x):
    avg_return_pct = (float(avg_x) - 1.0) * 100.0
    chart = {
        "type": "bar",
        "data": {
            "labels": ["Win Rate %", "Profitable %", "Avg Return %"],
            "datasets": [
                {
                    "label": "Performance",
                    "backgroundColor": ["#38bdf8", "#4ade80", "#f59e0b"],
                    "data": [
                        round(float(win_rate_pct), 2),
                        round(float(profitable_pct), 2),
                        round(avg_return_pct, 2),
                    ],
                }
            ],
        },
        "options": {
            "plugins": {
                "title": {"display": True, "text": title},
                "legend": {"display": False},
            },
            "scales": {
                "y": {"beginAtZero": True},
            },
        },
    }
    return quickchart_url(chart)


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


def derive_rug_stats(calls):
    eligible = 0
    rug_count = 0
    now = utc_now()

    for call in calls:
        initial = float(call.get("initial_mcap", 0) or 0)
        if initial <= 0:
            continue

        ts = call.get("timestamp")
        if not ts:
            continue
        if ts.tzinfo is None:
            ts = ts.replace(tzinfo=timezone.utc)
        age_hours = (now - ts).total_seconds() / 3600.0
        if age_hours < RUG_MIN_AGE_HOURS:
            continue

        current = float(call.get("current_mcap", initial) or initial)
        ath = float(max(call.get("ath_mcap", initial) or initial, current))
        ath_x = ath / initial
        current_x = current / initial

        eligible += 1
        if ath_x < RUG_ATH_MAX_X and current_x <= RUG_CURRENT_MAX_X:
            rug_count += 1

    rug_rate = (rug_count / eligible) * 100.0 if eligible > 0 else 0.0
    return {"rug_rate": rug_rate, "rug_count": rug_count, "eligible": eligible}


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


def get_tracked_chat_ids():
    settings_ids = settings_collection.distinct("chat_id")
    call_ids = calls_collection.distinct("chat_id")
    ids = set(settings_ids or []) | set(call_ids or [])
    return [chat_id for chat_id in ids if chat_id is not None]


def is_win_call(call_doc):
    initial = float(call_doc.get("initial_mcap", 0) or 0)
    if initial <= 0:
        return False
    ath = float(call_doc.get("ath_mcap", initial) or initial)
    current = float(call_doc.get("current_mcap", initial) or initial)
    return (max(ath, current) / initial) >= WIN_MULTIPLIER


def is_loss_call(call_doc):
    initial = float(call_doc.get("initial_mcap", 0) or 0)
    if initial <= 0:
        return False
    current = float(call_doc.get("current_mcap", initial) or initial)
    return (current / initial) < 1.0


def consecutive_count(values):
    streak = 0
    for value in values:
        if value:
            streak += 1
        else:
            break
    return streak


def _hours_since(dt):
    if not dt:
        return None
    if dt.tzinfo is None:
        dt = dt.replace(tzinfo=timezone.utc)
    return (utc_now() - dt).total_seconds() / 3600.0


async def user_is_admin(bot, chat_id, user_id):
    member = await bot.get_chat_member(chat_id, user_id)
    return member.status in {"administrator", "creator"}


def _accepted_query(chat_id, extra=None):
    query = accepted_call_filter(chat_id)
    if extra:
        query = {**query, **extra}
    return query


async def run_streak_scan_for_chat(bot, chat_id, manual=False):
    setting = settings_collection.find_one({"chat_id": chat_id}) or {}
    if not manual and not setting.get("alerts", False):
        return 0

    cutoff = utc_now() - timedelta(hours=ACTIVE_CALL_WINDOW_HOURS)
    active_calls = list(
        calls_collection.find(
            _accepted_query(chat_id, {"timestamp": {"$gte": cutoff}, "caller_id": {"$ne": None}})
        )
    )
    if not active_calls:
        return 0

    active_user_ids = sorted({call.get("caller_id") for call in active_calls if call.get("caller_id") is not None})
    triggered = 0

    for user_id in active_user_ids:
        recent_calls = list(
            calls_collection.find(_accepted_query(chat_id, {"caller_id": user_id}))
            .sort("timestamp", -1)
            .limit(STREAK_LOOKBACK)
        )
        if not recent_calls:
            continue

        latest_ts = recent_calls[0].get("timestamp")
        if latest_ts and latest_ts.tzinfo is None:
            latest_ts = latest_ts.replace(tzinfo=timezone.utc)
        if not latest_ts or latest_ts < cutoff:
            continue

        refresh_calls_market_data(recent_calls)
        wins = [is_win_call(call) for call in recent_calls]
        losses = [is_loss_call(call) for call in recent_calls]
        hot_streak = consecutive_count(wins)
        cold_streak = consecutive_count(losses)

        profile = user_profiles_collection.find_one({"chat_id": chat_id, "user_id": user_id}) or {}
        caller_name = recent_calls[0].get("caller_name", profile.get("display_name", f"User {user_id}"))
        now = utc_now()

        if hot_streak >= HOT_STREAK_MIN:
            last_hot = profile.get("alerts", {}).get("hot_notified_at")
            hours_since = _hours_since(last_hot)
            last_hot_len = int(profile.get("alerts", {}).get("hot_len", 0) or 0)
            should_send = manual or hours_since is None or hours_since >= ALERT_COOLDOWN_HOURS or hot_streak > last_hot_len
            if should_send:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"🔥 Hot Streak Alert\n"
                        f"{caller_name} is on a {hot_streak}-call win streak.\n"
                        f"Recent call was within the last {ACTIVE_CALL_WINDOW_HOURS}h."
                    ),
                )
                user_profiles_collection.update_one(
                    {"chat_id": chat_id, "user_id": user_id},
                    {"$set": {"alerts.hot_notified_at": now, "alerts.hot_len": hot_streak}},
                    upsert=True,
                )
                triggered += 1

        if cold_streak >= COLD_STREAK_MIN:
            last_cold = profile.get("alerts", {}).get("cold_notified_at")
            hours_since = _hours_since(last_cold)
            last_cold_len = int(profile.get("alerts", {}).get("cold_len", 0) or 0)
            should_send = manual or hours_since is None or hours_since >= ALERT_COOLDOWN_HOURS or cold_streak > last_cold_len
            if should_send:
                await bot.send_message(
                    chat_id=chat_id,
                    text=(
                        f"⚠️ Danger Streak\n"
                        f"{caller_name} is on a {cold_streak}-call losing streak.\n"
                        f"Review recent calls before trusting new entries."
                    ),
                )
                user_profiles_collection.update_one(
                    {"chat_id": chat_id, "user_id": user_id},
                    {"$set": {"alerts.cold_notified_at": now, "alerts.cold_len": cold_streak}},
                    upsert=True,
                )
                triggered += 1

    return triggered


def build_daily_digest(chat_id, since_ts):
    query = _accepted_query(chat_id, {"timestamp": {"$gte": since_ts}})
    calls = list(calls_collection.find(query))
    if not calls:
        return "📰 Daily Intel Digest\nNo accepted calls in the last 24h."

    refresh_calls_market_data(calls)
    user_calls = {}
    for call in calls:
        user_calls.setdefault(get_caller_key(call), []).append(call)

    ranking = []
    for _, call_set in user_calls.items():
        metrics = derive_user_metrics(call_set)
        if metrics["calls"] == 0:
            continue
        ranking.append(
            {
                "name": call_set[0].get("caller_name", "Unknown"),
                "calls": metrics["calls"],
                "avg_now_x": 1.0 + metrics["avg_now"],
                "best_x": metrics["best_x"],
                "win_rate": metrics["win_rate"] * 100,
            }
        )

    ranking.sort(key=lambda x: (x["avg_now_x"], x["win_rate"], x["calls"]), reverse=True)
    top = ranking[:3]
    worst = sorted(ranking, key=lambda x: (x["avg_now_x"], x["win_rate"]))[:3]

    best_call = max(
        calls,
        key=lambda c: (float(c.get("ath_mcap", 0) or 0) / float(c.get("initial_mcap", 1) or 1)),
        default=None,
    )
    worst_rug = min(
        calls,
        key=lambda c: (float(c.get("current_mcap", 0) or 0) / float(c.get("initial_mcap", 1) or 1)),
        default=None,
    )

    ca_counts = {}
    for call in calls:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        if not ca_norm:
            continue
        item = ca_counts.setdefault(ca_norm, {"count": 0, "symbol": call.get("token_symbol", ""), "ca": call.get("ca", ca_norm)})
        item["count"] += 1
    top_mentions = sorted(ca_counts.values(), key=lambda x: x["count"], reverse=True)[:5]

    lines = ["📰 Daily Intel Digest (24h)", ""]
    lines.append("🏆 Top Callers:")
    if top:
        for row in top:
            lines.append(
                f"- {row['name']}: Avg {format_return(row['avg_now_x'])}, Win {row['win_rate']:.1f}%, Calls {row['calls']}"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("🧯 Worst Callers:")
    if worst:
        for row in worst:
            lines.append(
                f"- {row['name']}: Avg {format_return(row['avg_now_x'])}, Win {row['win_rate']:.1f}%, Calls {row['calls']}"
            )
    else:
        lines.append("- None")

    lines.append("")
    if best_call:
        initial = float(best_call.get("initial_mcap", 1) or 1)
        best_x = float(best_call.get("ath_mcap", initial) or initial) / initial
        lines.append(f"🔥 Best Call: {format_return(best_x)} by {best_call.get('caller_name', 'Unknown')}")
    else:
        lines.append("🔥 Best Call: N/A")

    if worst_rug:
        initial = float(worst_rug.get("initial_mcap", 1) or 1)
        now_x = float(worst_rug.get("current_mcap", initial) or initial) / initial
        lines.append(f"🩸 Worst Rug: {format_return(now_x)} by {worst_rug.get('caller_name', 'Unknown')}")
    else:
        lines.append("🩸 Worst Rug: N/A")

    lines.append("")
    lines.append("📣 Most Mentioned CAs:")
    if top_mentions:
        for row in top_mentions:
            lines.append(f"- {token_label(row['symbol'], row['ca'])}: {row['count']} mentions")
    else:
        lines.append("- None")

    return "\n".join(lines)


async def send_daily_digest(bot, chat_id, manual=False):
    setting = settings_collection.find_one({"chat_id": chat_id}) or {}
    if not manual and not setting.get("alerts", False):
        return False

    now = utc_now()
    today = now.strftime("%Y-%m-%d")
    if not manual:
        if now.hour < DIGEST_HOUR_UTC:
            return False
        if setting.get("last_digest_date") == today:
            return False

    digest_text = build_daily_digest(chat_id, now - timedelta(hours=24))
    await bot.send_message(chat_id=chat_id, text=digest_text)
    settings_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"last_digest_date": today}},
        upsert=True,
    )
    return True


async def heartbeat_loop(application: Application):
    while True:
        try:
            chat_ids = get_tracked_chat_ids()
            for chat_id in chat_ids:
                try:
                    await run_streak_scan_for_chat(application.bot, chat_id, manual=False)
                    await send_daily_digest(application.bot, chat_id, manual=False)
                except Exception as exc:
                    print(f"Heartbeat chat error ({chat_id}): {exc}")
        except asyncio.CancelledError:
            break
        except Exception as exc:
            print(f"Heartbeat loop error: {exc}")

        await asyncio.sleep(HEARTBEAT_INTERVAL_SECONDS)


async def on_startup(application: Application):
    application.bot_data["heartbeat_task"] = asyncio.create_task(heartbeat_loop(application))


async def on_shutdown(application: Application):
    task = application.bot_data.get("heartbeat_task")
    if task:
        task.cancel()
        try:
            await task
        except asyncio.CancelledError:
            pass


async def toggle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    setting = settings_collection.find_one({"chat_id": chat_id}) or {}

    if not setting.get("alerts", False):
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.effective_message.reply_text(
            "Alerts ON: heartbeat streak alerts and daily digest are enabled."
        )
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.effective_message.reply_text(
            "Alerts OFF: heartbeat streak alerts and daily digest are disabled."
        )


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


async def render_leaderboard_page(message_obj, context, page=0):
    data = context.chat_data.get("leaderboard_data", [])
    title = context.chat_data.get("leaderboard_title", "Leaderboard")

    items_per_page = 10
    total_pages = max(1, math.ceil(len(data) / items_per_page))

    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_data = data[start_idx:end_idx]

    lines = [f"🏆 {title.upper()}", f"📄 Page {page + 1}/{total_pages}", "────────────────"]

    for idx, row in enumerate(page_data, start=start_idx + 1):
        badge = rank_badge(idx)
        stars = stars_from_pct(row["win_rate"])
        lines.append(
            f"{badge} {row['name']} {stars}\n"
            f"↳ 📈 Avg: {format_return(row['avg_now_x'])} | 🔥 Best: {format_return(row['best_x'])}\n"
            f"↳ 🎯 Win: {row['win_rate']:.1f}% | 📞 Calls: {row['calls']}"
        )
        lines.append("────────────────")

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
    rug = derive_rug_stats(all_user_calls)

    recent_calls = all_user_calls[:5]
    actual_name = recent_calls[0].get("caller_name", "Unknown")
    caller_id = recent_calls[0].get("caller_id")
    win_pct = metrics["win_rate"] * 100
    recent_cas_norm = [c.get("ca_norm", normalize_ca(c.get("ca", ""))) for c in recent_calls if c.get("ca")]
    recent_meta = get_dexscreener_batch_meta(recent_cas_norm)
    avg_text = format_return(1 + metrics["avg_now"])
    best_text = format_return(metrics["best_x"])
    stars = stars_from_pct(win_pct)

    lines = [
        f"👤 {html.escape(actual_name)}  {stars}",
        "────────────────",
        f"📞 Calls: {metrics['calls']}",
        f"📈 Avg: {avg_text} | 🔥 Best: {best_text}",
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_pct:.1f}%",
        f"🩸 Rug Calls: {rug['rug_rate']:.1f}% ({rug['rug_count']}/{rug['eligible']})",
        f"🏅 Badges: {html.escape(', '.join(metrics['badges']) if metrics['badges'] else 'None')}",
        "",
        "📚 Recent 5 Calls",
        "────────────────",
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
        lines.append("────────────────")

    reply_markup = None
    if caller_id is not None:
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📊 Mini Chart", callback_data=f"chart_caller_{caller_id}")]]
        )

    await update.effective_message.reply_text(
        "\n".join(lines),
        parse_mode="HTML",
        reply_markup=reply_markup,
    )


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
    stars = stars_from_pct(win_pct)

    text = (
        f"📈 Your Performance  {stars}\n"
        f"────────────────\n"
        f"📞 Calls: {metrics['calls']}\n"
        f"📈 Avg: {format_return(1 + metrics['avg_now'])} | 🔥 Best: {format_return(metrics['best_x'])}\n"
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_pct:.1f}%\n"
        f"⭐ Score: {score:.1f}/100\n"
        f"🏅 Badges: {', '.join(metrics['badges']) if metrics['badges'] else 'None'}"
    )
    await update.effective_message.reply_text(text)


async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.effective_chat.id
    status_message = await update.effective_message.reply_text("Analyzing group performance...")
    time_filter, time_text = _resolve_time_filter(context)
    query = {**accepted_call_filter(chat_id), **time_filter}
    all_calls = list(calls_collection.find(query))
    if not all_calls:
        await status_message.edit_text(f"No calls tracked in this group for {time_text}")
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
        f"📊 Group Performance ({time_text.upper()})\n"
        f"────────────────\n"
        f"👥 Callers: {len(unique_callers)} | 📞 Calls: {total_calls}\n"
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {group_metrics['win_rate'] * 100:.1f}%\n"
        f"📈 Group Avg: {format_return(1 + group_metrics['avg_now'])}\n"
        f"🔥 Best Call: {best_text}\n"
        f"{best_by_text}"
    )

    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📊 Mini Chart", callback_data="chart_group")]]
    )
    await status_message.edit_text(text, parse_mode="HTML", reply_markup=reply_markup)


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
        "────────────────",
        f"✅ Accepted: {accepted} | ❌ Rejected: {rejected}",
        f"🎯 Acceptance: {(accepted / (accepted + rejected) * 100) if (accepted + rejected) else 0:.1f}%",
        f"⏱ Delay avg/max: {avg_delay:.1f}s / {max_delay:.0f}s",
        "",
        "🚫 Reject Reasons",
        "────────────────",
    ]

    if reason_counts:
        for row in reason_counts[:5]:
            lines.append(f"- {row['_id'] or 'unknown'}: {row['count']}")
    else:
        lines.append("- None")

    lines.append("")
    lines.append("🕵️ Spam Watchlist")
    lines.append("────────────────")
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
    lines.append("📉 Low Performers (>=3 calls)")
    lines.append("────────────────")
    if low_performers:
        for row in low_performers[:5]:
            lines.append(
                f"- {row['name']}: win {row['win_rate']:.1f}%, avg {format_return(row['avg_now_x'])}, calls {row['calls']}"
            )
    else:
        lines.append("- None")

    await msg.reply_text("\n".join(lines))


async def send_group_mini_chart(context: ContextTypes.DEFAULT_TYPE, chat_id: int, time_arg: str = "7d"):
    fake_context = type("obj", (), {"args": [time_arg]})()
    time_filter, time_text = _resolve_time_filter(fake_context)
    calls = list(calls_collection.find(_accepted_query(chat_id, time_filter)))
    if not calls:
        await context.bot.send_message(chat_id=chat_id, text=f"No data for {time_text} to chart.")
        return

    refresh_calls_market_data(calls)
    metrics = derive_user_metrics(calls)
    chart_url = build_performance_chart_url(
        f"Group Mini Chart ({time_text})",
        metrics["win_rate"] * 100.0,
        metrics["profitable_rate"] * 100.0,
        1.0 + metrics["avg_now"],
    )
    caption = (
        f"📊 Group Mini Chart ({time_text})\n"
        f"Win Rate: {metrics['win_rate'] * 100:.1f}%\n"
        f"Profitable: {metrics['profitable_rate'] * 100:.1f}%\n"
        f"Avg: {format_return(1.0 + metrics['avg_now'])}"
    )
    await context.bot.send_photo(chat_id=chat_id, photo=chart_url, caption=caption)


async def send_caller_mini_chart(context: ContextTypes.DEFAULT_TYPE, chat_id: int, caller_id: int):
    calls = list(
        calls_collection.find(_accepted_query(chat_id, {"caller_id": caller_id}))
        .sort("timestamp", -1)
        .limit(50)
    )
    if not calls:
        await context.bot.send_message(chat_id=chat_id, text="No caller data found for chart.")
        return

    refresh_calls_market_data(calls)
    metrics = derive_user_metrics(calls)
    caller_name = calls[0].get("caller_name", f"User {caller_id}")
    chart_url = build_performance_chart_url(
        f"{caller_name} Mini Chart",
        metrics["win_rate"] * 100.0,
        metrics["profitable_rate"] * 100.0,
        1.0 + metrics["avg_now"],
    )
    caption = (
        f"📊 Caller Mini Chart: {caller_name}\n"
        f"Win Rate: {metrics['win_rate'] * 100:.1f}%\n"
        f"Profitable: {metrics['profitable_rate'] * 100:.1f}%\n"
        f"Avg: {format_return(1.0 + metrics['avg_now'])} | Best: {format_return(metrics['best_x'])}"
    )
    await context.bot.send_photo(chat_id=chat_id, photo=chart_url, caption=caption)


def top_caller_id(chat_id: int, lookback_days: int = 7):
    cutoff = utc_now() - timedelta(days=lookback_days)
    calls = list(calls_collection.find(_accepted_query(chat_id, {"timestamp": {"$gte": cutoff}})))
    if not calls:
        return None
    user_calls = {}
    for call in calls:
        caller_id = call.get("caller_id")
        if caller_id is None:
            continue
        user_calls.setdefault(caller_id, []).append(call)
    if not user_calls:
        return None
    best = None
    best_score = -10**9
    for caller_id, call_set in user_calls.items():
        metrics = derive_user_metrics(call_set)
        score = (1.0 + metrics["avg_now"]) + (metrics["win_rate"] * 0.5)
        if metrics["calls"] >= 2 and score > best_score:
            best = caller_id
            best_score = score
    return best


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    if not await user_is_admin(context.bot, chat.id, user.id):
        await update.effective_message.reply_text("Admin only command")
        return

    keyboard = InlineKeyboardMarkup(
        [
            [
                InlineKeyboardButton("🔥 Test Streak Scan", callback_data="admin_streak"),
                InlineKeyboardButton("📰 Test Daily Digest", callback_data="admin_digest"),
            ],
            [
                InlineKeyboardButton("📊 Group Mini Chart", callback_data="admin_group_chart"),
                InlineKeyboardButton("🏆 Top Caller Chart", callback_data="admin_top_caller_chart"),
            ],
        ]
    )
    await update.effective_message.reply_text("Admin Test Panel", reply_markup=keyboard)


async def admin_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    user_id = query.from_user.id

    if not await user_is_admin(context.bot, chat_id, user_id):
        await query.message.reply_text("Admin only action")
        return

    action = query.data
    if action == "admin_streak":
        count = await run_streak_scan_for_chat(context.bot, chat_id, manual=True)
        await query.message.reply_text(f"Streak scan complete. Alerts sent: {count}")
    elif action == "admin_digest":
        await send_daily_digest(context.bot, chat_id, manual=True)
        await query.message.reply_text("Daily digest sent.")
    elif action == "admin_group_chart":
        await send_group_mini_chart(context, chat_id, time_arg="7d")
    elif action == "admin_top_caller_chart":
        caller_id = top_caller_id(chat_id, lookback_days=7)
        if caller_id is None:
            await query.message.reply_text("No top caller found for chart.")
            return
        await send_caller_mini_chart(context, chat_id, caller_id)


async def chart_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = query.message.chat_id
    data = query.data

    if data == "chart_group":
        await send_group_mini_chart(context, chat_id, time_arg="7d")
        return

    if data.startswith("chart_caller_"):
        try:
            caller_id = int(data.split("_")[-1])
        except ValueError:
            await query.message.reply_text("Invalid caller chart request.")
            return
        await send_caller_mini_chart(context, chat_id, caller_id)


def main():
    ensure_indexes()

    app = (
        Application.builder()
        .token(TOKEN)
        .post_init(on_startup)
        .post_shutdown(on_shutdown)
        .build()
    )

    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_ca))
    app.add_handler(MessageHandler(filters.UpdateType.EDITED_MESSAGE & filters.TEXT & ~filters.COMMAND, track_ca))

    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("bottom", bottom))
    app.add_handler(CommandHandler("togglealerts", toggle_alerts))
    app.add_handler(CommandHandler("caller", caller_profile))
    app.add_handler(CommandHandler("groupstats", group_stats))
    app.add_handler(CommandHandler("myscore", my_score))
    app.add_handler(CommandHandler("adminstats", admin_stats))
    app.add_handler(CommandHandler("adminpanel", admin_panel))

    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern=r"^lb_"))
    app.add_handler(CallbackQueryHandler(admin_actions, pattern=r"^admin_"))
    app.add_handler(CallbackQueryHandler(chart_actions, pattern=r"^chart_"))

    print("YabaiRankBot running")
    app.run_polling()


if __name__ == "__main__":
    main()
