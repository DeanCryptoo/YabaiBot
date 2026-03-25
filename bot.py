import os
import re
import math
import html
import json
import time
import asyncio
import requests
import certifi
from io import BytesIO
from urllib.parse import quote
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient, ASCENDING, DESCENDING
from PIL import Image, ImageDraw, ImageFont
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton, User
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
ATH_TRACK_WINDOW_DAYS = 7
ATH_TRACK_MAX_CALLS_PER_CHAT = 800
HEARTBEAT_CALLS_PER_CALLER = 2
ATH_PRIORITY_KEEP_MAX_CALLS = max(0, int(os.getenv("ATH_PRIORITY_KEEP_MAX_CALLS", "40")))
ATH_PRIORITY_MIN_X = max(1.0, float(os.getenv("ATH_PRIORITY_MIN_X", "2.0")))
INACTIVE_CALLER_ARCHIVE_HOURS = 24
LOW_VOLUME_STASH_THRESHOLD = 1000.0
LOW_VOLUME_LOOKBACK = "h1"
LOW_VOLUME_ARCHIVE_MIN_AGE_HOURS = max(1, int(os.getenv("LOW_VOLUME_ARCHIVE_MIN_AGE_HOURS", "3")))
LOW_VOLUME_STASH_MIN_AGE_HOURS = max(0.0, float(os.getenv("LOW_VOLUME_STASH_MIN_AGE_HOURS", "1")))
CALLER_LIVE_METRIC_REFRESH_LIMIT = max(20, int(os.getenv("CALLER_LIVE_METRIC_REFRESH_LIMIT", "120")))
DEX_CACHE_TTL_SECONDS = max(5, int(os.getenv("DEX_CACHE_TTL_SECONDS", "20")))
DEX_CACHE_MAX_ENTRIES = max(200, int(os.getenv("DEX_CACHE_MAX_ENTRIES", "4000")))
GROUPSTATS_CACHE_TTL_SECONDS = max(10, int(os.getenv("GROUPSTATS_CACHE_TTL_SECONDS", "45")))
CHAT_AVATAR_CACHE_TTL_SECONDS = max(60, int(os.getenv("CHAT_AVATAR_CACHE_TTL_SECONDS", "3600")))
SCORE_SAMPLE_PRIOR_CALLS = max(1.0, float(os.getenv("SCORE_SAMPLE_PRIOR_CALLS", "8")))
SCORE_RATE_PRIOR_CALLS = max(1.0, float(os.getenv("SCORE_RATE_PRIOR_CALLS", "6")))
SCORE_BASELINE_WIN_RATE = float(os.getenv("SCORE_BASELINE_WIN_RATE", "0.35"))
SCORE_BASELINE_PROFITABLE_RATE = float(os.getenv("SCORE_BASELINE_PROFITABLE_RATE", "0.45"))
SCORE_AVG_SOFTCAP_X = max(1.5, float(os.getenv("SCORE_AVG_SOFTCAP_X", "4.0")))
SCORE_BEST_SOFTCAP_X = max(2.0, float(os.getenv("SCORE_BEST_SOFTCAP_X", "20.0")))
REFRESH_QUEUE_LOOKBACK_DAYS = max(7, int(os.getenv("REFRESH_QUEUE_LOOKBACK_DAYS", "21")))
REFRESH_QUEUE_MAX_CALLS_PER_CHAT = max(40, int(os.getenv("REFRESH_QUEUE_MAX_CALLS_PER_CHAT", "120")))
ACTIVE_LIVE_CALLS_PER_CHAT = max(40, int(os.getenv("ACTIVE_LIVE_CALLS_PER_CHAT", "180")))
GLOBAL_STASH_MIN_AGE_HOURS = max(1, int(os.getenv("GLOBAL_STASH_MIN_AGE_HOURS", "2")))
PRIORITY_STASH_ARCHIVE_MIN_AGE_HOURS = max(1, int(os.getenv("PRIORITY_STASH_ARCHIVE_MIN_AGE_HOURS", "12")))
HOT_REFRESH_INTERVAL_SECONDS = max(60, int(os.getenv("HOT_REFRESH_INTERVAL_SECONDS", "120")))
WARM_REFRESH_INTERVAL_SECONDS = max(HOT_REFRESH_INTERVAL_SECONDS, int(os.getenv("WARM_REFRESH_INTERVAL_SECONDS", "300")))
NORMAL_REFRESH_INTERVAL_SECONDS = max(WARM_REFRESH_INTERVAL_SECONDS, int(os.getenv("NORMAL_REFRESH_INTERVAL_SECONDS", "600")))
COLD_REFRESH_INTERVAL_SECONDS = max(NORMAL_REFRESH_INTERVAL_SECONDS, int(os.getenv("COLD_REFRESH_INTERVAL_SECONDS", "1800")))
RUNNER_PROTECT_MIN_X = max(1.1, float(os.getenv("RUNNER_PROTECT_MIN_X", "1.5")))
RUNNER_PROTECT_MAX_CALLS = max(10, int(os.getenv("RUNNER_PROTECT_MAX_CALLS", "80")))
RUNNER_PROTECT_MAX_AGE_HOURS = max(24, int(os.getenv("RUNNER_PROTECT_MAX_AGE_HOURS", "96")))
RUNNER_REPOST_BOOST_HOURS = max(1, int(os.getenv("RUNNER_REPOST_BOOST_HOURS", "24")))
LEADERBOARD_CACHE_TTL_SECONDS = max(5, int(os.getenv("LEADERBOARD_CACHE_TTL_SECONDS", "20")))
DAILY_ROLLUP_REPAIR_HOUR_UTC = min(23, max(0, int(os.getenv("DAILY_ROLLUP_REPAIR_HOUR_UTC", "3"))))
SOLANA_TRACKER_API_KEY = (os.getenv("SOLANA_TRACKER_API_KEY") or "").strip()
HISTORICAL_ATH_PROVIDER = (os.getenv("HISTORICAL_ATH_PROVIDER") or ("solanatracker" if SOLANA_TRACKER_API_KEY else "none")).strip().lower()
HISTORICAL_ATH_ENABLED = HISTORICAL_ATH_PROVIDER == "solanatracker" and bool(SOLANA_TRACKER_API_KEY)
HISTORICAL_ATH_REQUEST_TIMEOUT_SECONDS = max(3, int(os.getenv("HISTORICAL_ATH_REQUEST_TIMEOUT_SECONDS", "8")))
HISTORICAL_ATH_CACHE_TTL_SECONDS = max(15, int(os.getenv("HISTORICAL_ATH_CACHE_TTL_SECONDS", "120")))
HISTORICAL_ATH_RECHECK_MINUTES = max(10, int(os.getenv("HISTORICAL_ATH_RECHECK_MINUTES", "60")))
HISTORICAL_ATH_MIN_CALL_AGE_SECONDS = max(60, int(os.getenv("HISTORICAL_ATH_MIN_CALL_AGE_SECONDS", "180")))
HISTORICAL_ATH_HEARTBEAT_MAX_CALLS = max(0, int(os.getenv("HISTORICAL_ATH_HEARTBEAT_MAX_CALLS", "18")))
HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS = max(0, int(os.getenv("HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS", "8")))
HISTORICAL_ATH_MANUAL_MAX_CALLS = max(0, int(os.getenv("HISTORICAL_ATH_MANUAL_MAX_CALLS", "120")))
HISTORICAL_ATH_ARCHIVE_LOOKBACK_DAYS = max(1, int(os.getenv("HISTORICAL_ATH_ARCHIVE_LOOKBACK_DAYS", "30")))

if not TOKEN or not MONGO_URI:
    raise ValueError("Missing TELEGRAM_TOKEN or MONGO_URI environment variables")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client["yabai_crypto_bot"]

calls_collection = db["token_calls"]
calls_archive_collection = db["token_calls_archive"]
caller_rollups_collection = db["caller_rollups"]
settings_collection = db["group_settings"]
user_profiles_collection = db["user_profiles"]
private_links_collection = db["private_links"]

CA_REGEX = r"\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b"
_dex_meta_cache = {}
_ops_runtime = {"by_chat": {}}
_leaderboard_sessions = {}
_leaderboard_page_cache = {}
_groupstats_cache = {}
_groupstats_media_cache = {}
_chat_avatar_cache = {}
_historical_ath_cache = {}
ROLLUP_SCHEMA_VERSION = 3
KICKLIST_MAX_AVG_X = 1.40
KICKLIST_MIN_CALLS = 2
KICKLIST_LIMIT = 20


def ensure_indexes():
    calls_collection.create_index([
        ("chat_id", ASCENDING),
        ("ca_norm", ASCENDING),
        ("status", ASCENDING),
    ])
    calls_collection.create_index([("chat_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_collection.create_index([("chat_id", ASCENDING), ("is_stashed", ASCENDING), ("timestamp", DESCENDING)])
    calls_collection.create_index([("chat_id", ASCENDING), ("caller_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_collection.create_index([("chat_id", ASCENDING), ("is_stashed", ASCENDING), ("next_refresh_at", ASCENDING)])
    calls_collection.create_index([("chat_id", ASCENDING), ("is_stashed", ASCENDING), ("refresh_priority", DESCENDING), ("next_refresh_at", ASCENDING)])
    calls_collection.create_index([("message_id", ASCENDING), ("chat_id", ASCENDING)])
    calls_archive_collection.create_index([("chat_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_archive_collection.create_index([("chat_id", ASCENDING), ("caller_id", ASCENDING), ("timestamp", DESCENDING)])
    calls_archive_collection.create_index([("chat_id", ASCENDING), ("ca_norm", ASCENDING)])
    caller_rollups_collection.create_index([("chat_id", ASCENDING), ("caller_key", ASCENDING)], unique=True)
    caller_rollups_collection.create_index([("chat_id", ASCENDING), ("avg_x", DESCENDING), ("calls", DESCENDING)])
    caller_rollups_collection.create_index([("chat_id", ASCENDING), ("score", DESCENDING), ("calls", DESCENDING)])

    user_profiles_collection.create_index([("chat_id", ASCENDING), ("user_id", ASCENDING)], unique=True)
    settings_collection.create_index([("chat_id", ASCENDING)], unique=True)
    settings_collection.create_index([("group_key", ASCENDING)], unique=True, sparse=True)
    private_links_collection.create_index([("user_id", ASCENDING)], unique=True)


def utc_now():
    return datetime.now(timezone.utc)


def canonical_chat_id(chat_id):
    try:
        return int(chat_id)
    except (TypeError, ValueError):
        return chat_id


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


def stars_from_rank(rank):
    if rank <= 0:
        return ""
    filled = max(0, 6 - int(rank))
    return "★" * filled


def stars_from_score(score):
    filled = int(clamp(round(float(score or 0.0) / 20.0), 0, 5))
    return ("★" * filled) + ("☆" * (5 - filled))


def delete_callback_data(user_id):
    try:
        uid = int(user_id or 0)
    except (TypeError, ValueError):
        uid = 0
    return f"delm:{uid}"


def delete_button_markup(user_id):
    return InlineKeyboardMarkup(
        [[InlineKeyboardButton("🗑 Delete", callback_data=delete_callback_data(user_id))]]
    )


def with_delete_button(reply_markup, user_id):
    delete_row = [InlineKeyboardButton("🗑 Delete", callback_data=delete_callback_data(user_id))]
    if reply_markup is None:
        return InlineKeyboardMarkup([delete_row])
    rows = [list(row) for row in reply_markup.inline_keyboard]
    rows.append(delete_row)
    return InlineKeyboardMarkup(rows)


def _text_width(draw, text, font):
    if not text:
        return 0
    left, _, right, _ = draw.textbbox((0, 0), str(text), font=font)
    return right - left


def fit_text(draw, text, font, max_width):
    text = str(text or "")
    if _text_width(draw, text, font) <= max_width:
        return text
    if max_width <= 20:
        return ""
    trimmed = text
    while len(trimmed) > 1 and _text_width(draw, trimmed + "...", font) > max_width:
        trimmed = trimmed[:-1]
    return (trimmed + "...") if trimmed else ""


def wrap_text_lines(draw, text, font, max_width, max_lines=2):
    words = str(text or "").split()
    if not words:
        return [""]
    lines = []
    current = ""
    for word in words:
        candidate = f"{current} {word}".strip()
        if _text_width(draw, candidate, font) <= max_width:
            current = candidate
        else:
            if current:
                lines.append(current)
            current = word
            if len(lines) >= max_lines - 1:
                break
    if current and len(lines) < max_lines:
        lines.append(current)

    if len(lines) == max_lines:
        lines[-1] = fit_text(draw, lines[-1], font, max_width)
    return lines


def ascii_safe(text, fallback="N/A"):
    cleaned = "".join(ch for ch in str(text or "") if ord(ch) < 128)
    cleaned = " ".join(cleaned.split()).strip()
    return cleaned if cleaned else fallback


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


SCORE_BASELINE_WIN_RATE = clamp(SCORE_BASELINE_WIN_RATE, 0.0, 1.0)
SCORE_BASELINE_PROFITABLE_RATE = clamp(SCORE_BASELINE_PROFITABLE_RATE, 0.0, 1.0)


def smooth_rate(observed_rate, calls, baseline_rate, prior_calls=SCORE_RATE_PRIOR_CALLS):
    calls = max(0.0, float(calls or 0.0))
    observed_rate = clamp(float(observed_rate or 0.0), 0.0, 1.0)
    baseline_rate = clamp(float(baseline_rate or 0.0), 0.0, 1.0)
    prior_calls = max(1.0, float(prior_calls or SCORE_RATE_PRIOR_CALLS))
    return ((observed_rate * calls) + (baseline_rate * prior_calls)) / (calls + prior_calls)


def sample_confidence(calls, prior_calls=SCORE_SAMPLE_PRIOR_CALLS):
    calls = max(0.0, float(calls or 0.0))
    prior_calls = max(1.0, float(prior_calls or SCORE_SAMPLE_PRIOR_CALLS))
    return calls / (calls + prior_calls)


def compute_performance_score(calls, avg_x, win_rate, profitable_rate, best_x):
    calls = max(0.0, float(calls or 0.0))
    if calls <= 0:
        return 0.0

    avg_x = max(0.0, float(avg_x or 0.0))
    best_x = max(0.0, float(best_x or 0.0))
    win_rate = clamp(float(win_rate or 0.0), 0.0, 1.0)
    profitable_rate = clamp(float(profitable_rate or 0.0), 0.0, 1.0)

    conf = sample_confidence(calls)
    adjusted_avg_x = 1.0 + ((avg_x - 1.0) * conf)
    avg_component = clamp((adjusted_avg_x - 1.0) / max(0.1, SCORE_AVG_SOFTCAP_X - 1.0), 0.0, 1.0)
    win_component = smooth_rate(win_rate, calls, SCORE_BASELINE_WIN_RATE)
    profitable_component = smooth_rate(profitable_rate, calls, SCORE_BASELINE_PROFITABLE_RATE)
    best_component = clamp((max(best_x, 1.0) - 1.0) / max(0.1, SCORE_BEST_SOFTCAP_X - 1.0), 0.0, 1.0)

    score = 100.0 * (
        0.35 * avg_component
        + 0.20 * win_component
        + 0.20 * profitable_component
        + 0.20 * conf
        + 0.05 * best_component
    )
    return clamp(score, 0.0, 100.0)


def _mongo_clamp_expr(expr, low=0.0, high=1.0):
    return {"$min": [high, {"$max": [low, expr]}]}


def mongo_performance_score_expr(calls_expr, wins_expr, profitables_expr, avg_x_expr, best_x_expr):
    sample_expr = {
        "$cond": [
            {"$gt": [calls_expr, 0]},
            {"$divide": [calls_expr, {"$add": [calls_expr, SCORE_SAMPLE_PRIOR_CALLS]}]},
            0,
        ]
    }
    adjusted_avg_expr = {
        "$add": [
            1.0,
            {
                "$multiply": [
                    {"$subtract": [avg_x_expr, 1.0]},
                    sample_expr,
                ]
            },
        ]
    }
    avg_component_expr = _mongo_clamp_expr(
        {"$divide": [{"$subtract": [adjusted_avg_expr, 1.0]}, max(0.1, SCORE_AVG_SOFTCAP_X - 1.0)]}
    )
    win_component_expr = {
        "$cond": [
            {"$gt": [calls_expr, 0]},
            {
                "$divide": [
                    {"$add": [wins_expr, SCORE_BASELINE_WIN_RATE * SCORE_RATE_PRIOR_CALLS]},
                    {"$add": [calls_expr, SCORE_RATE_PRIOR_CALLS]},
                ]
            },
            0,
        ]
    }
    profitable_component_expr = {
        "$cond": [
            {"$gt": [calls_expr, 0]},
            {
                "$divide": [
                    {"$add": [profitables_expr, SCORE_BASELINE_PROFITABLE_RATE * SCORE_RATE_PRIOR_CALLS]},
                    {"$add": [calls_expr, SCORE_RATE_PRIOR_CALLS]},
                ]
            },
            0,
        ]
    }
    best_component_expr = _mongo_clamp_expr(
        {"$divide": [{"$subtract": [{"$max": [best_x_expr, 1.0]}, 1.0]}, max(0.1, SCORE_BEST_SOFTCAP_X - 1.0)]}
    )
    return {
        "$cond": [
            {"$gt": [calls_expr, 0]},
            {
                "$multiply": [
                    100.0,
                    {
                        "$add": [
                            {"$multiply": [0.35, avg_component_expr]},
                            {"$multiply": [0.20, win_component_expr]},
                            {"$multiply": [0.20, profitable_component_expr]},
                            {"$multiply": [0.20, sample_expr]},
                            {"$multiply": [0.05, best_component_expr]},
                        ]
                    },
                ]
            },
            0,
        ]
    }


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


def load_font(size, bold=False):
    candidates = []
    if bold:
        candidates += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans-Bold.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Bold.ttf",
            "C:\\Windows\\Fonts\\arialbd.ttf",
        ]
    else:
        candidates += [
            "/usr/share/fonts/truetype/dejavu/DejaVuSans.ttf",
            "/usr/share/fonts/truetype/liberation/LiberationSans-Regular.ttf",
            "C:\\Windows\\Fonts\\arial.ttf",
        ]

    for path in candidates:
        try:
            return ImageFont.truetype(path, size=size)
        except Exception:
            continue
    return ImageFont.load_default()


def draw_vertical_gradient(image, top_rgb, bottom_rgb):
    width, height = image.size
    draw = ImageDraw.Draw(image)
    for y in range(height):
        t = y / max(1, height - 1)
        r = int(top_rgb[0] * (1 - t) + bottom_rgb[0] * t)
        g = int(top_rgb[1] * (1 - t) + bottom_rgb[1] * t)
        b = int(top_rgb[2] * (1 - t) + bottom_rgb[2] * t)
        draw.line([(0, y), (width, y)], fill=(r, g, b))


def generate_group_stats_card(
    time_text,
    callers_count,
    total_calls,
    win_rate_pct,
    avg_text,
    best_text,
    best_caller,
    group_avatar_image=None,
):
    width, height = 1200, 440
    card = Image.new("RGB", (width, height), (14, 22, 38))
    draw_vertical_gradient(card, (12, 28, 46), (23, 54, 78))

    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((60, 28, 1140, 412), radius=28, fill=(8, 18, 34, 175), outline=(83, 138, 189, 135), width=2)
    od.ellipse((700, -90, 1170, 320), fill=(59, 130, 246, 48))
    card = Image.alpha_composite(card.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(card)

    title_font = load_font(46, bold=True)
    block_font = load_font(30, bold=True)
    stat_font = load_font(42, bold=True)
    sub_font = load_font(26, bold=False)

    left_x = 88
    right_x = 680
    right_w = 380

    draw.text((left_x, 54), "GROUP PERFORMANCE SPOTLIGHT", font=title_font, fill=(241, 247, 255))
    draw.text((left_x + 2, 112), fit_text(draw, f"Window: {ascii_safe(time_text)}", sub_font, 540), font=sub_font, fill=(161, 203, 235))

    draw.text((left_x, 175), fit_text(draw, f"Callers {callers_count} • Calls {total_calls}", block_font, 540), font=block_font, fill=(255, 255, 255))
    draw.text((left_x, 228), fit_text(draw, f"Hit Rate {win_rate_pct:.1f}%", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 276), fit_text(draw, f"Average {avg_text}", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 330), "Auto-generated by Yabai Bot", font=sub_font, fill=(130, 170, 205))

    draw.text((right_x, 180), "Best Call", font=block_font, fill=(204, 231, 255))
    draw.text((right_x, 228), fit_text(draw, best_text, stat_font, right_w), font=stat_font, fill=(255, 255, 255))
    best_caller_text = f"By {ascii_safe(best_caller, fallback='N/A')}"
    draw.text((right_x, 286), fit_text(draw, best_caller_text, block_font, right_w), font=block_font, fill=(217, 236, 255))

    group_avatar = build_circle_avatar(group_avatar_image, 88) if group_avatar_image is not None else None
    if group_avatar is not None:
        icon_x, icon_y = 1038, 46
        card_rgba = card.convert("RGBA")
        card_rgba.alpha_composite(group_avatar, (icon_x, icon_y))
        ring = ImageDraw.Draw(card_rgba)
        ring.ellipse((icon_x - 3, icon_y - 3, icon_x + 90, icon_y + 90), outline=(130, 190, 236, 200), width=3)
        card = card_rgba.convert("RGB")

    buffer = BytesIO()
    card.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return buffer


def generate_myscore_card(
    display_name,
    stars,
    calls,
    avg_text,
    best_text,
    hit_rate_pct,
    score_value,
    rug_text,
):
    width, height = 1200, 440
    card = Image.new("RGB", (width, height), (14, 22, 38))
    draw_vertical_gradient(card, (12, 28, 46), (23, 54, 78))

    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((60, 28, 1140, 412), radius=28, fill=(8, 18, 34, 175), outline=(83, 138, 189, 135), width=2)
    od.ellipse((700, -90, 1170, 320), fill=(59, 130, 246, 48))
    card = Image.alpha_composite(card.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(card)

    title_font = load_font(46, bold=True)
    block_font = load_font(30, bold=True)
    stat_font = load_font(42, bold=True)
    sub_font = load_font(26, bold=False)

    left_x = 88
    right_x = 680
    right_w = 380

    safe_name = fit_text(draw, ascii_safe(display_name, fallback="Caller"), block_font, 540)
    safe_stars = ascii_safe(stars, fallback="")

    draw.text((left_x, 54), "YOUR PERFORMANCE", font=title_font, fill=(241, 247, 255))
    draw.text((left_x + 2, 112), f"{safe_name} {safe_stars}".strip(), font=sub_font, fill=(161, 203, 235))
    draw.text((left_x, 175), fit_text(draw, f"Calls {calls}", block_font, 540), font=block_font, fill=(255, 255, 255))
    draw.text((left_x, 228), fit_text(draw, f"Avg {avg_text}", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 276), fit_text(draw, f"Best {best_text}", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 330), fit_text(draw, f"Hit Rate {hit_rate_pct:.1f}%", block_font, 540), font=block_font, fill=(217, 236, 255))

    draw.text((right_x, 180), "Score", font=block_font, fill=(204, 231, 255))
    draw.text((right_x, 228), fit_text(draw, f"{score_value:.1f}/100", stat_font, right_w), font=stat_font, fill=(255, 255, 255))
    draw.text((right_x, 286), fit_text(draw, rug_text, block_font, right_w), font=block_font, fill=(217, 236, 255))
    draw.text((right_x, 338), "Auto-generated by Yabai Bot", font=sub_font, fill=(130, 170, 205))

    buffer = BytesIO()
    card.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return buffer


def build_circle_avatar(image, diameter):
    if image is None:
        return None
    img = image.convert("RGB")
    width, height = img.size
    side = min(width, height)
    left = (width - side) // 2
    top = (height - side) // 2
    img = img.crop((left, top, left + side, top + side))
    try:
        resample = Image.Resampling.LANCZOS
    except AttributeError:
        resample = Image.LANCZOS
    img = img.resize((diameter, diameter), resample=resample)

    mask = Image.new("L", (diameter, diameter), 0)
    mask_draw = ImageDraw.Draw(mask)
    mask_draw.ellipse((0, 0, diameter - 1, diameter - 1), fill=255)

    avatar_rgba = Image.new("RGBA", (diameter, diameter), (0, 0, 0, 0))
    avatar_rgba.paste(img, (0, 0))
    avatar_rgba.putalpha(mask)
    return avatar_rgba


def generate_caller_profile_card(
    display_name,
    stars,
    calls,
    avg_text,
    best_text,
    hit_rate_pct,
    score_value,
    rug_text,
    badges_text,
    avatar_image=None,
):
    width, height = 1200, 440
    card = Image.new("RGB", (width, height), (14, 22, 38))
    draw_vertical_gradient(card, (12, 28, 46), (23, 54, 78))

    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((60, 28, 1140, 412), radius=28, fill=(8, 18, 34, 175), outline=(83, 138, 189, 135), width=2)
    od.ellipse((700, -90, 1170, 320), fill=(59, 130, 246, 48))
    card = Image.alpha_composite(card.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(card)

    title_font = load_font(46, bold=True)
    block_font = load_font(30, bold=True)
    stat_font = load_font(42, bold=True)
    sub_font = load_font(26, bold=False)

    left_x = 88
    right_x = 680
    right_w = 380

    safe_name = fit_text(draw, ascii_safe(display_name, fallback="Caller"), block_font, 540)
    safe_stars = ascii_safe(stars, fallback="")

    draw.text((left_x, 54), "CALLER PROFILE", font=title_font, fill=(241, 247, 255))
    draw.text((left_x + 2, 112), f"{safe_name} {safe_stars}".strip(), font=sub_font, fill=(161, 203, 235))
    draw.text((left_x, 175), fit_text(draw, f"Calls {calls}", block_font, 540), font=block_font, fill=(255, 255, 255))
    draw.text((left_x, 228), fit_text(draw, f"Avg {avg_text}", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 276), fit_text(draw, f"Best {best_text}", stat_font, 530), font=stat_font, fill=(141, 255, 113))
    draw.text((left_x, 330), fit_text(draw, f"Hit Rate {hit_rate_pct:.1f}%", block_font, 540), font=block_font, fill=(217, 236, 255))

    draw.text((right_x, 168), "Score", font=block_font, fill=(204, 231, 255))
    draw.text((right_x, 214), fit_text(draw, f"{score_value:.1f}/100", stat_font, right_w), font=stat_font, fill=(255, 255, 255))
    draw.text((right_x, 264), fit_text(draw, rug_text, block_font, right_w), font=block_font, fill=(217, 236, 255))
    draw.text((right_x, 308), fit_text(draw, f"Badges: {badges_text}", sub_font, right_w), font=sub_font, fill=(217, 236, 255))

    avatar = build_circle_avatar(avatar_image, 118) if avatar_image is not None else None
    if avatar is not None:
        card_rgba = card.convert("RGBA")
        card_rgba.alpha_composite(avatar, (right_x + right_w - 118, 44))
        card = card_rgba.convert("RGB")

    buffer = BytesIO()
    card.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return buffer


def generate_leaderboard_spotlight_card(
    title,
    top_name,
    top_avg,
    top_best,
    top_win_rate,
    highlight_text,
    highlight_label="Best Win Window",
    theme="leaderboard",
    group_avatar_image=None,
):
    width, height = 1200, 440
    card = Image.new("RGB", (width, height), (14, 22, 38))

    if theme == "danger":
        draw_vertical_gradient(card, (60, 8, 16), (108, 18, 22))
        glow_color = (239, 68, 68, 55)
        panel_fill = (36, 7, 11, 175)
        panel_outline = (215, 85, 96, 145)
        stat_color = (255, 142, 142)
        heading = "WALL OF SHAME SPOTLIGHT"
    else:
        draw_vertical_gradient(card, (12, 28, 46), (23, 54, 78))
        glow_color = (59, 130, 246, 48)
        panel_fill = (8, 18, 34, 175)
        panel_outline = (83, 138, 189, 135)
        stat_color = (141, 255, 113)
        heading = "LEADERBOARD SPOTLIGHT"

    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((60, 28, 1140, 412), radius=28, fill=panel_fill, outline=panel_outline, width=2)
    od.ellipse((700, -90, 1170, 320), fill=glow_color)
    card = Image.alpha_composite(card.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(card)

    title_font = load_font(46, bold=True)
    block_font = load_font(30, bold=True)
    stat_font = load_font(42, bold=True)
    sub_font = load_font(26, bold=False)

    left_x = 88
    right_x = 680
    right_w = 380

    safe_title = fit_text(draw, title, sub_font, 540)
    safe_name = fit_text(draw, top_name, block_font, 500)

    draw.text((left_x, 54), heading, font=title_font, fill=(241, 247, 255))
    draw.text((left_x + 2, 112), safe_title, font=sub_font, fill=(161, 203, 235))

    draw.text((left_x, 175), f"#1 {safe_name}", font=block_font, fill=(255, 255, 255))
    draw.text((left_x, 228), fit_text(draw, f"Avg {top_avg}", stat_font, 530), font=stat_font, fill=stat_color)
    draw.text((left_x, 276), fit_text(draw, f"Best {top_best}", stat_font, 530), font=stat_font, fill=stat_color)
    draw.text((left_x, 330), f"Hit Rate {top_win_rate:.1f}%", font=block_font, fill=(217, 236, 255))

    draw.text((right_x, 180), highlight_label, font=block_font, fill=(204, 231, 255))
    lines = wrap_text_lines(draw, ascii_safe(highlight_text, fallback="N/A"), sub_font, right_w, max_lines=4)
    y = 228
    for line in lines:
        draw.text((right_x, y), line, font=sub_font, fill=(255, 255, 255))
        y += 34

    group_avatar = build_circle_avatar(group_avatar_image, 84) if group_avatar_image is not None else None
    if group_avatar is not None:
        icon_x, icon_y = 1042, 46
        ring_color = (215, 105, 115, 210) if theme == "danger" else (130, 190, 236, 210)
        card_rgba = card.convert("RGBA")
        card_rgba.alpha_composite(group_avatar, (icon_x, icon_y))
        ring = ImageDraw.Draw(card_rgba)
        ring.ellipse((icon_x - 3, icon_y - 3, icon_x + 86, icon_y + 86), outline=ring_color, width=3)
        card = card_rgba.convert("RGB")

    buffer = BytesIO()
    card.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return buffer


def get_dexscreener_batch_meta(cas_list):
    results = {}
    if not cas_list:
        return results

    now_ts = time.time()
    unique_cas = []
    seen = set()
    for ca in cas_list:
        ca_norm = normalize_ca(ca)
        if not ca_norm or ca_norm in seen:
            continue
        seen.add(ca_norm)
        unique_cas.append(ca_norm)

    to_fetch = []
    for ca_norm in unique_cas:
        cached = _dex_meta_cache.get(ca_norm)
        if cached and cached.get("expires_at", 0) > now_ts:
            cached_value = cached.get("value")
            if cached_value:
                results[ca_norm] = dict(cached_value)
            continue
        if cached:
            _dex_meta_cache.pop(ca_norm, None)
        to_fetch.append(ca_norm)

    if not to_fetch:
        return results

    def _num(value):
        try:
            return float(value or 0)
        except (TypeError, ValueError):
            return 0.0

    for i in range(0, len(to_fetch), 30):
        chunk = to_fetch[i:i + 30]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(chunk)}"
        chunk_map = {}
        try:
            response = requests.get(url, timeout=10)
            response.raise_for_status()
            payload = response.json()
            if payload and payload.get("pairs"):
                for pair in payload["pairs"]:
                    address = pair.get("baseToken", {}).get("address")
                    symbol = pair.get("baseToken", {}).get("symbol") or ""
                    liquidity_usd = _num((pair.get("liquidity") or {}).get("usd"))
                    volume_h1 = _num((pair.get("volume") or {}).get("h1"))
                    volume_h24 = _num((pair.get("volume") or {}).get("h24"))
                    market_cap = _num(pair.get("marketCap"))
                    fdv = _num(pair.get("fdv"))
                    metric = market_cap if market_cap > 0 else fdv
                    if address and metric > 0:
                        addr_lower = address.lower()
                        score = (liquidity_usd, volume_h24, metric)
                        prev = chunk_map.get(addr_lower)
                        if not prev or score > prev["_score"]:
                            chunk_map[addr_lower] = {
                                "fdv": float(metric),
                                "symbol": symbol.upper() if symbol else "",
                                "volume_h1": float(volume_h1),
                                "volume_h24": float(volume_h24),
                                "_score": score,
                            }
        except Exception as exc:
            print(f"DexScreener batch fetch error: {exc}")

        expires_at = time.time() + DEX_CACHE_TTL_SECONDS
        for ca_norm in chunk:
            value = chunk_map.get(ca_norm)
            if value:
                value = dict(value)
                value.pop("_score", None)
                results[ca_norm] = value
                _dex_meta_cache[ca_norm] = {"value": value, "expires_at": expires_at}
            else:
                # Cache misses briefly to suppress repeated lookups for dead/invalid CAs.
                _dex_meta_cache[ca_norm] = {"value": None, "expires_at": expires_at}

    if len(_dex_meta_cache) > DEX_CACHE_MAX_ENTRIES:
        stale_keys = [key for key, entry in _dex_meta_cache.items() if entry.get("expires_at", 0) <= now_ts]
        for key in stale_keys:
            _dex_meta_cache.pop(key, None)
        while len(_dex_meta_cache) > DEX_CACHE_MAX_ENTRIES:
            _dex_meta_cache.pop(next(iter(_dex_meta_cache)), None)

    return results


def get_dexscreener_batch(cas_list):
    meta = get_dexscreener_batch_meta(cas_list)
    return {addr: data["fdv"] for addr, data in meta.items()}


def _to_utc_datetime(value):
    if isinstance(value, datetime):
        return value if value.tzinfo is not None else value.replace(tzinfo=timezone.utc)
    return None


def _historical_ath_cache_key(ca_norm, time_from, time_to):
    return str(ca_norm or "").strip().lower(), int(time_from or 0), int(int(time_to or 0) // 300)


def get_solanatracker_ath_range(ca_norm, time_from, time_to):
    if not HISTORICAL_ATH_ENABLED:
        return None

    ca_norm = normalize_ca(ca_norm or "")
    time_from = int(time_from or 0)
    time_to = int(time_to or 0)
    if not ca_norm or time_from <= 0 or time_to <= time_from:
        return None

    now_ts = time.time()
    cache_key = _historical_ath_cache_key(ca_norm, time_from, time_to)
    cached = _historical_ath_cache.get(cache_key)
    if cached and cached.get("expires_at", 0) > now_ts:
        return cached.get("value")

    value = None
    try:
        response = requests.get(
            "https://data.solanatracker.io/price/history/range",
            headers={"x-api-key": SOLANA_TRACKER_API_KEY},
            params={
                "token": ca_norm,
                "time_from": time_from,
                "time_to": time_to,
            },
            timeout=HISTORICAL_ATH_REQUEST_TIMEOUT_SECONDS,
        )
        if response.ok:
            payload = response.json() or {}
            highest = ((payload.get("price") or {}).get("highest") or {})
            marketcap = float(highest.get("marketcap", 0) or 0.0)
            ath_time = int(highest.get("time", 0) or 0)
            if marketcap > 0 and ath_time > 0:
                value = {
                    "marketcap": marketcap,
                    "time": ath_time,
                    "source": "solanatracker_range",
                }
        elif response.status_code not in {404, 422}:
            print(f"Historical ATH fetch error ({response.status_code}) for {ca_norm}")
    except Exception as exc:
        print(f"Historical ATH fetch exception for {ca_norm}: {exc}")

    _historical_ath_cache[cache_key] = {
        "value": value,
        "expires_at": now_ts + HISTORICAL_ATH_CACHE_TTL_SECONDS,
    }
    if len(_historical_ath_cache) > 4000:
        stale_keys = [key for key, row in _historical_ath_cache.items() if row.get("expires_at", 0) <= now_ts]
        for key in stale_keys:
            _historical_ath_cache.pop(key, None)
        while len(_historical_ath_cache) > 4000:
            _historical_ath_cache.pop(next(iter(_historical_ath_cache)), None)

    return value


def should_reconcile_historical_ath(call_doc, force=False, now=None):
    if not HISTORICAL_ATH_ENABLED:
        return False
    if not call_doc:
        return False
    if not (call_doc.get("ca_norm") or call_doc.get("ca")):
        return False

    ts = _to_utc_datetime(call_doc.get("timestamp"))
    if ts is None:
        return False
    now = _to_utc_datetime(now) or utc_now()
    age_seconds = max(0.0, (now - ts).total_seconds())
    if age_seconds < HISTORICAL_ATH_MIN_CALL_AGE_SECONDS:
        return False
    if force:
        return True

    last_checked = _to_utc_datetime(call_doc.get("last_hist_ath_checked_at"))
    if last_checked is None:
        return True
    return (now - last_checked).total_seconds() >= (HISTORICAL_ATH_RECHECK_MINUTES * 60)


def reconcile_calls_with_historical_ath(call_entries, limit=0, force=False):
    stats = {"checked": 0, "updated": 0}
    if not HISTORICAL_ATH_ENABLED or not call_entries or limit == 0:
        return stats

    now = utc_now()
    deduped = []
    seen_keys = set()
    for entry in call_entries:
        if not isinstance(entry, dict):
            continue
        call_doc = entry.get("call")
        collection_name = entry.get("collection")
        if collection_name not in {"live", "archive"} or not call_doc or call_doc.get("_id") is None:
            continue
        dedupe_key = (collection_name, call_doc.get("_id"))
        if dedupe_key in seen_keys:
            continue
        seen_keys.add(dedupe_key)
        deduped.append(entry)

    if limit > 0:
        deduped = deduped[:int(limit)]

    for entry in deduped:
        call_doc = entry["call"]
        if not should_reconcile_historical_ath(call_doc, force=force, now=now):
            continue

        ts = _to_utc_datetime(call_doc.get("timestamp"))
        time_from = int(ts.timestamp()) if ts is not None else 0
        time_to = int(now.timestamp())
        if time_from <= 0 or time_to <= time_from:
            continue

        stats["checked"] += 1
        hist = get_solanatracker_ath_range(
            call_doc.get("ca_norm", normalize_ca(call_doc.get("ca", ""))),
            time_from=time_from,
            time_to=time_to,
        )

        collection = calls_collection if entry["collection"] == "live" else calls_archive_collection
        set_fields = {
            "last_hist_ath_checked_at": now,
            "last_hist_ath_provider": HISTORICAL_ATH_PROVIDER,
        }

        initial_val = float(call_doc.get("initial_mcap", 0) or 0)
        old_ath_val = float(call_doc.get("ath_mcap", 0) or 0)
        old_current_val = float(call_doc.get("current_mcap", initial_val) or initial_val)
        old_x_peak = (max(old_ath_val, old_current_val) / initial_val) if initial_val > 0 else 0.0
        new_ath_val = old_ath_val

        if hist:
            hist_ath = float(hist.get("marketcap", 0) or 0)
            if hist_ath > new_ath_val:
                new_ath_val = hist_ath
                ath_seen_at = datetime.fromtimestamp(int(hist.get("time")), tz=timezone.utc)
                set_fields.update(
                    {
                        "ath_mcap": hist_ath,
                        "ath_seen_at": ath_seen_at,
                        "ath_source": hist.get("source", "solanatracker_range"),
                    }
                )

        result = collection.update_one({"_id": call_doc["_id"]}, {"$set": set_fields})
        call_doc["last_hist_ath_checked_at"] = now
        call_doc["last_hist_ath_provider"] = HISTORICAL_ATH_PROVIDER
        if new_ath_val > old_ath_val + 1e-12:
            new_x_peak = (max(new_ath_val, old_current_val) / initial_val) if initial_val > 0 else 0.0
            upsert_rollup_for_call_peak_delta(call_doc, old_x_peak, new_x_peak)
            call_doc["ath_mcap"] = new_ath_val
            if "ath_seen_at" in set_fields:
                call_doc["ath_seen_at"] = set_fields["ath_seen_at"]
                call_doc["ath_source"] = set_fields["ath_source"]
            stats["updated"] += 1

    return stats


def build_historical_reconcile_entries(call_docs, collection_name, protected_ids=None, sort_by_recent=True):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    entries = []
    for call in call_docs or []:
        ts = _to_utc_datetime(call.get("timestamp")) or datetime.min.replace(tzinfo=timezone.utc)
        checked_at = _to_utc_datetime(call.get("last_hist_ath_checked_at")) or datetime.min.replace(tzinfo=timezone.utc)
        entries.append(
            {
                "call": call,
                "collection": collection_name,
                "_sort": (
                    0 if call.get("_id") in protected_ids else 1,
                    0 if call.get("last_hist_ath_checked_at") is None else 1,
                    -int(ts.timestamp()) if sort_by_recent else int(checked_at.timestamp()),
                    -call_peak_x(call),
                ),
            }
        )
    entries.sort(key=lambda row: row.get("_sort"))
    for entry in entries:
        entry.pop("_sort", None)
    return entries


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
    profitable_peak = 0
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
        if x_ath > 1.0:
            profitable_peak += 1

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
    profitable_rate = profitable_peak / n
    reputation = compute_performance_score(
        calls=n,
        avg_x=1.0 + avg_ath,
        win_rate=win_rate,
        profitable_rate=profitable_rate,
        best_x=best_x,
    )

    badges = []
    if best_x >= 100.0:
        badges.append("100x Legend")
    elif best_x >= 25.0:
        badges.append("Moonshot")
    elif best_x >= 10.0:
        badges.append("Sniper")
    if n >= 10 and win_rate >= 0.60:
        badges.append("High Hit Rate")
    if n >= 5 and avg_ath > 0:
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
    total = 0
    eligible = 0
    rug_count = 0
    now = utc_now()

    for call in calls:
        initial = float(call.get("initial_mcap", 0) or 0)
        if initial <= 0:
            continue
        total += 1

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

    rug_rate = (rug_count / total) * 100.0 if total > 0 else 0.0
    return {
        "rug_rate": rug_rate,
        "rug_count": rug_count,
        "total": total,
        "eligible": eligible,
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
    if existing is not None:
        return True
    archived = calls_archive_collection.find_one(
        {
            "chat_id": chat_id,
            "ca_norm": ca_norm,
            "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
        },
        {"_id": 1},
    )
    return archived is not None


def get_caller_key(call_doc):
    caller_id = call_doc.get("caller_id")
    if caller_id is not None:
        return f"id:{caller_id}"
    legacy_name = (call_doc.get("caller_name") or "unknown").strip().lower()
    return f"legacy:{legacy_name}"


def caller_key_query(chat_id, caller_key, caller_name=None):
    if str(caller_key or "").startswith("id:"):
        try:
            caller_id = int(str(caller_key).split(":", 1)[1])
            return _accepted_query(chat_id, {"caller_id": caller_id})
        except (TypeError, ValueError):
            pass
    legacy_name = str(caller_name or "")
    return _accepted_query(chat_id, {"caller_name": {"$regex": f"^{re.escape(legacy_name)}$", "$options": "i"}})


def resolve_caller_identity(chat_id, target):
    target_clean = str(target or "").strip().lstrip("@")
    if not target_clean:
        return {"target": "", "caller_id": None, "query": None}

    query_by_id = None
    try:
        target_id = int(target_clean)
        query_by_id = {"chat_id": chat_id, "caller_id": target_id}
    except ValueError:
        target_id = None

    if query_by_id:
        any_doc = calls_collection.find_one(_accepted_query(chat_id, {"caller_id": target_id}), {"caller_id": 1}) or \
            calls_archive_collection.find_one(_accepted_query(chat_id, {"caller_id": target_id}), {"caller_id": 1})
        if any_doc:
            return {
                "target": target_clean,
                "caller_id": target_id,
                "query": {"chat_id": chat_id, "caller_id": target_id},
            }

    profile = user_profiles_collection.find_one(
        {
            "chat_id": chat_id,
            "$or": [
                {"username": {"$regex": f"^{re.escape(target_clean)}$", "$options": "i"}},
                {"display_name": {"$regex": f"^{re.escape(target_clean)}$", "$options": "i"}},
            ],
        },
        {"user_id": 1},
    ) or {}
    profile_id = profile.get("user_id")
    if profile_id is not None:
        return {
            "target": target_clean,
            "caller_id": profile_id,
            "query": {"chat_id": chat_id, "caller_id": profile_id},
        }

    name_query = {
        "chat_id": chat_id,
        "$and": [
            {
                "$or": [
                    {"caller_name": {"$regex": f"^{re.escape(target_clean)}$", "$options": "i"}},
                    {"caller_username": {"$regex": f"^{re.escape(target_clean)}$", "$options": "i"}},
                ]
            }
        ],
    }
    return {
        "target": target_clean,
        "caller_id": None,
        "query": name_query,
    }


def enrich_calls_with_live_meta(calls, limit=CALLER_LIVE_METRIC_REFRESH_LIMIT):
    if not calls:
        return []
    enriched = [dict(call) for call in calls]
    target = enriched[:max(0, int(limit))]
    cas = []
    seen = set()
    for call in target:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        if not ca_norm or ca_norm in seen:
            continue
        seen.add(ca_norm)
        cas.append(ca_norm)
    meta_map = get_dexscreener_batch_meta(cas)
    for call in target:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        meta = meta_map.get(ca_norm, {})
        live_fdv = float(meta.get("fdv", 0) or 0)
        if live_fdv > 0:
            call["current_mcap"] = live_fdv
            old_ath = float(call.get("ath_mcap", live_fdv) or live_fdv)
            call["ath_mcap"] = max(old_ath, live_fdv)
        if meta.get("symbol"):
            call["token_symbol"] = meta.get("symbol")
    return enriched


def build_kick_list_text(chat_id, avg_x_threshold=KICKLIST_MAX_AVG_X, min_calls=KICKLIST_MIN_CALLS, limit=KICKLIST_LIMIT):
    ensure_rollups_ready(chat_id)
    rows = list(
        caller_rollups_collection.find(
            {
                "chat_id": chat_id,
                "calls": {"$gte": int(min_calls)},
                "avg_x": {"$lt": float(avg_x_threshold)},
            },
            {
                "_id": 0,
                "caller_key": 1,
                "caller_id": 1,
                "name": 1,
                "calls": 1,
                "avg_x": 1,
                "best_x": 1,
                "win_rate": 1,
            },
        )
        .sort([("avg_x", ASCENDING), ("calls", DESCENDING)])
        .limit(int(limit))
    )
    if not rows:
        return (
            "Kick Watchlist\n"
            "----------------\n"
            f"No callers with >= {min_calls} calls and avg below {format_return(avg_x_threshold)}."
        )

    lines = [
        "Kick Watchlist",
        "----------------",
        f"Avg threshold: below {format_return(avg_x_threshold)}",
    ]
    for idx, row in enumerate(rows, start=1):
        caller_query = caller_key_query(chat_id, row.get("caller_key"), row.get("name"))
        calls = list(calls_collection.find(caller_query)) + list(calls_archive_collection.find(caller_query))
        rug = derive_rug_stats(calls)
        lines.append(
            f"{idx}. {row.get('name', 'Unknown')} | Calls {int(row.get('calls', 0) or 0)} | "
            f"Avg {format_return(float(row.get('avg_x', 0) or 0))} | "
            f"Rugs {rug['rug_count']}/{rug['total']}"
        )
    return "\n".join(lines)


def call_peak_x(call_doc):
    initial = float(call_doc.get("initial_mcap", 0) or 0)
    if initial <= 0:
        return 0.0
    current = float(call_doc.get("current_mcap", initial) or initial)
    ath = float(call_doc.get("ath_mcap", initial) or initial)
    return max(ath, current) / initial


def _refresh_rollup_rates(chat_id, caller_key):
    avg_x_expr = {
        "$cond": [
            {"$gt": ["$calls", 0]},
            {"$divide": ["$sum_x_peak", "$calls"]},
            0,
        ]
    }
    caller_rollups_collection.update_one(
        {"chat_id": chat_id, "caller_key": caller_key},
        [
            {
                "$set": {
                    "avg_x": avg_x_expr,
                    "win_rate": {
                        "$multiply": [
                            100,
                            {
                                "$cond": [
                                    {"$gt": ["$calls", 0]},
                                    {"$divide": ["$wins", "$calls"]},
                                    0,
                                ]
                            },
                        ]
                    },
                    "profitable_rate": {
                        "$multiply": [
                            100,
                            {
                                "$cond": [
                                    {"$gt": ["$calls", 0]},
                                    {"$divide": ["$profitables", "$calls"]},
                                    0,
                                ]
                            },
                        ]
                    },
                    "score": mongo_performance_score_expr(
                        calls_expr="$calls",
                        wins_expr="$wins",
                        profitables_expr="$profitables",
                        avg_x_expr=avg_x_expr,
                        best_x_expr="$best_x",
                    ),
                }
            }
        ],
    )


def apply_rollup_delta(
    chat_id,
    caller_id,
    caller_name,
    caller_key,
    delta_calls=0,
    delta_wins=0,
    delta_profitables=0,
    delta_sum_x_peak=0.0,
    best_x_candidate=0.0,
):
    if not caller_key:
        return
    now = utc_now()
    update_doc = {
        "$set": {
            "chat_id": chat_id,
            "caller_key": caller_key,
            "caller_id": caller_id,
            "name": caller_name or "Unknown",
            "updated_at": now,
        },
        "$setOnInsert": {
            "avg_x": 0.0,
            "win_rate": 0.0,
            "profitable_rate": 0.0,
            "score": 0.0,
        },
        "$max": {"best_x": float(best_x_candidate or 0.0)},
    }
    inc_doc = {}
    if delta_calls:
        inc_doc["calls"] = int(delta_calls)
    if delta_wins:
        inc_doc["wins"] = int(delta_wins)
    if delta_profitables:
        inc_doc["profitables"] = int(delta_profitables)
    if abs(float(delta_sum_x_peak or 0.0)) > 1e-12:
        inc_doc["sum_x_peak"] = float(delta_sum_x_peak)
    if inc_doc:
        update_doc["$inc"] = inc_doc
    caller_rollups_collection.update_one(
        {"chat_id": chat_id, "caller_key": caller_key},
        update_doc,
        upsert=True,
    )
    _refresh_rollup_rates(chat_id, caller_key)


def upsert_rollup_for_call_insert(call_doc):
    chat_id = int(call_doc.get("chat_id"))
    caller_key = get_caller_key(call_doc)
    x_peak = call_peak_x(call_doc)
    if x_peak <= 0:
        return
    apply_rollup_delta(
        chat_id=chat_id,
        caller_id=call_doc.get("caller_id"),
        caller_name=call_doc.get("caller_name", "Unknown"),
        caller_key=caller_key,
        delta_calls=1,
        delta_wins=1 if x_peak >= WIN_MULTIPLIER else 0,
        delta_profitables=1 if x_peak > 1.0 else 0,
        delta_sum_x_peak=x_peak,
        best_x_candidate=x_peak,
    )


def upsert_rollup_for_call_peak_delta(call_doc, old_x_peak, new_x_peak):
    delta = float(new_x_peak or 0.0) - float(old_x_peak or 0.0)
    if delta <= 1e-12:
        return
    caller_key = get_caller_key(call_doc)
    delta_wins = 1 if (old_x_peak < WIN_MULTIPLIER <= new_x_peak) else 0
    delta_profitables = 1 if (old_x_peak <= 1.0 < new_x_peak) else 0
    apply_rollup_delta(
        chat_id=int(call_doc.get("chat_id")),
        caller_id=call_doc.get("caller_id"),
        caller_name=call_doc.get("caller_name", "Unknown"),
        caller_key=caller_key,
        delta_calls=0,
        delta_wins=delta_wins,
        delta_profitables=delta_profitables,
        delta_sum_x_peak=delta,
        best_x_candidate=new_x_peak,
    )


def recompute_rollups_for_chat(chat_id):
    match_query = accepted_call_filter(chat_id)
    pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "caller_id": 1,
                "caller_name": 1,
                "initial_mcap": 1,
                "ath_mcap": 1,
                "current_mcap": 1,
            }
        },
        {
            "$unionWith": {
                "coll": "token_calls_archive",
                "pipeline": [
                    {"$match": match_query},
                    {
                        "$project": {
                            "caller_id": 1,
                            "caller_name": 1,
                            "initial_mcap": 1,
                            "ath_mcap": 1,
                            "current_mcap": 1,
                        }
                    },
                ],
            }
        },
        {
            "$addFields": {
                "_initial": {"$toDouble": {"$ifNull": ["$initial_mcap", 0]}},
                "_ath": {"$toDouble": {"$ifNull": ["$ath_mcap", 0]}},
                "_current": {"$toDouble": {"$ifNull": ["$current_mcap", 0]}},
                "_name": {"$ifNull": ["$caller_name", "Unknown"]},
            }
        },
        {"$match": {"_initial": {"$gt": 0}}},
        {"$addFields": {"_peak": {"$cond": [{"$gt": ["$_ath", "$_current"]}, "$_ath", "$_current"]}}},
        {
            "$addFields": {
                "_x_peak": {"$divide": ["$_peak", "$_initial"]},
                "_caller_key": _mongo_caller_key_expr(),
            }
        },
        {
            "$group": {
                "_id": "$_caller_key",
                "caller_id": {"$first": "$caller_id"},
                "name": {"$first": "$_name"},
                "calls": {"$sum": 1},
                "wins": {"$sum": {"$cond": [{"$gte": ["$_x_peak", WIN_MULTIPLIER]}, 1, 0]}},
                "profitables": {"$sum": {"$cond": [{"$gt": ["$_x_peak", 1]}, 1, 0]}},
                "sum_x_peak": {"$sum": "$_x_peak"},
                "best_x": {"$max": "$_x_peak"},
            }
        },
    ]
    rows = list(calls_collection.aggregate(pipeline, allowDiskUse=True))
    caller_rollups_collection.delete_many({"chat_id": chat_id})
    if not rows:
        return 0

    docs = []
    now = utc_now()
    for row in rows:
        calls = int(row.get("calls", 0) or 0)
        wins = int(row.get("wins", 0) or 0)
        profitables = int(row.get("profitables", 0) or 0)
        sum_x_peak = float(row.get("sum_x_peak", 0) or 0.0)
        avg_x = (sum_x_peak / calls) if calls > 0 else 0.0
        win_rate_ratio = (wins / calls) if calls > 0 else 0.0
        profitable_rate_ratio = (profitables / calls) if calls > 0 else 0.0
        score = compute_performance_score(
            calls=calls,
            avg_x=avg_x,
            win_rate=win_rate_ratio,
            profitable_rate=profitable_rate_ratio,
            best_x=float(row.get("best_x", 0) or 0.0),
        )
        docs.append(
            {
                "chat_id": chat_id,
                "caller_key": row.get("_id"),
                "caller_id": row.get("caller_id"),
                "name": row.get("name", "Unknown"),
                "calls": calls,
                "wins": wins,
                "profitables": profitables,
                "sum_x_peak": sum_x_peak,
                "best_x": float(row.get("best_x", 0) or 0.0),
                "avg_x": float(avg_x),
                "win_rate": float(win_rate_ratio * 100.0),
                "profitable_rate": float(profitable_rate_ratio * 100.0),
                "score": float(score),
                "updated_at": now,
            }
        )
    if docs:
        caller_rollups_collection.insert_many(docs, ordered=False)
    return len(docs)


def ensure_rollups_ready(chat_id):
    setting = settings_collection.find_one({"chat_id": chat_id}, {"rollup_version": 1}) or {}
    if int(setting.get("rollup_version", 0) or 0) >= ROLLUP_SCHEMA_VERSION:
        return
    recompute_rollups_for_chat(chat_id)
    settings_collection.update_one(
        {"chat_id": chat_id},
        {"$set": {"rollup_version": ROLLUP_SCHEMA_VERSION}},
        upsert=True,
    )


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
    merged = list(settings_ids or []) + list(call_ids or [])
    ids = set()
    for raw_chat_id in merged:
        normalized = canonical_chat_id(raw_chat_id)
        if isinstance(normalized, int):
            ids.add(normalized)
    return list(ids)


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


def refresh_cache_key(chat_id, time_filter, is_bottom, page, items_per_page):
    time_filter = time_filter or {}
    timestamp_filter = time_filter.get("timestamp") or {}
    ts_from = timestamp_filter.get("$gte")
    ts_key = ts_from.astimezone(timezone.utc).isoformat() if isinstance(ts_from, datetime) else ""
    return (
        int(chat_id),
        bool(is_bottom),
        ts_key,
        int(page or 0),
        int(items_per_page or 0),
    )


def get_leaderboard_page_cache(chat_id, time_filter, is_bottom, page, items_per_page):
    key = refresh_cache_key(chat_id, time_filter, is_bottom, page, items_per_page)
    now_ts = time.time()
    row = _leaderboard_page_cache.get(key)
    if not row:
        return None
    if row.get("expires_at", 0) <= now_ts:
        _leaderboard_page_cache.pop(key, None)
        return None
    return row.get("value")


def set_leaderboard_page_cache(chat_id, time_filter, is_bottom, page, items_per_page, value):
    key = refresh_cache_key(chat_id, time_filter, is_bottom, page, items_per_page)
    now_ts = time.time()
    _leaderboard_page_cache[key] = {
        "value": value,
        "expires_at": now_ts + LEADERBOARD_CACHE_TTL_SECONDS,
    }
    if len(_leaderboard_page_cache) > 800:
        stale = [k for k, row in _leaderboard_page_cache.items() if row.get("expires_at", 0) <= now_ts]
        for k in stale:
            _leaderboard_page_cache.pop(k, None)
        while len(_leaderboard_page_cache) > 800:
            _leaderboard_page_cache.pop(next(iter(_leaderboard_page_cache)), None)


def invalidate_leaderboard_cache(chat_id):
    target = int(chat_id)
    keys = [k for k in _leaderboard_page_cache.keys() if int(k[0]) == target]
    for k in keys:
        _leaderboard_page_cache.pop(k, None)


def call_current_x(call_doc):
    initial = float(call_doc.get("initial_mcap", 0) or 0)
    if initial <= 0:
        return 0.0
    current = float(call_doc.get("current_mcap", initial) or initial)
    return current / initial


def compute_call_refresh_state(call_doc, now=None):
    now = _to_utc_datetime(now) or utc_now()
    ts = _to_utc_datetime(call_doc.get("timestamp")) or now
    age_hours = max(0.0, (now - ts).total_seconds() / 3600.0)
    x_peak = call_peak_x(call_doc)
    current_x = call_current_x(call_doc)
    volume_h1 = float(call_doc.get("volume_h1", call_doc.get("volume_h24", 0)) or 0.0)
    repost_count = int(call_doc.get("repost_count", 0) or 0)
    repost_hours = _hours_since(_to_utc_datetime(call_doc.get("last_reposted_at")))
    ath_change_hours = _hours_since(_to_utc_datetime(call_doc.get("last_ath_change_at")) or _to_utc_datetime(call_doc.get("ath_seen_at")))

    priority = 0.0
    priority += min(420.0, max(0.0, x_peak - 1.0) * 140.0)
    priority += min(140.0, volume_h1 / 120.0)

    if age_hours <= 1:
        priority += 180.0
    elif age_hours <= 6:
        priority += 120.0
    elif age_hours <= 24:
        priority += 70.0
    elif age_hours <= 72:
        priority += 35.0
    else:
        priority += 10.0

    if current_x >= 2.0:
        priority += 110.0
    elif current_x >= 1.2:
        priority += 50.0

    if ath_change_hours is not None:
        if ath_change_hours <= 1:
            priority += 220.0
        elif ath_change_hours <= 6:
            priority += 140.0
        elif ath_change_hours <= 24:
            priority += 70.0

    if repost_hours is not None:
        if repost_hours <= 1:
            priority += 140.0
        elif repost_hours <= 6:
            priority += 80.0
        elif repost_hours <= RUNNER_REPOST_BOOST_HOURS:
            priority += 35.0

    if repost_count > 0:
        priority += min(90.0, repost_count * 12.0)

    if volume_h1 < LOW_VOLUME_STASH_THRESHOLD:
        priority -= 65.0
    if age_hours >= 12 and current_x <= 0.5 and x_peak < 1.2:
        priority -= 140.0
    if age_hours >= 96 and x_peak < RUNNER_PROTECT_MIN_X:
        priority -= 50.0
    if bool(call_doc.get("is_stashed", False)):
        priority -= 120.0

    priority = int(max(0.0, priority))

    if ath_change_hours is not None and ath_change_hours <= 1:
        interval_seconds = HOT_REFRESH_INTERVAL_SECONDS
    elif repost_hours is not None and repost_hours <= 1:
        interval_seconds = HOT_REFRESH_INTERVAL_SECONDS
    elif x_peak >= 5.0 or (x_peak >= 2.0 and age_hours <= 24):
        interval_seconds = HOT_REFRESH_INTERVAL_SECONDS
    elif x_peak >= 2.0 or current_x >= 1.5 or age_hours <= 6:
        interval_seconds = WARM_REFRESH_INTERVAL_SECONDS
    elif x_peak >= 1.2 or age_hours <= 24 or volume_h1 >= LOW_VOLUME_STASH_THRESHOLD:
        interval_seconds = NORMAL_REFRESH_INTERVAL_SECONDS
    elif age_hours >= 12 and current_x <= 0.5 and x_peak < 1.2:
        interval_seconds = max(COLD_REFRESH_INTERVAL_SECONDS, 3600)
    else:
        interval_seconds = COLD_REFRESH_INTERVAL_SECONDS

    should_protect = False
    if x_peak >= RUNNER_PROTECT_MIN_X and age_hours <= RUNNER_PROTECT_MAX_AGE_HOURS:
        should_protect = True
    if current_x >= RUNNER_PROTECT_MIN_X and age_hours <= max(24, RUNNER_PROTECT_MAX_AGE_HOURS // 2):
        should_protect = True
    if ath_change_hours is not None and ath_change_hours <= 24 and x_peak >= 1.2:
        should_protect = True
    if repost_hours is not None and repost_hours <= RUNNER_REPOST_BOOST_HOURS and age_hours <= RUNNER_PROTECT_MAX_AGE_HOURS:
        should_protect = True

    return {
        "priority": priority,
        "interval_seconds": int(interval_seconds),
        "next_refresh_at": now + timedelta(seconds=int(interval_seconds)),
        "should_protect": bool(should_protect),
    }


def refresh_state_update_fields(call_doc, now=None):
    now = _to_utc_datetime(now) or utc_now()
    state = compute_call_refresh_state(call_doc, now=now)
    return {
        "refresh_priority": int(state["priority"]),
        "refresh_interval_seconds": int(state["interval_seconds"]),
        "next_refresh_at": state["next_refresh_at"],
    }


def should_stash_low_volume_call(call_doc, volume_h1, now=None, protected_ids=None, state=None):
    if float(volume_h1 or 0.0) >= LOW_VOLUME_STASH_THRESHOLD:
        return False
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    if call_doc.get("_id") in protected_ids:
        return False
    now = _to_utc_datetime(now) or utc_now()
    ts = _to_utc_datetime(call_doc.get("timestamp")) or now
    age_hours = max(0.0, (now - ts).total_seconds() / 3600.0)
    if age_hours < LOW_VOLUME_STASH_MIN_AGE_HOURS:
        return False
    state = state or compute_call_refresh_state(call_doc, now=now)
    if state.get("should_protect"):
        return False
    return True


def seed_refresh_queue_metadata(chat_id, limit=250):
    candidates = list(
        calls_collection.find(
            _accepted_query(
                chat_id,
                {
                    "$or": [
                        {"next_refresh_at": {"$exists": False}},
                        {"refresh_priority": {"$exists": False}},
                    ]
                },
            )
        )
        .sort("timestamp", -1)
        .limit(max(1, int(limit)))
    )
    if not candidates:
        return 0
    now = utc_now()
    updated = 0
    for call in candidates:
        fields = refresh_state_update_fields(call, now=now)
        result = calls_collection.update_one({"_id": call["_id"]}, {"$set": fields})
        updated += int(result.modified_count or 0)
    return updated


def select_runner_protected_ids(chat_id, lookback_days=REFRESH_QUEUE_LOOKBACK_DAYS, limit=RUNNER_PROTECT_MAX_CALLS):
    cutoff = utc_now() - timedelta(days=max(1, int(lookback_days or REFRESH_QUEUE_LOOKBACK_DAYS)))
    candidates = list(
        calls_collection.find(
            _accepted_query(chat_id, {"timestamp": {"$gte": cutoff}})
        )
        .sort("timestamp", -1)
        .limit(max(limit * 5, limit))
    )
    scored = []
    now = utc_now()
    for call in candidates:
        state = compute_call_refresh_state(call, now=now)
        if state["should_protect"]:
            scored.append((int(state["priority"]), call.get("_id")))
    scored.sort(key=lambda row: row[0], reverse=True)
    return {obj_id for _, obj_id in scored[:max(1, int(limit))] if obj_id is not None}


def stash_low_priority_calls(chat_id, active_limit=ACTIVE_LIVE_CALLS_PER_CHAT, protected_ids=None):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    active_query = _accepted_query(chat_id, {"is_stashed": {"$ne": True}})
    active_count = calls_collection.count_documents(active_query)
    overflow = max(0, int(active_count) - int(active_limit))
    if overflow <= 0:
        return 0

    cutoff = utc_now() - timedelta(hours=GLOBAL_STASH_MIN_AGE_HOURS)
    candidates = list(
        calls_collection.find(
            _accepted_query(
                chat_id,
                {
                    "is_stashed": {"$ne": True},
                    "timestamp": {"$lt": cutoff},
                    **({"_id": {"$nin": list(protected_ids)}} if protected_ids else {}),
                },
            )
        )
        .sort([("refresh_priority", ASCENDING), ("next_refresh_at", ASCENDING), ("timestamp", ASCENDING)])
        .limit(max(overflow * 3, overflow))
    )
    if not candidates:
        return 0

    to_stash_ids = [call["_id"] for call in candidates[:overflow] if call.get("_id") is not None]
    if not to_stash_ids:
        return 0

    result = calls_collection.update_many(
        {"_id": {"$in": to_stash_ids}},
        {
            "$set": {
                "is_stashed": True,
                "stashed_reason": "priority_queue",
                "stashed_at": utc_now(),
            }
        },
    )
    return int(result.modified_count or 0)


def load_due_refresh_calls(chat_id, protected_ids=None, limit=REFRESH_QUEUE_MAX_CALLS_PER_CHAT, lookback_days=REFRESH_QUEUE_LOOKBACK_DAYS):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    now = utc_now()
    cutoff = now - timedelta(days=max(1, int(lookback_days or REFRESH_QUEUE_LOOKBACK_DAYS)))
    due_or = [
        {"next_refresh_at": {"$lte": now}},
        {"next_refresh_at": {"$exists": False}},
    ]
    if protected_ids:
        due_or.append({"_id": {"$in": list(protected_ids)}})
    query = _accepted_query(
        chat_id,
        {
            "timestamp": {"$gte": cutoff},
            "is_stashed": {"$ne": True},
            "$or": due_or,
        },
    )
    calls = list(
        calls_collection.find(query)
        .sort([("refresh_priority", DESCENDING), ("next_refresh_at", ASCENDING), ("timestamp", DESCENDING)])
        .limit(max(1, int(limit)))
    )
    if calls:
        return calls

    fallback_query = _accepted_query(
        chat_id,
        {
            "timestamp": {"$gte": cutoff},
            "is_stashed": {"$ne": True},
        },
    )
    return list(
        calls_collection.find(fallback_query)
        .sort([("refresh_priority", DESCENDING), ("timestamp", DESCENDING)])
        .limit(max(1, int(limit)))
    )


def maybe_run_daily_rollup_repair(chat_id, now=None):
    now = _to_utc_datetime(now) or utc_now()
    if now.hour < DAILY_ROLLUP_REPAIR_HOUR_UTC:
        return False
    today = now.strftime("%Y-%m-%d")
    setting = settings_collection.find_one({"chat_id": chat_id}, {"last_rollup_rebuild_date": 1}) or {}
    if setting.get("last_rollup_rebuild_date") == today:
        return False
    recompute_rollups_for_chat(chat_id)
    settings_collection.update_one(
        {"chat_id": chat_id},
        {
            "$set": {
                "rollup_version": ROLLUP_SCHEMA_VERSION,
                "last_rollup_rebuild_date": today,
            }
        },
        upsert=True,
    )
    invalidate_leaderboard_cache(chat_id)
    return True


GLOBAL_ADMIN_USERNAMES = {"deanncrypto"}


def is_global_admin_user(user: User | None) -> bool:
    username = (getattr(user, "username", "") or "").strip().lower()
    return username in GLOBAL_ADMIN_USERNAMES


async def user_is_admin(bot, chat_id, user_id, user: User | None = None):
    if is_global_admin_user(user):
        return True
    member = await bot.get_chat_member(chat_id, user_id)
    return member.status in {"administrator", "creator"}


def ensure_group_key(chat_id):
    canonical_key = str(int(chat_id))
    settings_collection.update_one(
        {"chat_id": chat_id},
        {"$setOnInsert": {"chat_id": chat_id}, "$set": {"group_key": canonical_key}},
        upsert=True,
    )
    return canonical_key


async def resolve_target_chat_id(update: Update, context: ContextTypes.DEFAULT_TYPE, admin_required=False):
    chat = update.effective_chat
    user = update.effective_user
    message = update.effective_message

    if chat.type != "private":
        target_chat_id = chat.id
    else:
        link = private_links_collection.find_one({"user_id": user.id}) or {}
        target_chat_id = link.get("chat_id")
        if target_chat_id is None:
            await message.reply_text("No linked group. Use /linkgroup <group_key> in private chat.")
            return None

    if admin_required:
        if not await user_is_admin(context.bot, target_chat_id, user.id, user):
            await message.reply_text("Admin only command")
            return None
    return target_chat_id


def resolve_callback_target_chat_id(query) -> int | None:
    chat = getattr(query, "message", None).chat if getattr(query, "message", None) else None
    if chat and chat.type != "private":
        return chat.id
    link = private_links_collection.find_one({"user_id": query.from_user.id}) or {}
    return link.get("chat_id")


async def fetch_chat_avatar_image(bot, chat_id):
    try:
        chat = await bot.get_chat(chat_id)
        photo = getattr(chat, "photo", None)
        if not photo:
            return None
        file_id = getattr(photo, "big_file_id", None) or getattr(photo, "small_file_id", None)
        if not file_id:
            return None
        file_obj = await bot.get_file(file_id)
        data = await file_obj.download_as_bytearray()
        return Image.open(BytesIO(data)).convert("RGB")
    except Exception:
        return None


async def fetch_chat_avatar_image_cached(bot, chat_id):
    now_ts = time.time()
    cached = _chat_avatar_cache.get(chat_id)
    if cached and cached.get("expires_at", 0) > now_ts:
        return cached.get("image")

    image = await fetch_chat_avatar_image(bot, chat_id)
    _chat_avatar_cache[chat_id] = {
        "image": image,
        "expires_at": now_ts + CHAT_AVATAR_CACHE_TTL_SECONDS,
    }
    if len(_chat_avatar_cache) > 200:
        stale_keys = [key for key, row in _chat_avatar_cache.items() if row.get("expires_at", 0) <= now_ts]
        for key in stale_keys:
            _chat_avatar_cache.pop(key, None)
        while len(_chat_avatar_cache) > 200:
            _chat_avatar_cache.pop(next(iter(_chat_avatar_cache)), None)
    return image


def record_refresh_runtime(chat_id, refresh_duration_ms, refreshed_calls):
    chat_id = canonical_chat_id(chat_id)
    now = utc_now()
    by_chat = _ops_runtime.setdefault("by_chat", {})
    row = by_chat.setdefault(
        chat_id,
        {
            "last_heartbeat_at": None,
            "last_refresh_duration_ms": 0.0,
            "avg_refresh_duration_ms": 0.0,
            "refresh_runs": 0,
            "last_refreshed_calls": 0,
        },
    )
    prev_runs = int(row.get("refresh_runs", 0) or 0)
    prev_avg = float(row.get("avg_refresh_duration_ms", 0.0) or 0.0)
    new_runs = prev_runs + 1
    new_avg = ((prev_avg * prev_runs) + float(refresh_duration_ms)) / max(1, new_runs)
    row["last_heartbeat_at"] = now
    row["last_refresh_duration_ms"] = float(refresh_duration_ms)
    row["avg_refresh_duration_ms"] = float(new_avg)
    row["refresh_runs"] = new_runs
    row["last_refreshed_calls"] = int(refreshed_calls or 0)


def save_leaderboard_session(message_obj, state):
    if not message_obj:
        return
    key = (int(message_obj.chat_id), int(message_obj.message_id))
    _leaderboard_sessions[key] = {**(state or {}), "saved_at": utc_now()}
    if len(_leaderboard_sessions) > 600:
        oldest = sorted(
            _leaderboard_sessions.items(),
            key=lambda kv: kv[1].get("saved_at", utc_now()),
        )[:100]
        for old_key, _ in oldest:
            _leaderboard_sessions.pop(old_key, None)


def load_leaderboard_session(message_obj):
    if not message_obj:
        return None
    key = (int(message_obj.chat_id), int(message_obj.message_id))
    row = _leaderboard_sessions.get(key)
    if not row:
        return None
    saved_at = row.get("saved_at")
    if isinstance(saved_at, datetime) and (utc_now() - saved_at).total_seconds() > 6 * 3600:
        _leaderboard_sessions.pop(key, None)
        return None
    return dict(row)


def apply_leaderboard_state(context, state):
    if not state:
        return
    keys = [
        "leaderboard_chat_id",
        "leaderboard_time_filter",
        "leaderboard_is_bottom",
        "leaderboard_title",
        "leaderboard_total",
        "leaderboard_highlight_label",
        "leaderboard_highlight_text",
        "leaderboard_owner_id",
        "leaderboard_image_mode",
    ]
    for key in keys:
        if key in state:
            context.chat_data[key] = state[key]


def snapshot_leaderboard_state(context):
    keys = [
        "leaderboard_chat_id",
        "leaderboard_time_filter",
        "leaderboard_is_bottom",
        "leaderboard_title",
        "leaderboard_total",
        "leaderboard_highlight_label",
        "leaderboard_highlight_text",
        "leaderboard_owner_id",
        "leaderboard_image_mode",
    ]
    return {key: context.chat_data.get(key) for key in keys}


def _groupstats_cache_key(chat_id, time_arg):
    return int(chat_id), str(time_arg or "all").strip().lower()


def get_groupstats_cache(chat_id, time_arg):
    now_ts = time.time()
    key = _groupstats_cache_key(chat_id, time_arg)
    row = _groupstats_cache.get(key)
    if not row:
        return None
    if row.get("expires_at", 0) <= now_ts:
        _groupstats_cache.pop(key, None)
        return None
    return row.get("value")


def set_groupstats_cache(chat_id, time_arg, value):
    now_ts = time.time()
    key = _groupstats_cache_key(chat_id, time_arg)
    _groupstats_cache[key] = {
        "value": value,
        "expires_at": now_ts + GROUPSTATS_CACHE_TTL_SECONDS,
    }
    if len(_groupstats_cache) > 400:
        stale = [k for k, row in _groupstats_cache.items() if row.get("expires_at", 0) <= now_ts]
        for k in stale:
            _groupstats_cache.pop(k, None)
        while len(_groupstats_cache) > 400:
            _groupstats_cache.pop(next(iter(_groupstats_cache)), None)


def get_groupstats_media_cache(chat_id, time_arg):
    now_ts = time.time()
    key = _groupstats_cache_key(chat_id, time_arg)
    row = _groupstats_media_cache.get(key)
    if not row:
        return None
    if row.get("expires_at", 0) <= now_ts:
        _groupstats_media_cache.pop(key, None)
        return None
    return row.get("file_id")


def set_groupstats_media_cache(chat_id, time_arg, file_id):
    if not file_id:
        return
    now_ts = time.time()
    key = _groupstats_cache_key(chat_id, time_arg)
    _groupstats_media_cache[key] = {
        "file_id": str(file_id),
        "expires_at": now_ts + GROUPSTATS_CACHE_TTL_SECONDS,
    }
    if len(_groupstats_media_cache) > 400:
        stale = [k for k, row in _groupstats_media_cache.items() if row.get("expires_at", 0) <= now_ts]
        for k in stale:
            _groupstats_media_cache.pop(k, None)
        while len(_groupstats_media_cache) > 400:
            _groupstats_media_cache.pop(next(iter(_groupstats_media_cache)), None)


def invalidate_groupstats_cache(chat_id):
    target = int(chat_id)
    keys = [k for k in _groupstats_cache.keys() if int(k[0]) == target]
    for k in keys:
        _groupstats_cache.pop(k, None)
    media_keys = [k for k in _groupstats_media_cache.keys() if int(k[0]) == target]
    for k in media_keys:
        _groupstats_media_cache.pop(k, None)
    invalidate_leaderboard_cache(chat_id)


def compute_group_stats_snapshot(chat_id, time_filter):
    match_query = {**accepted_call_filter(chat_id), **(time_filter or {})}
    pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "caller_id": 1,
                "caller_name": 1,
                "initial_mcap": 1,
                "ath_mcap": 1,
                "current_mcap": 1,
                "token_symbol": 1,
                "ca": 1,
                "ca_norm": 1,
            }
        },
        {
            "$unionWith": {
                "coll": "token_calls_archive",
                "pipeline": [
                    {"$match": match_query},
                    {
                        "$project": {
                            "caller_id": 1,
                            "caller_name": 1,
                            "initial_mcap": 1,
                            "ath_mcap": 1,
                            "current_mcap": 1,
                            "token_symbol": 1,
                            "ca": "$ca_norm",
                            "ca_norm": 1,
                        }
                    },
                ],
            }
        },
        {
            "$addFields": {
                "_initial": {"$toDouble": {"$ifNull": ["$initial_mcap", 0]}},
                "_ath": {"$toDouble": {"$ifNull": ["$ath_mcap", 0]}},
                "_current": {"$toDouble": {"$ifNull": ["$current_mcap", 0]}},
                "_caller_name": {"$ifNull": ["$caller_name", "Unknown"]},
            }
        },
        {"$match": {"_initial": {"$gt": 0}}},
        {"$addFields": {"_peak": {"$cond": [{"$gt": ["$_ath", "$_current"]}, "$_ath", "$_current"]}}},
        {
            "$addFields": {
                "_x_peak": {"$divide": ["$_peak", "$_initial"]},
                "_caller_key": {
                    "$cond": [
                        {"$ne": ["$caller_id", None]},
                        {"$concat": ["id:", {"$toString": "$caller_id"}]},
                        {"$concat": ["legacy:", {"$toLower": "$_caller_name"}]},
                    ]
                },
            }
        },
        {
            "$facet": {
                "metrics": [
                    {
                        "$group": {
                            "_id": None,
                            "total_calls": {"$sum": 1},
                            "unique_callers": {"$addToSet": "$_caller_key"},
                            "wins": {"$sum": {"$cond": [{"$gte": ["$_x_peak", WIN_MULTIPLIER]}, 1, 0]}},
                            "avg_x": {"$avg": "$_x_peak"},
                        }
                    }
                ],
                "best": [
                    {"$sort": {"_x_peak": -1}},
                    {"$limit": 1},
                    {
                        "$project": {
                            "_id": 0,
                            "caller_name": "$_caller_name",
                            "x_peak": "$_x_peak",
                            "token_symbol": 1,
                            "ca": 1,
                            "ca_norm": 1,
                        }
                    },
                ],
            }
        },
    ]

    rows = list(calls_collection.aggregate(pipeline, allowDiskUse=True))
    if not rows:
        return None
    row = rows[0] or {}
    metrics_rows = row.get("metrics") or []
    if not metrics_rows:
        return None

    m = metrics_rows[0] or {}
    total_calls = int(m.get("total_calls", 0) or 0)
    if total_calls <= 0:
        return None
    unique_callers = len(m.get("unique_callers") or [])
    wins = int(m.get("wins", 0) or 0)
    avg_x = float(m.get("avg_x", 1.0) or 1.0)
    win_rate = (wins / total_calls) if total_calls > 0 else 0.0

    best_row = (row.get("best") or [None])[0]
    if best_row:
        best_ca = best_row.get("ca") or best_row.get("ca_norm") or ""
        best = {
            "best_x": float(best_row.get("x_peak", 0) or 0),
            "caller_name": str(best_row.get("caller_name") or "Unknown"),
            "token_symbol": str(best_row.get("token_symbol") or ""),
            "ca": str(best_ca),
        }
    else:
        best = None

    return {
        "total_calls": total_calls,
        "unique_callers": unique_callers,
        "win_rate": float(win_rate),
        "avg_x": float(avg_x),
        "best": best,
    }


def _accepted_query(chat_id, extra=None):
    query = accepted_call_filter(chat_id)
    if extra:
        query = {**query, **extra}
    return query


def _mongo_caller_key_expr():
    return {
        "$cond": [
            {"$ne": ["$caller_id", None]},
            {"$concat": ["id:", {"$toString": "$caller_id"}]},
            {"$concat": ["legacy:", {"$toLower": {"$ifNull": ["$caller_name", "unknown"]}}]},
        ]
    }


def select_priority_ath_call_ids(chat_id, lookback_days=ATH_TRACK_WINDOW_DAYS, limit=ATH_PRIORITY_KEEP_MAX_CALLS):
    if limit <= 0:
        return set()

    cutoff = utc_now() - timedelta(days=max(1, int(lookback_days or ATH_TRACK_WINDOW_DAYS)))
    pipeline = [
        {"$match": _accepted_query(chat_id, {"timestamp": {"$gte": cutoff}})},
        {
            "$project": {
                "_id": 1,
                "initial_mcap": 1,
                "ath_mcap": 1,
                "current_mcap": 1,
                "timestamp": 1,
            }
        },
        {
            "$addFields": {
                "_initial": {"$toDouble": {"$ifNull": ["$initial_mcap", 0]}},
                "_ath": {"$toDouble": {"$ifNull": ["$ath_mcap", 0]}},
                "_current": {"$toDouble": {"$ifNull": ["$current_mcap", 0]}},
            }
        },
        {"$match": {"_initial": {"$gt": 0}}},
        {"$addFields": {"_peak": {"$cond": [{"$gt": ["$_ath", "$_current"]}, "$_ath", "$_current"]}}},
        {"$addFields": {"_x_peak": {"$divide": ["$_peak", "$_initial"]}}},
        {"$match": {"_x_peak": {"$gte": ATH_PRIORITY_MIN_X}}},
        {"$sort": {"_x_peak": -1, "timestamp": -1}},
        {"$limit": int(limit)},
        {"$project": {"_id": 1}},
    ]
    rows = list(calls_collection.aggregate(pipeline, allowDiskUse=True))
    return {row.get("_id") for row in rows if row.get("_id") is not None}


def reactivate_priority_calls(protected_ids):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    if not protected_ids:
        return 0
    result = calls_collection.update_many(
        {"_id": {"$in": list(protected_ids)}, "is_stashed": True},
        {
            "$set": {
                "is_stashed": False,
                "last_reactivated_at": utc_now(),
            },
            "$unset": {"stashed_reason": "", "stashed_at": ""},
        },
    )
    return int(result.modified_count or 0)


def stash_old_calls_per_caller(chat_id, keep_latest=HEARTBEAT_CALLS_PER_CALLER, protected_ids=None):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    active_calls = list(
        calls_collection.find(
            _accepted_query(chat_id, {"is_stashed": {"$ne": True}})
        ).sort("timestamp", -1)
    )
    if not active_calls:
        return 0

    now = utc_now()
    seen_per_caller = {}
    to_stash_ids = []
    for call in active_calls:
        if call.get("_id") in protected_ids:
            continue
        key = get_caller_key(call)
        seen = seen_per_caller.get(key, 0)
        if seen < keep_latest:
            seen_per_caller[key] = seen + 1
            continue
        to_stash_ids.append(call["_id"])

    if not to_stash_ids:
        return 0

    result = calls_collection.update_many(
        {"_id": {"$in": to_stash_ids}},
        {
            "$set": {
                "is_stashed": True,
                "stashed_reason": "older_call",
                "stashed_at": now,
            }
        },
    )
    return int(result.modified_count or 0)


def _to_archive_doc(call_doc):
    return {
        "chat_id": call_doc.get("chat_id"),
        "status": "accepted",
        "ca_norm": call_doc.get("ca_norm", normalize_ca(call_doc.get("ca", ""))),
        "token_symbol": call_doc.get("token_symbol", ""),
        "caller_id": call_doc.get("caller_id"),
        "caller_name": call_doc.get("caller_name", "Unknown"),
        "timestamp": call_doc.get("timestamp", utc_now()),
        "initial_mcap": float(call_doc.get("initial_mcap", 0) or 0),
        "ath_mcap": float(call_doc.get("ath_mcap", 0) or 0),
        "current_mcap": float(call_doc.get("current_mcap", 0) or 0),
        "stashed_reason": call_doc.get("stashed_reason", "older_call"),
        "ath_seen_at": call_doc.get("ath_seen_at"),
        "ath_source": call_doc.get("ath_source"),
        "repost_count": int(call_doc.get("repost_count", 0) or 0),
        "last_reposted_at": call_doc.get("last_reposted_at"),
        "last_ath_change_at": call_doc.get("last_ath_change_at"),
        "last_hist_ath_checked_at": call_doc.get("last_hist_ath_checked_at"),
        "last_hist_ath_provider": call_doc.get("last_hist_ath_provider"),
        "archived_at": utc_now(),
    }


def archive_stashed_calls(chat_id, reason="older_call", limit=1000, older_than=None, protected_ids=None):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    query = _accepted_query(chat_id, {"is_stashed": True, "stashed_reason": reason})
    if older_than is not None:
        query = {**query, "timestamp": {"$lt": older_than}}
    if protected_ids:
        query = {**query, "_id": {"$nin": list(protected_ids)}}
    candidates = list(
        calls_collection.find(
            query,
            {"message_id": 0, "message_date": 0, "caller_username": 0, "ca": 0},
        )
        .sort("timestamp", 1)
        .limit(limit)
    )
    if not candidates:
        return 0

    archive_docs = [_to_archive_doc(doc) for doc in candidates]
    if archive_docs:
        calls_archive_collection.insert_many(archive_docs, ordered=False)

    ids = [doc["_id"] for doc in candidates if doc.get("_id") is not None]
    if not ids:
        return 0
    result = calls_collection.delete_many({"_id": {"$in": ids}})
    return int(result.deleted_count or 0)


def archive_inactive_callers(chat_id, inactive_hours=INACTIVE_CALLER_ARCHIVE_HOURS, limit=5000, protected_ids=None):
    if inactive_hours <= 0:
        return 0

    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    cutoff = utc_now() - timedelta(hours=int(inactive_hours))
    caller_key_expr = _mongo_caller_key_expr()
    inactive_pipeline = [
        {"$match": _accepted_query(chat_id)},
        {"$addFields": {"_caller_key": caller_key_expr}},
        {"$sort": {"timestamp": -1}},
        {"$group": {"_id": "$_caller_key", "latest_ts": {"$first": "$timestamp"}}},
        {"$match": {"latest_ts": {"$lt": cutoff}}},
        {"$limit": 1000},
    ]
    inactive_rows = list(calls_collection.aggregate(inactive_pipeline, allowDiskUse=True))
    inactive_keys = [row.get("_id") for row in inactive_rows if row.get("_id")]
    if not inactive_keys:
        return 0

    candidates = list(
        calls_collection.find(
            {
                **_accepted_query(chat_id),
                "$expr": {"$in": [caller_key_expr, inactive_keys]},
                **({"_id": {"$nin": list(protected_ids)}} if protected_ids else {}),
            },
            {"message_id": 0, "message_date": 0, "caller_username": 0, "ca": 0},
        )
        .sort("timestamp", 1)
        .limit(int(limit))
    )
    if not candidates:
        return 0

    archive_docs = []
    ids = []
    for doc in candidates:
        if not doc.get("stashed_reason"):
            doc["stashed_reason"] = "inactive_caller"
        archive_docs.append(_to_archive_doc(doc))
        if doc.get("_id") is not None:
            ids.append(doc["_id"])

    if archive_docs:
        calls_archive_collection.insert_many(archive_docs, ordered=False)
    if not ids:
        return 0
    result = calls_collection.delete_many({"_id": {"$in": ids}})
    return int(result.deleted_count or 0)


def load_calls_for_stats(chat_id, extra=None, include_archive=True):
    query = _accepted_query(chat_id, extra or {})
    live_calls = list(calls_collection.find(query))
    archived_calls = list(calls_archive_collection.find(query)) if include_archive else []
    return live_calls, archived_calls, (live_calls + archived_calls)


def refresh_archived_calls_market_data(calls):
    if not calls:
        return 0

    unique_cas = list(
        {
            call.get("ca_norm", normalize_ca(call.get("ca", "")))
            for call in calls
            if call.get("ca_norm") or call.get("ca")
        }
    )
    if not unique_cas:
        return 0

    latest_meta = get_dexscreener_batch_meta(unique_cas)
    updated = 0
    now = utc_now()

    for call in calls:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        meta = latest_meta.get(ca_norm, {})
        current_mcap = meta.get("fdv", call.get("current_mcap", call.get("initial_mcap", 0)))
        if not current_mcap:
            continue

        initial_val = float(call.get("initial_mcap", 0) or 0)
        old_ath_val = float(call.get("ath_mcap", current_mcap) or current_mcap)
        old_current_val = float(call.get("current_mcap", current_mcap) or current_mcap)
        old_x_peak = (max(old_ath_val, old_current_val) / initial_val) if initial_val > 0 else 0.0
        ath = max(float(call.get("ath_mcap", current_mcap) or current_mcap), float(current_mcap))
        new_x_peak = (float(ath) / initial_val) if initial_val > 0 else 0.0

        update_fields = {
            "current_mcap": current_mcap,
            "ath_mcap": ath,
            "last_market_refresh_at": now,
        }
        if ath > old_ath_val + 1e-12:
            update_fields["ath_seen_at"] = now
            update_fields["last_ath_change_at"] = now
            update_fields["ath_source"] = "dex_live"
        if meta.get("symbol"):
            update_fields["token_symbol"] = meta["symbol"]

        result = calls_archive_collection.update_one({"_id": call["_id"]}, {"$set": update_fields})
        updated += int(result.modified_count or 0)
        upsert_rollup_for_call_peak_delta(call, old_x_peak, new_x_peak)
        call["current_mcap"] = current_mcap
        call["ath_mcap"] = ath
        if ath > old_ath_val + 1e-12:
            call["ath_seen_at"] = now
            call["last_ath_change_at"] = now
            call["ath_source"] = "dex_live"
        if meta.get("symbol"):
            call["token_symbol"] = meta["symbol"]

    return updated


def refresh_recent_call_peaks(chat_id, lookback_days=ATH_TRACK_WINDOW_DAYS, limit=ATH_TRACK_MAX_CALLS_PER_CHAT):
    seeded_metadata = seed_refresh_queue_metadata(chat_id)
    protected_ids = select_runner_protected_ids(
        chat_id,
        lookback_days=max(lookback_days, REFRESH_QUEUE_LOOKBACK_DAYS),
        limit=RUNNER_PROTECT_MAX_CALLS,
    )
    protected_ids.update(
        select_priority_ath_call_ids(chat_id, lookback_days=lookback_days, limit=ATH_PRIORITY_KEEP_MAX_CALLS)
    )
    reactivated_count = reactivate_priority_calls(protected_ids)
    archived_inactive = archive_inactive_callers(
        chat_id,
        inactive_hours=INACTIVE_CALLER_ARCHIVE_HOURS,
        limit=5000,
        protected_ids=protected_ids,
    )
    stashed_count = stash_low_priority_calls(
        chat_id,
        active_limit=ACTIVE_LIVE_CALLS_PER_CHAT,
        protected_ids=protected_ids,
    )
    priority_stash_cutoff = utc_now() - timedelta(hours=PRIORITY_STASH_ARCHIVE_MIN_AGE_HOURS)
    archived_old = archive_stashed_calls(
        chat_id,
        reason="priority_queue",
        limit=1000,
        older_than=priority_stash_cutoff,
        protected_ids=protected_ids,
    )
    migrated_old = archive_stashed_calls(chat_id, reason="older_call", limit=1000, protected_ids=protected_ids)
    low_volume_cutoff = utc_now() - timedelta(hours=LOW_VOLUME_ARCHIVE_MIN_AGE_HOURS)
    archived_low_pre = archive_stashed_calls(
        chat_id,
        reason="low_volume",
        limit=1000,
        older_than=low_volume_cutoff,
        protected_ids=protected_ids,
    )
    refresh_limit = max(1, min(int(limit or REFRESH_QUEUE_MAX_CALLS_PER_CHAT), REFRESH_QUEUE_MAX_CALLS_PER_CHAT))
    calls = load_due_refresh_calls(
        chat_id,
        protected_ids=protected_ids,
        limit=refresh_limit,
        lookback_days=max(lookback_days, REFRESH_QUEUE_LOOKBACK_DAYS),
    )
    if not calls:
        if (
            seeded_metadata > 0
            or reactivated_count > 0
            or archived_inactive > 0
            or stashed_count > 0
            or archived_old > 0
            or migrated_old > 0
            or archived_low_pre > 0
        ):
            invalidate_groupstats_cache(chat_id)
        return 0
    refreshed_count = refresh_calls_market_data(
        calls,
        include_stashed=False,
        apply_stash_policy=True,
        protected_ids=protected_ids,
    )
    live_hist_stats = {"checked": 0, "updated": 0}
    archive_hist_stats = {"checked": 0, "updated": 0}
    if HISTORICAL_ATH_ENABLED:
        live_entries = build_historical_reconcile_entries(
            calls,
            collection_name="live",
            protected_ids=protected_ids,
        )
        live_hist_stats = reconcile_calls_with_historical_ath(
            live_entries,
            limit=HISTORICAL_ATH_HEARTBEAT_MAX_CALLS,
            force=False,
        )
        if HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS > 0:
            archived_cutoff = utc_now() - timedelta(days=HISTORICAL_ATH_ARCHIVE_LOOKBACK_DAYS)
            archived_candidates = list(
                calls_archive_collection.find(
                    _accepted_query(chat_id, {"timestamp": {"$gte": archived_cutoff}})
                )
                .sort("timestamp", -1)
                .limit(max(HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS * 4, HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS))
            )
            archive_entries = build_historical_reconcile_entries(
                archived_candidates,
                collection_name="archive",
            )
            archive_hist_stats = reconcile_calls_with_historical_ath(
                archive_entries,
                limit=HISTORICAL_ATH_ARCHIVE_HEARTBEAT_MAX_CALLS,
                force=False,
            )
    archived_low_post = archive_stashed_calls(
        chat_id,
        reason="low_volume",
        limit=1000,
        older_than=low_volume_cutoff,
        protected_ids=protected_ids,
    )
    if (
        seeded_metadata > 0
        or reactivated_count > 0
        or archived_inactive > 0
        or stashed_count > 0
        or archived_old > 0
        or migrated_old > 0
        or archived_low_pre > 0
        or archived_low_post > 0
        or refreshed_count > 0
        or live_hist_stats["updated"] > 0
        or archive_hist_stats["updated"] > 0
    ):
        invalidate_groupstats_cache(chat_id)
    return len(calls)


def refresh_all_call_peaks(chat_id):
    live_calls = list(calls_collection.find(_accepted_query(chat_id)))
    archived_cutoff = utc_now() - timedelta(days=max(ATH_TRACK_WINDOW_DAYS, 30))
    archived_calls = list(
        calls_archive_collection.find(_accepted_query(chat_id, {"timestamp": {"$gte": archived_cutoff}}))
        .sort("timestamp", -1)
        .limit(ATH_TRACK_MAX_CALLS_PER_CHAT)
    )
    if not live_calls and not archived_calls:
        return {"calls": 0, "tokens": 0, "updated": 0}

    tokens = len(
        {
            call.get("ca_norm", normalize_ca(call.get("ca", "")))
            for call in (live_calls + archived_calls)
            if call.get("ca_norm") or call.get("ca")
        }
    )
    updated_live = refresh_calls_market_data(live_calls, include_stashed=True, apply_stash_policy=True) if live_calls else 0
    updated_archived = refresh_archived_calls_market_data(archived_calls) if archived_calls else 0
    historical_stats = {"checked": 0, "updated": 0}
    if HISTORICAL_ATH_ENABLED and HISTORICAL_ATH_MANUAL_MAX_CALLS > 0:
        historical_entries = build_historical_reconcile_entries(
            live_calls,
            collection_name="live",
            protected_ids={call.get("_id") for call in live_calls if call_peak_x(call) >= ATH_PRIORITY_MIN_X},
        )
        historical_entries += build_historical_reconcile_entries(
            archived_calls,
            collection_name="archive",
        )
        historical_stats = reconcile_calls_with_historical_ath(
            historical_entries,
            limit=HISTORICAL_ATH_MANUAL_MAX_CALLS,
            force=True,
        )
    invalidate_groupstats_cache(chat_id)
    return {
        "calls": len(live_calls) + len(archived_calls),
        "tokens": tokens,
        "updated": updated_live + updated_archived + historical_stats["updated"],
        "live_calls": len(live_calls),
        "archived_calls": len(archived_calls),
        "historical_checked": historical_stats["checked"],
        "historical_updated": historical_stats["updated"],
    }


def bump_live_ath_for_chat(chat_id, token_meta_map, reactivate=False):
    if not token_meta_map:
        return {"matched": 0, "modified": 0}

    total_matched = 0
    total_modified = 0
    now = utc_now()
    for ca_norm, token_meta in token_meta_map.items():
        mcap = float(token_meta.get("fdv", 0) or 0)
        if mcap <= 0:
            continue
        candidates = list(
            calls_collection.find(
                _accepted_query(chat_id, {"ca_norm": ca_norm}),
                {
                    "_id": 1,
                    "chat_id": 1,
                    "caller_id": 1,
                    "caller_name": 1,
                    "initial_mcap": 1,
                    "ath_mcap": 1,
                    "current_mcap": 1,
                },
            )
        )
        volume_h1 = float(token_meta.get("volume_h1", token_meta.get("volume_h24", 0)) or 0)
        volume_h24 = float(token_meta.get("volume_h24", 0) or 0)

        update_doc = {
            "$set": {
                "current_mcap": mcap,
                "volume_h1": volume_h1,
                "volume_h24": volume_h24,
                "last_market_refresh_at": now,
            },
            "$max": {"ath_mcap": mcap},
        }
        symbol = token_meta.get("symbol")
        if symbol:
            update_doc["$set"]["token_symbol"] = symbol
        if reactivate:
            update_doc["$set"]["is_stashed"] = False
            update_doc["$set"]["last_reactivated_at"] = now
            update_doc["$unset"] = {"stashed_reason": "", "stashed_at": ""}

        result = calls_collection.update_many(
            _accepted_query(chat_id, {"ca_norm": ca_norm}),
            update_doc,
        )
        total_matched += int(result.matched_count or 0)
        total_modified += int(result.modified_count or 0)
        for doc in candidates:
            initial = float(doc.get("initial_mcap", 0) or 0)
            if initial <= 0:
                continue
            old_ath = float(doc.get("ath_mcap", initial) or initial)
            old_current = float(doc.get("current_mcap", initial) or initial)
            old_x = max(old_ath, old_current) / initial
            new_x = max(old_ath, float(mcap)) / initial
            doc["current_mcap"] = mcap
            doc["ath_mcap"] = max(old_ath, float(mcap))
            doc["volume_h1"] = volume_h1
            doc["volume_h24"] = volume_h24
            set_fields = refresh_state_update_fields(doc, now=now)
            if new_x > old_x + 1e-12:
                set_fields["ath_seen_at"] = now
                set_fields["last_ath_change_at"] = now
                set_fields["ath_source"] = "dex_live"
            calls_collection.update_one({"_id": doc["_id"]}, {"$set": set_fields})
            upsert_rollup_for_call_peak_delta(doc, old_x, new_x)

    return {"matched": total_matched, "modified": total_modified}


def bump_archived_ath_for_chat(chat_id, token_meta_map):
    if not token_meta_map:
        return {"matched": 0, "modified": 0}

    total_matched = 0
    total_modified = 0
    now = utc_now()
    for ca_norm, token_meta in token_meta_map.items():
        mcap = float(token_meta.get("fdv", 0) or 0)
        if mcap <= 0:
            continue

        candidates = list(
            calls_archive_collection.find(
                _accepted_query(chat_id, {"ca_norm": ca_norm}),
                {
                    "_id": 1,
                    "chat_id": 1,
                    "caller_id": 1,
                    "caller_name": 1,
                    "initial_mcap": 1,
                    "ath_mcap": 1,
                    "current_mcap": 1,
                },
            )
        )
        if not candidates:
            continue

        update_doc = {
            "$set": {
                "current_mcap": mcap,
                "last_market_refresh_at": now,
            },
            "$max": {"ath_mcap": mcap},
        }
        symbol = token_meta.get("symbol")
        if symbol:
            update_doc["$set"]["token_symbol"] = symbol

        result = calls_archive_collection.update_many(
            _accepted_query(chat_id, {"ca_norm": ca_norm}),
            update_doc,
        )
        total_matched += int(result.matched_count or 0)
        total_modified += int(result.modified_count or 0)

        for doc in candidates:
            initial = float(doc.get("initial_mcap", 0) or 0)
            if initial <= 0:
                continue
            old_ath = float(doc.get("ath_mcap", initial) or initial)
            old_current = float(doc.get("current_mcap", initial) or initial)
            old_x = max(old_ath, old_current) / initial
            new_x = max(old_ath, float(mcap)) / initial
            if new_x > old_x + 1e-12:
                calls_archive_collection.update_one(
                    {"_id": doc["_id"]},
                    {"$set": {"ath_seen_at": now, "last_ath_change_at": now, "ath_source": "dex_live"}},
                )
            upsert_rollup_for_call_peak_delta(doc, old_x, new_x)

    return {"matched": total_matched, "modified": total_modified}


def mark_reposted_calls(chat_id, ca_norm):
    ca_norm = normalize_ca(ca_norm or "")
    if not ca_norm:
        return {"live": 0, "archive": 0}

    now = utc_now()
    live_calls = list(calls_collection.find(_accepted_query(chat_id, {"ca_norm": ca_norm})).limit(20))
    archived_calls = list(calls_archive_collection.find(_accepted_query(chat_id, {"ca_norm": ca_norm})).limit(20))
    live_updated = 0
    archive_updated = 0

    for call in live_calls:
        call["last_reposted_at"] = now
        call["repost_count"] = int(call.get("repost_count", 0) or 0) + 1
        set_fields = {
            "last_reposted_at": now,
            "repost_count": call["repost_count"],
            "is_stashed": False,
            "last_reactivated_at": now,
        }
        set_fields.update(refresh_state_update_fields(call, now=now))
        result = calls_collection.update_one(
            {"_id": call["_id"]},
            {"$set": set_fields, "$unset": {"stashed_reason": "", "stashed_at": ""}},
        )
        live_updated += int(result.modified_count or 0)

    for call in archived_calls:
        result = calls_archive_collection.update_one(
            {"_id": call["_id"]},
            {
                "$set": {"last_reposted_at": now},
                "$inc": {"repost_count": 1},
            },
        )
        archive_updated += int(result.modified_count or 0)

    return {"live": live_updated, "archive": archive_updated}


def reconcile_existing_call_history_for_ca(chat_id, ca_norm, force=True):
    if not HISTORICAL_ATH_ENABLED:
        return {"checked": 0, "updated": 0}

    ca_norm = normalize_ca(ca_norm or "")
    if not ca_norm:
        return {"checked": 0, "updated": 0}

    live_calls = list(calls_collection.find(_accepted_query(chat_id, {"ca_norm": ca_norm})).sort("timestamp", 1).limit(20))
    archived_calls = list(calls_archive_collection.find(_accepted_query(chat_id, {"ca_norm": ca_norm})).sort("timestamp", 1).limit(20))
    entries = build_historical_reconcile_entries(live_calls, collection_name="live")
    entries += build_historical_reconcile_entries(archived_calls, collection_name="archive")
    return reconcile_calls_with_historical_ath(entries, limit=40, force=force)


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
                        f"🔥 HOT HAND ALERT\n"
                        f"────────────────\n"
                        f"👤 Caller: {caller_name}\n"
                        f"🏅 Win Streak: {hot_streak}\n"
                        f"⏱ Last call inside {ACTIVE_CALL_WINDOW_HOURS}h"
                    ),
                    reply_markup=delete_button_markup(0),
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
                        f"⚠️ DANGER STREAK\n"
                        f"────────────────\n"
                        f"👤 Caller: {caller_name}\n"
                        f"🩸 Losing Streak: {cold_streak}\n"
                        f"🔎 Review before trusting new calls"
                    ),
                    reply_markup=delete_button_markup(0),
                )
                user_profiles_collection.update_one(
                    {"chat_id": chat_id, "user_id": user_id},
                    {"$set": {"alerts.cold_notified_at": now, "alerts.cold_len": cold_streak}},
                    upsert=True,
                )
                triggered += 1

    return triggered


def compute_daily_digest_data(chat_id, since_ts):
    live_calls, archived_calls, calls = load_calls_for_stats(chat_id, {"timestamp": {"$gte": since_ts}}, include_archive=True)
    if not calls:
        return {
            "has_calls": False,
            "calls": [],
            "user_calls": {},
            "top": [],
            "worst": [],
            "best_call": None,
            "worst_rug": None,
            "top_mentions": [],
            "total_calls": 0,
            "total_callers": 0,
        }

    refresh_calls_market_data(live_calls)
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
                "avg_now_x": 1.0 + metrics["avg_ath"],
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

    return {
        "has_calls": True,
        "calls": calls,
        "user_calls": user_calls,
        "top": top,
        "worst": worst,
        "best_call": best_call,
        "worst_rug": worst_rug,
        "top_mentions": top_mentions,
        "total_calls": len(calls),
        "total_callers": len(user_calls),
    }


def build_daily_digest(chat_id, since_ts, digest_data=None):
    data = digest_data or compute_daily_digest_data(chat_id, since_ts)
    if not data["has_calls"]:
        return "📰 DAILY INTEL DIGEST • 24H\n────────────────\nNo accepted calls."

    top = data["top"]
    worst = data["worst"]
    best_call = data["best_call"]
    worst_rug = data["worst_rug"]
    top_mentions = data["top_mentions"]

    lines = [
        "📰 DAILY INTEL DIGEST • 24H",
        "────────────────",
        f"📞 Calls: {data['total_calls']} | 👥 Callers: {data['total_callers']}",
        "",
        "🏆 TOP CALLERS",
        "────────────────",
    ]
    if top:
        for idx, row in enumerate(top, start=1):
            lines.append(
                f"{rank_badge(idx)} {row['name']} {stars_from_pct(row['win_rate'])}\n"
                f"↳ Avg {format_return(row['avg_now_x'])} | Win {row['win_rate']:.1f}% | Calls {row['calls']}"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("🧯 WORST CALLERS")
    lines.append("────────────────")
    if worst:
        for idx, row in enumerate(worst, start=1):
            lines.append(
                f"{idx}. {row['name']}\n"
                f"↳ Avg {format_return(row['avg_now_x'])} | Win {row['win_rate']:.1f}% | Calls {row['calls']}"
            )
    else:
        lines.append("- None")

    lines.append("")
    lines.append("⚡ HIGHLIGHTS")
    lines.append("────────────────")
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
    lines.append("📣 MOST MENTIONED CAs")
    lines.append("────────────────")
    if top_mentions:
        for idx, row in enumerate(top_mentions, start=1):
            lines.append(f"{idx}. {token_label(row['symbol'], row['ca'])} • {row['count']} mentions")
    else:
        lines.append("- None")

    return "\n".join(lines)


def generate_daily_digest_card(digest_data, group_avatar_image=None):
    width, height = 1200, 440
    card = Image.new("RGB", (width, height), (14, 22, 38))
    draw_vertical_gradient(card, (21, 27, 54), (34, 48, 86))
    overlay = Image.new("RGBA", (width, height), (0, 0, 0, 0))
    od = ImageDraw.Draw(overlay)
    od.rounded_rectangle((60, 28, 1140, 412), radius=28, fill=(13, 20, 43, 185), outline=(98, 121, 186, 145), width=2)
    od.ellipse((700, -90, 1170, 320), fill=(96, 165, 250, 45))
    card = Image.alpha_composite(card.convert("RGBA"), overlay).convert("RGB")
    draw = ImageDraw.Draw(card)

    title_font = load_font(46, bold=True)
    block_font = load_font(30, bold=True)
    stat_font = load_font(42, bold=True)
    sub_font = load_font(26, bold=False)

    left_x = 88
    right_x = 680
    right_w = 380

    draw.text((left_x, 54), "DAILY INTEL SNAPSHOT", font=title_font, fill=(241, 247, 255))
    draw.text((left_x + 2, 112), "Window: Last 24h", font=sub_font, fill=(170, 198, 235))
    draw.text((left_x, 175), fit_text(draw, f"Calls {digest_data['total_calls']} • Callers {digest_data['total_callers']}", block_font, 540), font=block_font, fill=(255, 255, 255))

    top = digest_data["top"][0] if digest_data["top"] else None
    if top:
        draw.text((left_x, 228), fit_text(draw, f"Top: {ascii_safe(top['name'])}", stat_font, 540), font=stat_font, fill=(141, 255, 113))
        draw.text((left_x, 276), fit_text(draw, f"Avg {format_return(top['avg_now_x'])} • Win {top['win_rate']:.1f}%", block_font, 540), font=block_font, fill=(217, 236, 255))
    else:
        draw.text((left_x, 228), "Top: N/A", font=stat_font, fill=(141, 255, 113))

    best_call = digest_data["best_call"]
    draw.text((right_x, 180), "Best Call", font=block_font, fill=(204, 231, 255))
    if best_call:
        initial = float(best_call.get("initial_mcap", 1) or 1)
        best_x = float(best_call.get("ath_mcap", initial) or initial) / initial
        draw.text((right_x, 228), fit_text(draw, format_return(best_x), stat_font, right_w), font=stat_font, fill=(255, 255, 255))
        by_line = f"By {ascii_safe(best_call.get('caller_name', 'Unknown'))}"
        draw.text((right_x, 286), fit_text(draw, by_line, block_font, right_w), font=block_font, fill=(217, 236, 255))
    else:
        draw.text((right_x, 228), "N/A", font=stat_font, fill=(255, 255, 255))

    group_avatar = build_circle_avatar(group_avatar_image, 84) if group_avatar_image is not None else None
    if group_avatar is not None:
        icon_x, icon_y = 1042, 46
        card_rgba = card.convert("RGBA")
        card_rgba.alpha_composite(group_avatar, (icon_x, icon_y))
        ring = ImageDraw.Draw(card_rgba)
        ring.ellipse((icon_x - 3, icon_y - 3, icon_x + 86, icon_y + 86), outline=(145, 174, 235, 210), width=3)
        card = card_rgba.convert("RGB")

    buffer = BytesIO()
    card.save(buffer, format="PNG", optimize=True)
    buffer.seek(0)
    return buffer


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

    digest_data = compute_daily_digest_data(chat_id, now - timedelta(hours=24))
    digest_text = build_daily_digest(chat_id, now - timedelta(hours=24), digest_data=digest_data)

    if digest_data["has_calls"]:
        group_avatar_image = await fetch_chat_avatar_image_cached(bot, chat_id)
        digest_card = generate_daily_digest_card(digest_data, group_avatar_image=group_avatar_image)
        digest_caption = digest_text if len(digest_text) <= 1024 else (digest_text[:1021] + "...")
        await bot.send_photo(
            chat_id=chat_id,
            photo=digest_card,
            caption=digest_caption,
            reply_markup=delete_button_markup(0),
        )
    else:
        await bot.send_message(chat_id=chat_id, text=digest_text, reply_markup=delete_button_markup(0))
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
                    ensure_group_key(chat_id)
                    refresh_started = time.perf_counter()
                    refreshed_calls = refresh_recent_call_peaks(chat_id)
                    maybe_run_daily_rollup_repair(chat_id)
                    refresh_elapsed_ms = (time.perf_counter() - refresh_started) * 1000.0
                    record_refresh_runtime(chat_id, refresh_elapsed_ms, refreshed_calls)
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


async def link_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if chat.type != "private":
        await msg.reply_text("Use this command in private chat with the bot.")
        return
    if not context.args:
        await msg.reply_text("Usage: /linkgroup <group_key>")
        return

    raw_key = context.args[0].strip()
    target_chat_id = None
    group_key = None

    # Primary path: group key is the literal Telegram group chat id.
    try:
        target_chat_id = int(raw_key)
        group_key = str(target_chat_id)
    except ValueError:
        # Backward compatibility: allow legacy/random keys that may still exist in settings.
        legacy = settings_collection.find_one({"group_key": raw_key}) or {}
        target_chat_id = legacy.get("chat_id")
        if target_chat_id is not None:
            group_key = str(int(target_chat_id))

    if target_chat_id is None:
        await msg.reply_text("Invalid group key. Use the group chat id from /adminstats.")
        return
    if not await user_is_admin(context.bot, target_chat_id, user.id, user):
        await msg.reply_text("You must be an admin of that group to link it.")
        return

    # Normalize/migrate settings to canonical key.
    group_key = ensure_group_key(target_chat_id)

    private_links_collection.update_one(
        {"user_id": user.id},
        {
            "$set": {
                "user_id": user.id,
                "chat_id": target_chat_id,
                "group_key": group_key,
                "linked_at": utc_now(),
            }
        },
        upsert=True,
    )

    group_title = str(target_chat_id)
    try:
        group_obj = await context.bot.get_chat(target_chat_id)
        if getattr(group_obj, "title", None):
            group_title = group_obj.title
    except Exception:
        pass

    await msg.reply_text(f"Linked to group: {group_title}\nKey: {group_key}")


async def unlink_group(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat = update.effective_chat
    user = update.effective_user
    msg = update.effective_message

    if chat.type != "private":
        await msg.reply_text("Use this command in private chat with the bot.")
        return
    private_links_collection.delete_one({"user_id": user.id})
    await msg.reply_text("Private link removed.")


async def toggle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return
    setting = settings_collection.find_one({"chat_id": chat_id}) or {}

    if not setting.get("alerts", False):
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.effective_message.reply_text(
            "🔔 ALERTS: ON\n────────────────\nHeartbeat streak alerts and daily digest are enabled."
        )
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.effective_message.reply_text(
            "🔕 ALERTS: OFF\n────────────────\nHeartbeat streak alerts and daily digest are disabled."
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
    found_cas_list = sorted(found_cas)
    batch_data = get_dexscreener_batch_meta(found_cas_list)
    bump_live_ath_for_chat(chat_id, batch_data, reactivate=True)
    bump_archived_ath_for_chat(chat_id, batch_data)

    is_edited = update.edited_message is not None

    msg_time = message_obj.date
    now = utc_now()
    if msg_time.tzinfo is None:
        msg_time = msg_time.replace(tzinfo=timezone.utc)
    delay_seconds = max(0, int((now - msg_time).total_seconds()))

    for ca_norm in found_cas_list:
        rejection_reason = None

        if is_edited:
            rejection_reason = "edited_message"
        elif delay_seconds > MAX_CALL_DELAY_SECONDS:
            rejection_reason = "late_submission"
        elif call_is_duplicate(chat_id, ca_norm):
            rejection_reason = "duplicate_ca"

        if rejection_reason:
            if rejection_reason == "duplicate_ca":
                mark_reposted_calls(chat_id, ca_norm)
                reconcile_existing_call_history_for_ca(chat_id, ca_norm, force=True)
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

        token_meta = batch_data.get(ca_norm, {})
        mcap = token_meta.get("fdv")
        symbol = token_meta.get("symbol", "")
        volume_h1 = float(token_meta.get("volume_h1", token_meta.get("volume_h24", 0)) or 0)
        volume_h24 = float(token_meta.get("volume_h24", 0) or 0)

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
                "ath_seen_at": now,
                "ath_source": "dex_live",
                "last_ath_change_at": now,
                "current_mcap": mcap,
                "token_symbol": symbol,
                "volume_h1": volume_h1,
                "volume_h24": volume_h24,
                "is_stashed": False,
                "repost_count": 0,
                "timestamp": now,
                "message_id": message_obj.message_id,
                "message_date": msg_time,
                "ingest_delay_seconds": delay_seconds,
            }
            is_stashed = should_stash_low_volume_call(call_data, volume_h1, now=now)
            call_data["is_stashed"] = is_stashed
            call_data.update(refresh_state_update_fields(call_data, now=now))
            if is_stashed:
                call_data["stashed_reason"] = "low_volume"
                call_data["stashed_at"] = now
            calls_collection.insert_one(call_data)
            update_user_profile(chat_id, user, "accepted")
            upsert_rollup_for_call_insert(call_data)

    invalidate_groupstats_cache(chat_id)


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


def refresh_calls_market_data(calls, include_stashed=False, apply_stash_policy=False, protected_ids=None):
    protected_ids = {obj_id for obj_id in (protected_ids or set()) if obj_id is not None}
    refresh_targets = []
    for call in calls:
        if not call.get("ca"):
            continue
        if not include_stashed and bool(call.get("is_stashed", False)):
            continue
        refresh_targets.append(call)

    unique_cas = list({call.get("ca_norm", normalize_ca(call["ca"])) for call in refresh_targets})
    if not unique_cas:
        return 0
    latest_meta = get_dexscreener_batch_meta(unique_cas)
    updated = 0
    now = utc_now()

    for call in refresh_targets:
        ca_norm = call.get("ca_norm", normalize_ca(call.get("ca", "")))
        meta = latest_meta.get(ca_norm, {})
        current_mcap = meta.get("fdv", call.get("current_mcap", call.get("initial_mcap", 0)))
        if not current_mcap:
            continue
        initial_val = float(call.get("initial_mcap", 0) or 0)
        old_ath_val = float(call.get("ath_mcap", current_mcap) or current_mcap)
        old_current_val = float(call.get("current_mcap", current_mcap) or current_mcap)
        old_x_peak = (max(old_ath_val, old_current_val) / initial_val) if initial_val > 0 else 0.0
        ath = max(float(call.get("ath_mcap", current_mcap)), float(current_mcap))
        new_x_peak = (float(ath) / initial_val) if initial_val > 0 else 0.0
        volume_h1 = float(
            meta.get(
                "volume_h1",
                call.get("volume_h1", call.get("volume_h24", 0)),
            ) or 0
        )
        volume_h24 = float(meta.get("volume_h24", call.get("volume_h24", 0)) or 0)
        update_fields = {
            "current_mcap": current_mcap,
            "ath_mcap": ath,
            "volume_h1": volume_h1,
            "volume_h24": volume_h24,
            "last_market_refresh_at": now,
        }
        unset_fields = {}
        if ath > old_ath_val + 1e-12:
            update_fields["ath_seen_at"] = now
            update_fields["last_ath_change_at"] = now
            update_fields["ath_source"] = "dex_live"
        if meta.get("symbol"):
            update_fields["token_symbol"] = meta["symbol"]

        call["current_mcap"] = current_mcap
        call["ath_mcap"] = ath
        call["volume_h1"] = volume_h1
        call["volume_h24"] = volume_h24
        if ath > old_ath_val + 1e-12:
            call["ath_seen_at"] = now
            call["last_ath_change_at"] = now
            call["ath_source"] = "dex_live"
        refresh_state = compute_call_refresh_state(call, now=now)
        if apply_stash_policy:
            should_stash = should_stash_low_volume_call(
                call,
                volume_h1,
                now=now,
                protected_ids=protected_ids,
                state=refresh_state,
            )
            if should_stash:
                update_fields["is_stashed"] = True
                update_fields["stashed_reason"] = "low_volume"
                update_fields["stashed_at"] = now
            else:
                update_fields["is_stashed"] = False
                unset_fields["stashed_reason"] = ""
                unset_fields["stashed_at"] = ""
        update_doc = {
            "$set": {
                **update_fields,
                "refresh_priority": int(refresh_state["priority"]),
                "refresh_interval_seconds": int(refresh_state["interval_seconds"]),
                "next_refresh_at": refresh_state["next_refresh_at"],
            }
        }
        if unset_fields:
            update_doc["$unset"] = unset_fields
        result = calls_collection.update_one({"_id": call["_id"]}, update_doc)
        updated += int(result.modified_count or 0)
        upsert_rollup_for_call_peak_delta(call, old_x_peak, new_x_peak)
        if meta.get("symbol"):
            call["token_symbol"] = meta["symbol"]
        if apply_stash_policy:
            call["is_stashed"] = bool(update_fields.get("is_stashed", False))
    return updated


def fetch_best_win_text(chat_id, time_filter):
    match_query = {**accepted_call_filter(chat_id), **(time_filter or {})}
    pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "initial_mcap": 1,
                "ath_mcap": 1,
                "current_mcap": 1,
                "token_symbol": 1,
                "ca": 1,
                "ca_norm": 1,
                "caller_name": 1,
            }
        },
        {
            "$unionWith": {
                "coll": "token_calls_archive",
                "pipeline": [
                    {"$match": match_query},
                    {
                        "$project": {
                            "initial_mcap": 1,
                            "ath_mcap": 1,
                            "current_mcap": 1,
                            "token_symbol": 1,
                            "ca": "$ca_norm",
                            "ca_norm": 1,
                            "caller_name": 1,
                        }
                    },
                ],
            }
        },
        {
            "$addFields": {
                "_initial": {"$toDouble": {"$ifNull": ["$initial_mcap", 0]}},
                "_ath": {"$toDouble": {"$ifNull": ["$ath_mcap", 0]}},
                "_current": {"$toDouble": {"$ifNull": ["$current_mcap", 0]}},
            }
        },
        {"$addFields": {"_peak": {"$cond": [{"$gt": ["$_ath", "$_current"]}, "$_ath", "$_current"]}}},
        {
            "$addFields": {
                "_x_peak": {
                    "$cond": [
                        {"$gt": ["$_initial", 0]},
                        {"$divide": ["$_peak", "$_initial"]},
                        0,
                    ]
                }
            }
        },
        {"$sort": {"_x_peak": -1}},
        {"$limit": 1},
    ]
    rows = list(calls_collection.aggregate(pipeline, allowDiskUse=True))
    if not rows:
        return "N/A"
    row = rows[0]
    initial = float(row.get("initial_mcap", 1) or 1)
    ath = float(row.get("ath_mcap", initial) or initial)
    current = float(row.get("current_mcap", initial) or initial)
    best_x = max(ath, current) / max(initial, 1.0)
    token = token_label(row.get("token_symbol", ""), row.get("ca", ""))
    return f"{format_return(best_x)} by {row.get('caller_name', 'Unknown')} ({token})"


def best_win_text_from_snapshot(snapshot):
    if not snapshot:
        return "N/A"
    best = snapshot.get("best") or {}
    best_x = float(best.get("best_x", 0) or 0.0)
    if best_x <= 0:
        return "N/A"
    token = token_label(best.get("token_symbol", ""), best.get("ca", ""))
    return f"{format_return(best_x)} by {best.get('caller_name', 'Unknown')} ({token})"


def fetch_ranked_leaderboard_page(chat_id, time_filter, is_bottom, page, items_per_page):
    time_filter = time_filter or {}
    cached = get_leaderboard_page_cache(chat_id, time_filter, is_bottom, page, items_per_page)
    if cached is not None:
        return cached.get("rows", []), int(cached.get("total", 0) or 0)
    match_query = {**accepted_call_filter(chat_id), **(time_filter or {})}
    skip_rows = max(0, int(page)) * max(1, int(items_per_page))
    if not time_filter:
        ensure_rollups_ready(chat_id)
        query = {"chat_id": chat_id, "calls": {"$gte": MIN_CALLS_REQUIRED}}
        total = caller_rollups_collection.count_documents(query)
        if total > 0:
            sort_order = [
                ("score", ASCENDING if is_bottom else DESCENDING),
                ("avg_x", ASCENDING if is_bottom else DESCENDING),
                ("best_x", ASCENDING if is_bottom else DESCENDING),
                ("win_rate", ASCENDING if is_bottom else DESCENDING),
                ("calls", DESCENDING),
            ]
            rows = list(
                caller_rollups_collection.find(
                    query,
                    {
                        "_id": 0,
                        "caller_id": 1,
                        "name": 1,
                        "calls": 1,
                        "avg_x": 1,
                        "best_x": 1,
                        "win_rate": 1,
                        "profitable_rate": 1,
                        "score": 1,
                    },
                )
                .sort(sort_order)
                .skip(skip_rows)
                .limit(max(1, int(items_per_page)))
            )
            mapped = [
                {
                    "caller_id": row.get("caller_id"),
                    "name": row.get("name", "Unknown"),
                    "calls": int(row.get("calls", 0) or 0),
                    "avg_now_x": float(row.get("avg_x", 0.0) or 0.0),
                    "best_x": float(row.get("best_x", 0.0) or 0.0),
                    "win_rate": float(row.get("win_rate", 0.0) or 0.0),
                    "profitable_rate": float(row.get("profitable_rate", 0.0) or 0.0),
                    "score": float(row.get("score", 0.0) or 0.0),
                }
                for row in rows
            ]
            set_leaderboard_page_cache(
                chat_id,
                time_filter,
                is_bottom,
                page,
                items_per_page,
                {"rows": mapped, "total": int(total)},
            )
            return mapped, int(total)
        set_leaderboard_page_cache(
            chat_id,
            time_filter,
            is_bottom,
            page,
            items_per_page,
            {"rows": [], "total": 0},
        )
        return [], 0

    group_pipeline = [
        {"$match": match_query},
        {
            "$project": {
                "caller_id": 1,
                "caller_name": 1,
                "initial_mcap": 1,
                "ath_mcap": 1,
                "current_mcap": 1,
            }
        },
        {
            "$unionWith": {
                "coll": "token_calls_archive",
                "pipeline": [
                    {"$match": match_query},
                    {
                        "$project": {
                            "caller_id": 1,
                            "caller_name": 1,
                            "initial_mcap": 1,
                            "ath_mcap": 1,
                            "current_mcap": 1,
                        }
                    },
                ],
            }
        },
        {
            "$addFields": {
                "_initial": {"$toDouble": {"$ifNull": ["$initial_mcap", 0]}},
                "_ath": {"$toDouble": {"$ifNull": ["$ath_mcap", 0]}},
                "_current": {"$toDouble": {"$ifNull": ["$current_mcap", 0]}},
                "_name": {"$ifNull": ["$caller_name", "Unknown"]},
            }
        },
        {"$addFields": {"_peak": {"$cond": [{"$gt": ["$_ath", "$_current"]}, "$_ath", "$_current"]}}},
        {
            "$addFields": {
                "_x_peak": {
                    "$cond": [
                        {"$gt": ["$_initial", 0]},
                        {"$divide": ["$_peak", "$_initial"]},
                        0,
                    ]
                },
                "_caller_key": {
                    "$cond": [
                        {"$ne": ["$caller_id", None]},
                        {"$concat": ["id:", {"$toString": "$caller_id"}]},
                        {"$concat": ["legacy:", {"$toLower": "$_name"}]},
                    ]
                },
            }
        },
        {"$match": {"_x_peak": {"$gt": 0}}},
        {
            "$group": {
                "_id": "$_caller_key",
                "caller_id": {"$first": "$caller_id"},
                "name": {"$first": "$_name"},
                "calls": {"$sum": 1},
                "avg_now_x": {"$avg": "$_x_peak"},
                "best_x": {"$max": "$_x_peak"},
                "wins": {"$sum": {"$cond": [{"$gte": ["$_x_peak", WIN_MULTIPLIER]}, 1, 0]}},
                "profit": {"$sum": {"$cond": [{"$gt": ["$_x_peak", 1]}, 1, 0]}},
            }
        },
        {"$match": {"calls": {"$gte": MIN_CALLS_REQUIRED}}},
        {
            "$addFields": {
                "win_rate": {
                    "$multiply": [
                        {"$cond": [{"$gt": ["$calls", 0]}, {"$divide": ["$wins", "$calls"]}, 0]},
                        100,
                    ]
                },
                "profitable_rate": {
                    "$multiply": [
                        {"$cond": [{"$gt": ["$calls", 0]}, {"$divide": ["$profit", "$calls"]}, 0]},
                        100,
                    ]
                },
                "score": mongo_performance_score_expr(
                    calls_expr="$calls",
                    wins_expr="$wins",
                    profitables_expr="$profit",
                    avg_x_expr="$avg_now_x",
                    best_x_expr="$best_x",
                ),
            }
        },
        {
            "$project": {
                "_id": 0,
                "caller_id": 1,
                "name": 1,
                "calls": 1,
                "avg_now_x": 1,
                "best_x": 1,
                "win_rate": 1,
                "profitable_rate": 1,
                "score": 1,
            }
        },
    ]

    count_pipeline = group_pipeline + [{"$count": "total"}]
    count_rows = list(calls_collection.aggregate(count_pipeline, allowDiskUse=True))
    total = int(count_rows[0]["total"]) if count_rows else 0

    sort_stage = (
        {"$sort": {"score": 1, "avg_now_x": 1, "best_x": 1, "win_rate": 1, "calls": -1}}
        if is_bottom
        else {"$sort": {"score": -1, "avg_now_x": -1, "best_x": -1, "win_rate": -1, "calls": -1}}
    )
    page_pipeline = group_pipeline + [sort_stage, {"$skip": skip_rows}, {"$limit": max(1, int(items_per_page))}]
    rows = list(calls_collection.aggregate(page_pipeline, allowDiskUse=True))
    set_leaderboard_page_cache(
        chat_id,
        time_filter,
        is_bottom,
        page,
        items_per_page,
        {"rows": rows, "total": int(total)},
    )
    return rows, total


async def _fetch_and_calculate_rankings(
    update: Update,
    context: ContextTypes.DEFAULT_TYPE,
    is_bottom=False,
    target_chat_id=None,
):
    chat_id = int(target_chat_id) if target_chat_id is not None else update.effective_chat.id
    time_filter, time_text = _resolve_time_filter(context)
    first_page_rows, total_ranked = fetch_ranked_leaderboard_page(
        chat_id=chat_id,
        time_filter=time_filter,
        is_bottom=is_bottom,
        page=0,
        items_per_page=6,
    )
    if total_ranked <= 0 or not first_page_rows:
        await update.effective_message.reply_text(
            f"No one has reached the minimum {MIN_CALLS_REQUIRED} calls to be ranked",
            reply_markup=delete_button_markup(update.effective_user.id if update.effective_user else 0),
        )
        return

    snapshot = None
    if not is_bottom:
        time_arg_key = str(context.args[0]).strip().lower() if context.args else "all"
        snapshot = get_groupstats_cache(chat_id, time_arg_key)

    if is_bottom:
        title = f"Wall of Shame ({time_text})"
        worst_row = first_page_rows[0]
        highlight_label = "☠️ Worst Avg"
        highlight_text = f"{format_return(worst_row['avg_now_x'])} by {worst_row['name']}"
    else:
        title = f"Yabai Callers ({time_text})"
        highlight_label = "🔥 Best Win"
        highlight_text = best_win_text_from_snapshot(snapshot)
        if highlight_text == "N/A":
            highlight_text = fetch_best_win_text(chat_id, time_filter)

    context.chat_data["leaderboard_chat_id"] = chat_id
    context.chat_data["leaderboard_time_filter"] = time_filter
    context.chat_data["leaderboard_is_bottom"] = bool(is_bottom)
    context.chat_data["leaderboard_title"] = title
    context.chat_data["leaderboard_total"] = total_ranked
    context.chat_data["leaderboard_highlight_label"] = highlight_label
    context.chat_data["leaderboard_highlight_text"] = highlight_text
    context.chat_data["leaderboard_owner_id"] = (update.effective_user.id if update.effective_user else 0)
    context.chat_data["leaderboard_image_mode"] = False

    try:
        group_avatar_image = await fetch_chat_avatar_image_cached(context.bot, chat_id)
        top = first_page_rows[0]
        spotlight = generate_leaderboard_spotlight_card(
            title=ascii_safe(title, fallback="Yabai Leaderboard"),
            top_name=ascii_safe(top["name"], fallback="Top Caller"),
            top_avg=ascii_safe(format_return(top["avg_now_x"]), fallback="N/A"),
            top_best=ascii_safe(format_return(top["best_x"]), fallback="N/A"),
            top_win_rate=top["win_rate"],
            highlight_text=ascii_safe(highlight_text, fallback="N/A"),
            highlight_label=ascii_safe(highlight_label, fallback="Highlight"),
            theme="danger" if is_bottom else "leaderboard",
            group_avatar_image=group_avatar_image,
        )
        context.chat_data["leaderboard_image_mode"] = True
        caption_text = compose_leaderboard_page_text(
            page_data=first_page_rows,
            page=0,
            items_per_page=6,
            total_ranked=total_ranked,
            title=title,
            highlight_label=highlight_label,
            highlight_text=highlight_text,
            image_mode=True,
        )
        reply_markup = build_leaderboard_reply_markup(
            page=0,
            items_per_page=6,
            total_ranked=total_ranked,
            owner_id=context.chat_data["leaderboard_owner_id"],
        )
        sent = await update.effective_message.reply_photo(
            photo=spotlight,
            caption=caption_text,
            reply_markup=reply_markup,
        )
        save_leaderboard_session(sent, snapshot_leaderboard_state(context))
        return
    except Exception:
        context.chat_data["leaderboard_image_mode"] = False

    text, reply_markup = build_leaderboard_page(context, page=0)
    sent = await update.effective_message.reply_text(text, reply_markup=reply_markup)
    save_leaderboard_session(sent, snapshot_leaderboard_state(context))


async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return
    await _fetch_and_calculate_rankings(update, context, is_bottom=False, target_chat_id=chat_id)


async def bottom(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return
    await _fetch_and_calculate_rankings(update, context, is_bottom=True, target_chat_id=chat_id)


def compose_leaderboard_page_text(
    page_data,
    page,
    items_per_page,
    total_ranked,
    title,
    highlight_label,
    highlight_text,
    image_mode=False,
):
    total_pages = max(1, math.ceil(max(0, int(total_ranked or 0)) / max(1, int(items_per_page))))
    start_idx = max(0, int(page or 0)) * max(1, int(items_per_page))
    lines = [
        f"🏆 {title.upper()}",
        f"📄 Page {int(page or 0) + 1}/{total_pages}",
        f"{highlight_label}: {highlight_text}",
        "────────────────",
    ]

    for idx, row in enumerate(page_data, start=start_idx + 1):
        badge = rank_badge(idx)
        trend_emoji = "📉" if float(row.get("avg_now_x", 0) or 0) < 1 else "📈"
        lines.append(
            f"{badge} {row.get('name', 'Unknown')}\n"
            f"↳ ⭐ Score: {float(row.get('score', 0) or 0):.1f} | {trend_emoji} Avg: {format_return(row.get('avg_now_x', 0))}\n"
            f"↳ 🔥 Best: {format_return(row.get('best_x', 0))} | 🎯 Win: {float(row.get('win_rate', 0) or 0):.1f}% | 📞 Calls {int(row.get('calls', 0) or 0)}"
        )
        lines.append("────────────────")

    text = "\n".join(lines).strip()
    if image_mode and len(text) > 1020:
        text = text[:1017] + "..."
    return text


def build_leaderboard_reply_markup(page, items_per_page, total_ranked, owner_id):
    total_pages = max(1, math.ceil(max(0, int(total_ranked or 0)) / max(1, int(items_per_page))))
    page = max(0, min(int(page or 0), total_pages - 1))
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("Prev", callback_data=f"lb_{page-1}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next", callback_data=f"lb_{page+1}"))

    reply_markup = InlineKeyboardMarkup([buttons]) if buttons else None
    return with_delete_button(reply_markup, owner_id)


def build_leaderboard_page(context, page=0):
    chat_id = context.chat_data.get("leaderboard_chat_id")
    time_filter = context.chat_data.get("leaderboard_time_filter", {}) or {}
    is_bottom = bool(context.chat_data.get("leaderboard_is_bottom", False))
    title = context.chat_data.get("leaderboard_title", "Leaderboard")
    highlight_label = context.chat_data.get("leaderboard_highlight_label", "🔥 Best Win")
    highlight_text = context.chat_data.get("leaderboard_highlight_text", "N/A")
    image_mode = bool(context.chat_data.get("leaderboard_image_mode", False))
    owner_id = int(context.chat_data.get("leaderboard_owner_id", 0) or 0)
    items_per_page = 6 if image_mode else 10
    if chat_id is None:
        return "Data expired. Run the command again.", None

    total_ranked = int(context.chat_data.get("leaderboard_total", 0) or 0)
    total_pages = max(1, math.ceil(max(0, total_ranked) / items_per_page))
    page = max(0, min(int(page or 0), total_pages - 1))

    page_data, _ = fetch_ranked_leaderboard_page(
        chat_id=chat_id,
        time_filter=time_filter,
        is_bottom=is_bottom,
        page=page,
        items_per_page=items_per_page,
    )
    text = compose_leaderboard_page_text(
        page_data=page_data,
        page=page,
        items_per_page=items_per_page,
        total_ranked=total_ranked,
        title=title,
        highlight_label=highlight_label,
        highlight_text=highlight_text,
        image_mode=image_mode,
    )
    reply_markup = build_leaderboard_reply_markup(
        page=page,
        items_per_page=items_per_page,
        total_ranked=total_ranked,
        owner_id=owner_id,
    )
    return text, reply_markup


async def render_leaderboard_page(message_obj, context, page=0):
    image_mode = bool(context.chat_data.get("leaderboard_image_mode", False))
    text, reply_markup = build_leaderboard_page(context, page=page)

    try:
        if image_mode:
            await message_obj.edit_caption(caption=text, reply_markup=reply_markup)
        else:
            await message_obj.edit_text(text, reply_markup=reply_markup)
    except Exception:
        pass


async def paginate_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    page = int(query.data.split("_")[1])

    session_state = load_leaderboard_session(query.message)
    if session_state:
        apply_leaderboard_state(context, session_state)

    if "leaderboard_chat_id" in context.chat_data:
        await render_leaderboard_page(query.message, context, page)
        save_leaderboard_session(query.message, snapshot_leaderboard_state(context))
    else:
        try:
            if getattr(query.message, "photo", None):
                await query.message.edit_caption(caption="Data expired. Run the command again.")
            else:
                await query.message.edit_text("Data expired. Run the command again.")
        except Exception:
            await query.message.reply_text("Data expired. Run the command again.")


async def delete_bot_message(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    if query is None or query.message is None:
        return

    data = str(query.data or "")
    try:
        owner_id = int(data.split(":", 1)[1])
    except (IndexError, ValueError, TypeError):
        owner_id = 0

    actor_id = query.from_user.id if query.from_user else 0
    chat = query.message.chat
    can_delete = bool(owner_id > 0 and actor_id == owner_id)

    if not can_delete:
        if chat and chat.type == "private":
            can_delete = bool(owner_id <= 0 or actor_id == owner_id)
        elif chat:
            try:
                can_delete = await user_is_admin(context.bot, chat.id, actor_id, query.from_user)
            except Exception:
                can_delete = False

    if not can_delete:
        await query.answer("Only requester/admin can delete this.", show_alert=True)
        return

    try:
        await query.message.delete()
    except Exception:
        try:
            await query.message.edit_reply_markup(reply_markup=None)
            await query.answer("Could not delete message; removed buttons.")
        except Exception:
            await query.answer("Unable to delete this message.", show_alert=True)
        return

    await query.answer("Deleted")


async def caller_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    requester_id = update.effective_user.id if update.effective_user else 0
    if not context.args:
        await update.effective_message.reply_text(
            "Provide a name or @username. Example: /caller John",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    target = " ".join(context.args).strip()
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return

    identity = resolve_caller_identity(chat_id, target)
    base_query = identity.get("query") or {"chat_id": chat_id}
    query = _accepted_query(chat_id, {k: v for k, v in base_query.items() if k != "chat_id"})

    live_user_calls = list(calls_collection.find(query).sort("timestamp", -1))
    archived_user_calls = list(calls_archive_collection.find(query).sort("timestamp", -1))
    all_user_calls = sorted(live_user_calls + archived_user_calls, key=lambda c: c.get("timestamp", utc_now()), reverse=True)
    if not all_user_calls:
        await update.effective_message.reply_text(
            f"No calls found for '{identity.get('target') or target}' in this group",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    refresh_calls_market_data(live_user_calls)
    enriched_calls = enrich_calls_with_live_meta(all_user_calls, limit=CALLER_LIVE_METRIC_REFRESH_LIMIT)
    metrics = derive_user_metrics(enriched_calls)
    rug = derive_rug_stats(enriched_calls)

    recent_calls = enriched_calls[:5]
    actual_name = recent_calls[0].get("caller_name", "Unknown")
    caller_id = recent_calls[0].get("caller_id")
    win_pct = metrics["win_rate"] * 100
    caller_score = float(metrics["reputation"])
    avg_text = format_return(1 + metrics["avg_ath"])
    best_text = format_return(metrics["best_x"])
    stars = stars_from_score(caller_score)

    lines = [
        f"👤 {html.escape(actual_name)}  {stars}",
        "────────────────",
        f"📞 Calls: {metrics['calls']}",
        f"📈 Avg: {avg_text} | 🔥 Best: {best_text}",
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_pct:.1f}%",
        f"⭐ Score: {caller_score:.1f}/100",
        f"🩸 Rug Calls: {rug['rug_rate']:.1f}% ({rug['rug_count']}/{rug['total']})",
        f"🏅 Badges: {html.escape(', '.join(metrics['badges']) if metrics['badges'] else 'None')}",
        "",
        "📚 Recent 5 Calls",
        "────────────────",
    ]

    for call in recent_calls:
        ca = call.get("ca", "") or call.get("ca_norm", "")
        initial = float(call.get("initial_mcap", 0) or 0)
        current = float(call.get("current_mcap", initial) or initial)
        ath = float(call.get("ath_mcap", current) or current)
        if initial <= 0:
            continue
        call_date = call.get("timestamp", utc_now()).strftime("%Y-%m-%d")
        ca_norm = call.get("ca_norm", normalize_ca(ca))
        symbol = call.get("token_symbol", "")
        token = token_label(symbol, ca)
        lines.append(
            f"• {html.escape(token)} ({call_date})\n"
            f"   📈 Peak: {format_return(ath / initial)} | 💰 Now: {format_return(current / initial)}\n"
            f"   <code>{html.escape(ca)}</code>"
        )
        lines.append("────────────────")

    reply_markup = None
    if caller_id is not None:
        reply_markup = InlineKeyboardMarkup(
            [[InlineKeyboardButton("📊 Mini Chart", callback_data=f"chart_caller:{chat_id}:{caller_id}")]]
        )
    reply_markup = with_delete_button(reply_markup, requester_id)

    caption = "\n".join(lines)
    avatar_image = None
    if caller_id is not None:
        try:
            photos = await context.bot.get_user_profile_photos(user_id=caller_id, limit=1)
            if photos and photos.total_count > 0 and photos.photos and photos.photos[0]:
                file_obj = await context.bot.get_file(photos.photos[0][-1].file_id)
                data = await file_obj.download_as_bytearray()
                avatar_image = Image.open(BytesIO(data)).convert("RGB")
        except Exception:
            avatar_image = None

    try:
        card = generate_caller_profile_card(
            display_name=actual_name,
            stars=stars,
            calls=metrics["calls"],
            avg_text=avg_text,
            best_text=best_text,
            hit_rate_pct=win_pct,
            score_value=caller_score,
            rug_text=f"Rug {rug['rug_count']}/{rug['total']}",
            badges_text=", ".join(metrics["badges"]) if metrics["badges"] else "None",
            avatar_image=avatar_image,
        )
        caption_text = caption if len(caption) <= 1024 else (caption[:1021] + "...")
        await update.effective_message.reply_photo(
            photo=card,
            caption=caption_text,
            parse_mode="HTML",
            reply_markup=reply_markup,
        )
        return
    except Exception:
        pass

    await update.effective_message.reply_text(caption, parse_mode="HTML", reply_markup=reply_markup)


async def my_score(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return
    user = update.effective_user
    requester_id = user.id if user else 0

    live_calls = list(
        calls_collection.find(
            {
                "chat_id": chat_id,
                "caller_id": user.id,
                "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
            }
        )
    )
    archived_calls = list(
        calls_archive_collection.find(
            {
                "chat_id": chat_id,
                "caller_id": user.id,
                "$or": [{"status": "accepted"}, {"status": {"$exists": False}}],
            }
        )
    )
    user_calls = live_calls + archived_calls

    if not user_calls:
        await update.effective_message.reply_text(
            "You do not have tracked calls yet.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    refresh_calls_market_data(live_calls)
    metrics = derive_user_metrics(user_calls)
    rug = derive_rug_stats(user_calls)

    win_pct = metrics["win_rate"] * 100
    score = float(metrics["reputation"])
    stars = stars_from_score(score)

    text = (
        f"📈 Your Performance  {stars}\n"
        f"────────────────\n"
        f"📞 Calls: {metrics['calls']}\n"
        f"📈 Avg: {format_return(1 + metrics['avg_ath'])} | 🔥 Best: {format_return(metrics['best_x'])}\n"
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_pct:.1f}%\n"
        f"⭐ Score: {score:.1f}/100\n"
        f"🩸 Rug Calls: {rug['rug_rate']:.1f}% ({rug['rug_count']}/{rug['total']})\n"
        f"🏅 Badges: {', '.join(metrics['badges']) if metrics['badges'] else 'None'}"
    )
    badges_text = ", ".join(metrics["badges"]) if metrics["badges"] else "None"
    caption = (
        f"📈 Your Performance  {stars}\n"
        f"────────────────\n"
        f"📞 Calls: {metrics['calls']}\n"
        f"📈 Avg: {format_return(1 + metrics['avg_ath'])} | 🔥 Best: {format_return(metrics['best_x'])}\n"
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_pct:.1f}%\n"
        f"⭐ Score: {score:.1f}/100\n"
        f"🩸 Rug Calls: {rug['rug_rate']:.1f}% ({rug['rug_count']}/{rug['total']})\n"
        f"🏅 Badges: {badges_text}"
    )

    try:
        card = generate_myscore_card(
            display_name=update.effective_user.full_name or update.effective_user.first_name or "Caller",
            stars=stars,
            calls=metrics["calls"],
            avg_text=format_return(1 + metrics["avg_ath"]),
            best_text=format_return(metrics["best_x"]),
            hit_rate_pct=win_pct,
            score_value=score,
            rug_text=f"Rug {rug['rug_count']}/{rug['total']}",
        )
        caption_text = caption if len(caption) <= 1024 else (caption[:1021] + "...")
        await update.effective_message.reply_photo(
            photo=card,
            caption=caption_text,
            reply_markup=delete_button_markup(requester_id),
        )
        return
    except Exception:
        pass

    await update.effective_message.reply_text(text, reply_markup=delete_button_markup(requester_id))


async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    requester_id = update.effective_user.id if update.effective_user else 0
    chat_id = await resolve_target_chat_id(update, context, admin_required=False)
    if chat_id is None:
        return

    time_filter, time_text = _resolve_time_filter(context)
    time_arg_key = context.args[0].lower() if context.args else "all"
    snapshot = get_groupstats_cache(chat_id, time_arg_key)
    snapshot_cache_hit = snapshot is not None
    if snapshot is None:
        snapshot = compute_group_stats_snapshot(chat_id, time_filter)
        if snapshot is not None:
            set_groupstats_cache(chat_id, time_arg_key, snapshot)

    if not snapshot:
        await update.effective_message.reply_text(
            f"No calls tracked in this group for {time_text}",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    total_calls = int(snapshot.get("total_calls", 0) or 0)
    callers_count = int(snapshot.get("unique_callers", 0) or 0)
    win_rate = float(snapshot.get("win_rate", 0.0) or 0.0)
    avg_x = float(snapshot.get("avg_x", 1.0) or 1.0)
    best = snapshot.get("best")

    best_text = "N/A"
    best_caller = "N/A"
    if best:
        best_x = float(best.get("best_x", 0) or 0)
        best_caller = str(best.get("caller_name") or "Unknown")
        best_ca = str(best.get("ca") or "")
        best_symbol = str(best.get("token_symbol") or "")
        best_token = token_label(best_symbol, best_ca)
        best_text = format_return(best_x)
        if best_ca:
            best_by_text = f"   └ By {html.escape(best_caller)} ({html.escape(best_token)})\n   <code>{html.escape(best_ca)}</code>"
        else:
            best_by_text = f"   └ By {html.escape(best_caller)} ({html.escape(best_token)})"
    else:
        best_by_text = "   └ By N/A"

    text = (
        f"📊 Group Performance ({time_text.upper()})\n"
        f"────────────────\n"
        f"👥 Callers: {callers_count} | 📞 Calls: {total_calls}\n"
        f"🎯 Hit Rate {WIN_MULTIPLIER:.1f}x: {win_rate * 100:.1f}%\n"
        f"📈 Group Avg: {format_return(avg_x)}\n"
        f"🔥 Best Call: {best_text}\n"
        f"{best_by_text}"
    )

    reply_markup = InlineKeyboardMarkup(
        [[InlineKeyboardButton("📊 Mini Chart", callback_data=f"chart_group:{chat_id}")]]
    )
    reply_markup = with_delete_button(reply_markup, requester_id)

    if snapshot_cache_hit:
        cached_file_id = get_groupstats_media_cache(chat_id, time_arg_key)
        if cached_file_id:
            await update.effective_message.reply_photo(
                photo=cached_file_id,
                caption=text,
                parse_mode="HTML",
                reply_markup=reply_markup,
            )
            return

    group_avatar_image = await fetch_chat_avatar_image_cached(context.bot, chat_id)

    card_image = generate_group_stats_card(
        time_text=time_text,
        callers_count=callers_count,
        total_calls=total_calls,
        win_rate_pct=win_rate * 100,
        avg_text=format_return(avg_x),
        best_text=best_text,
        best_caller=best_caller,
        group_avatar_image=group_avatar_image,
    )

    sent = await update.effective_message.reply_photo(
        photo=card_image,
        caption=text,
        parse_mode="HTML",
        reply_markup=reply_markup,
    )
    try:
        if sent and getattr(sent, "photo", None):
            file_id = sent.photo[-1].file_id
            set_groupstats_media_cache(chat_id, time_arg_key, file_id)
    except Exception:
        pass


async def admin_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    requester_id = update.effective_user.id if update.effective_user else 0

    chat_id = await resolve_target_chat_id(update, context, admin_required=True)
    if chat_id is None:
        return

    group_key = ensure_group_key(chat_id) or "N/A"

    base = {"chat_id": chat_id}
    accepted = (
        calls_collection.count_documents({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]})
        + calls_archive_collection.count_documents({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]})
    )
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
        user_profiles_collection.find({"chat_id": chat_id})
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

    recent_calls = (
        list(calls_collection.find({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]}))
        + list(calls_archive_collection.find({**base, "$or": [{"status": "accepted"}, {"status": {"$exists": False}}]}))
    )
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
                "avg_now_x": 1 + m["avg_ath"],
            }
        )
    low_performers.sort(key=lambda x: (x["win_rate"], x["avg_now_x"]))

    accepted_query = accepted_call_filter(chat_id)
    tracked_calls = calls_collection.count_documents(accepted_query)
    stashed_calls = calls_collection.count_documents({**accepted_query, "is_stashed": True})
    active_calls = max(0, tracked_calls - stashed_calls)
    stashed_pct = (stashed_calls / tracked_calls * 100.0) if tracked_calls > 0 else 0.0

    now_ts = time.time()
    cache_total = len(_dex_meta_cache)
    cache_live = sum(1 for entry in _dex_meta_cache.values() if entry.get("expires_at", 0) > now_ts)

    runtime_map = _ops_runtime.get("by_chat", {})
    runtime = (
        runtime_map.get(chat_id)
        or runtime_map.get(str(chat_id))
        or runtime_map.get(canonical_chat_id(chat_id))
        or {}
    )
    last_heartbeat_at = runtime.get("last_heartbeat_at")
    if isinstance(last_heartbeat_at, datetime):
        last_heartbeat_text = last_heartbeat_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        next_heartbeat_at = last_heartbeat_at + timedelta(seconds=HEARTBEAT_INTERVAL_SECONDS)
        next_heartbeat_text = next_heartbeat_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
    else:
        last_heartbeat_text = "N/A"
        next_heartbeat_at = None
        next_heartbeat_text = "N/A (waiting first heartbeat)"

    avg_refresh_ms = float(runtime.get("avg_refresh_duration_ms", 0.0) or 0.0)
    last_refresh_ms = float(runtime.get("last_refresh_duration_ms", 0.0) or 0.0)
    refresh_runs = int(runtime.get("refresh_runs", 0) or 0)
    last_refreshed_calls = int(runtime.get("last_refreshed_calls", 0) or 0)

    setting = settings_collection.find_one({"chat_id": chat_id}) or {}
    alerts_enabled = bool(setting.get("alerts", False))
    now_utc = utc_now()
    today = now_utc.strftime("%Y-%m-%d")
    last_digest_date = setting.get("last_digest_date")
    digest_hour_today = now_utc.replace(hour=DIGEST_HOUR_UTC, minute=0, second=0, microsecond=0)

    if not alerts_enabled:
        next_digest_text = "Disabled (alerts off)"
    else:
        if last_digest_date == today:
            next_digest_at = digest_hour_today + timedelta(days=1)
        elif now_utc < digest_hour_today:
            next_digest_at = digest_hour_today
        else:
            next_digest_at = next_heartbeat_at

        if isinstance(next_digest_at, datetime):
            next_digest_text = next_digest_at.astimezone(timezone.utc).strftime("%Y-%m-%d %H:%M:%S UTC")
        else:
            next_digest_text = "Pending (next heartbeat)"

    lines = [
        "🛡️ Admin Panel",
        "────────────────",
        f"✅ Accepted: {accepted} | ❌ Rejected: {rejected}",
        f"🎯 Acceptance: {(accepted / (accepted + rejected) * 100) if (accepted + rejected) else 0:.1f}%",
        f"🔑 Group Key: {group_key}",
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

    lines.append("")
    lines.append("📡 Ops Snapshot")
    lines.append("────────────────")
    lines.append(f"🗂 Tracked Calls: {tracked_calls}")
    lines.append(f"🧊 Stashed/Active: {stashed_calls}/{active_calls} ({stashed_pct:.1f}% stashed)")
    lines.append(f"🧠 Dex Cache: {cache_live}/{cache_total} live entries")
    lines.append(f"💓 Last Heartbeat: {last_heartbeat_text}")
    lines.append(f"⏭ Next Heartbeat: {next_heartbeat_text}")
    lines.append(f"📰 Next Digest: {next_digest_text}")
    lines.append(f"⏱ Avg Refresh: {avg_refresh_ms:.1f}ms (last {last_refresh_ms:.1f}ms)")
    lines.append(f"🔁 Refresh Runs: {refresh_runs} | Last Refreshed Calls: {last_refreshed_calls}")

    admin_stats_markup = InlineKeyboardMarkup(
        [
            [InlineKeyboardButton("Kick Watchlist", callback_data="admin_kicklist")],
            [InlineKeyboardButton("🗑 Delete", callback_data=delete_callback_data(requester_id))],
        ]
    )
    await msg.reply_text("\n".join(lines), reply_markup=admin_stats_markup)


async def clear_data(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    requester_id = update.effective_user.id if update.effective_user else 0
    chat_id = await resolve_target_chat_id(update, context, admin_required=True)
    if chat_id is None:
        return

    if not context.args:
        await msg.reply_text(
            "Usage: /cleardata <Nd|Nh>\nExample: /cleardata 20d",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    arg = str(context.args[0]).strip().lower()
    if len(arg) < 2 or arg[-1] not in {"d", "h"}:
        await msg.reply_text(
            "Invalid window. Use format like 20d or 48h.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    try:
        value = int(arg[:-1])
    except ValueError:
        await msg.reply_text(
            "Invalid window. Use format like 20d or 48h.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    if value <= 0:
        await msg.reply_text("Window must be greater than 0.", reply_markup=delete_button_markup(requester_id))
        return

    if arg.endswith("d"):
        cutoff = utc_now() - timedelta(days=value)
        window_text = f"{value}d"
    else:
        cutoff = utc_now() - timedelta(hours=value)
        window_text = f"{value}h"

    query = {"chat_id": chat_id, "timestamp": {"$lt": cutoff}}
    live_deleted = calls_collection.delete_many(query).deleted_count
    archive_deleted = calls_archive_collection.delete_many(query).deleted_count
    total_deleted = int(live_deleted or 0) + int(archive_deleted or 0)
    if total_deleted > 0:
        recompute_rollups_for_chat(chat_id)
        settings_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"rollup_version": ROLLUP_SCHEMA_VERSION}},
            upsert=True,
        )
    invalidate_groupstats_cache(chat_id)

    await msg.reply_text(
        "🧹 DATA CLEAR COMPLETE\n"
        "────────────────\n"
        f"Window kept: last {window_text}\n"
        f"Cutoff: {cutoff.astimezone(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}\n"
        f"Deleted live: {live_deleted}\n"
        f"Deleted archive: {archive_deleted}\n"
        f"Total deleted: {total_deleted}",
        reply_markup=delete_button_markup(requester_id),
    )


async def delete_call(update: Update, context: ContextTypes.DEFAULT_TYPE):
    msg = update.effective_message
    requester_id = update.effective_user.id if update.effective_user else 0
    chat_id = await resolve_target_chat_id(update, context, admin_required=True)
    if chat_id is None:
        return

    if len(context.args) < 2:
        await msg.reply_text(
            "Usage: /delete <@name|name> <ca>\nExample: /delete @yodizm 6r...pump",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    ca_raw = str(context.args[-1]).strip()
    target_raw = " ".join(context.args[:-1]).strip()
    ca_norm = normalize_ca(ca_raw)
    if not target_raw:
        await msg.reply_text(
            "Missing caller target. Usage: /delete <@name|name> <ca>",
            reply_markup=delete_button_markup(requester_id),
        )
        return
    if not ca_norm or len(ca_norm) < 20:
        await msg.reply_text(
            "Invalid CA format. Provide the full contract address.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    identity = resolve_caller_identity(chat_id, target_raw)
    base_query = identity.get("query") or {"chat_id": chat_id}
    delete_query = _accepted_query(
        chat_id,
        {
            **{k: v for k, v in base_query.items() if k != "chat_id"},
            "ca_norm": ca_norm,
        },
    )

    live_deleted = calls_collection.delete_many(delete_query).deleted_count
    archive_deleted = calls_archive_collection.delete_many(delete_query).deleted_count
    total_deleted = int(live_deleted or 0) + int(archive_deleted or 0)
    if total_deleted > 0:
        recompute_rollups_for_chat(chat_id)
        settings_collection.update_one(
            {"chat_id": chat_id},
            {"$set": {"rollup_version": ROLLUP_SCHEMA_VERSION}},
            upsert=True,
        )
        invalidate_groupstats_cache(chat_id)

    await msg.reply_text(
        "🗑 CALL DELETE RESULT\n"
        "────────────────\n"
        f"Caller: {identity.get('target') or target_raw}\n"
        f"CA: {ca_norm}\n"
        f"Deleted live: {live_deleted}\n"
        f"Deleted archive: {archive_deleted}\n"
        f"Total deleted: {total_deleted}",
        reply_markup=delete_button_markup(requester_id),
    )


async def send_group_mini_chart(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    time_arg: str = "7d",
    requester_id: int = 0,
):
    fake_context = type("obj", (), {"args": [time_arg]})()
    time_filter, time_text = _resolve_time_filter(fake_context)
    live_calls, archived_calls, calls = load_calls_for_stats(chat_id, time_filter, include_archive=True)
    if not calls:
        await context.bot.send_message(
            chat_id=chat_id,
            text=f"No data for {time_text} to chart.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    refresh_calls_market_data(live_calls)
    metrics = derive_user_metrics(calls)
    chart_url = build_performance_chart_url(
        f"Group Mini Chart ({time_text})",
        metrics["win_rate"] * 100.0,
        metrics["profitable_rate"] * 100.0,
        1.0 + metrics["avg_ath"],
    )
    caption = (
        f"📊 GROUP MINI CHART ({time_text.upper()})\n"
        f"────────────────\n"
        f"🎯 Win Rate: {metrics['win_rate'] * 100:.1f}%\n"
        f"💹 Profitable: {metrics['profitable_rate'] * 100:.1f}%\n"
        f"📈 Avg: {format_return(1.0 + metrics['avg_ath'])}"
    )
    await context.bot.send_photo(
        chat_id=chat_id,
        photo=chart_url,
        caption=caption,
        reply_markup=delete_button_markup(requester_id),
    )


async def send_caller_mini_chart(
    context: ContextTypes.DEFAULT_TYPE,
    chat_id: int,
    caller_id: int,
    requester_id: int = 0,
):
    live_calls = list(
        calls_collection.find(_accepted_query(chat_id, {"caller_id": caller_id}))
        .sort("timestamp", -1)
        .limit(50)
    )
    archived_calls = list(
        calls_archive_collection.find(_accepted_query(chat_id, {"caller_id": caller_id}))
        .sort("timestamp", -1)
        .limit(200)
    )
    calls = sorted(live_calls + archived_calls, key=lambda c: c.get("timestamp", utc_now()), reverse=True)[:50]
    if not calls:
        await context.bot.send_message(
            chat_id=chat_id,
            text="No caller data found for chart.",
            reply_markup=delete_button_markup(requester_id),
        )
        return

    refresh_calls_market_data(live_calls)
    metrics = derive_user_metrics(calls)
    caller_name = calls[0].get("caller_name", f"User {caller_id}")
    chart_url = build_performance_chart_url(
        f"{caller_name} Mini Chart",
        metrics["win_rate"] * 100.0,
        metrics["profitable_rate"] * 100.0,
        1.0 + metrics["avg_ath"],
    )
    caption = (
        f"📊 CALLER MINI CHART\n"
        f"────────────────\n"
        f"👤 {caller_name}\n"
        f"🎯 Win Rate: {metrics['win_rate'] * 100:.1f}%\n"
        f"💹 Profitable: {metrics['profitable_rate'] * 100:.1f}%\n"
        f"📈 Avg: {format_return(1.0 + metrics['avg_ath'])} | 🔥 Best: {format_return(metrics['best_x'])}"
    )
    await context.bot.send_photo(
        chat_id=chat_id,
        photo=chart_url,
        caption=caption,
        reply_markup=delete_button_markup(requester_id),
    )


def top_caller_id(chat_id: int, lookback_days: int = 7):
    cutoff = utc_now() - timedelta(days=lookback_days)
    calls = (
        list(calls_collection.find(_accepted_query(chat_id, {"timestamp": {"$gte": cutoff}})))
        + list(calls_archive_collection.find(_accepted_query(chat_id, {"timestamp": {"$gte": cutoff}})))
    )
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
        score = float(metrics["reputation"])
        if metrics["calls"] >= 2 and score > best_score:
            best = caller_id
            best_score = score
    return best


async def admin_panel(update: Update, context: ContextTypes.DEFAULT_TYPE):
    user = update.effective_user
    target_chat_id = await resolve_target_chat_id(update, context, admin_required=True)
    if target_chat_id is None:
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
            [
                InlineKeyboardButton("⚡ Refresh All ATH Now", callback_data="admin_refresh_ath"),
            ],
            [
                InlineKeyboardButton("🗑 Delete", callback_data=delete_callback_data(user.id)),
            ],
        ]
    )
    await update.effective_message.reply_text(
        "🛠️ ADMIN TEST PANEL\n────────────────\nTrigger streaks, digest, chart events, and ATH refresh safely.",
        reply_markup=keyboard,
    )


async def admin_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    chat_id = resolve_callback_target_chat_id(query)
    user_id = query.from_user.id

    if chat_id is None:
        await query.message.reply_text("No linked group. Use /linkgroup <group_key> in private chat.")
        return

    if not await user_is_admin(context.bot, chat_id, user_id, query.from_user):
        await query.message.reply_text("Admin only action")
        return

    action = query.data
    if action == "admin_streak":
        count = await run_streak_scan_for_chat(context.bot, chat_id, manual=True)
        await query.message.reply_text(
            f"🔥 STREAK TEST COMPLETE\n────────────────\nAlerts sent: {count}",
            reply_markup=delete_button_markup(user_id),
        )
    elif action == "admin_digest":
        await send_daily_digest(context.bot, chat_id, manual=True)
        await query.message.reply_text(
            "📰 DIGEST TEST COMPLETE\n────────────────\nDaily digest sent.",
            reply_markup=delete_button_markup(user_id),
        )
    elif action == "admin_group_chart":
        await send_group_mini_chart(context, chat_id, time_arg="7d", requester_id=user_id)
    elif action == "admin_top_caller_chart":
        caller_id = top_caller_id(chat_id, lookback_days=7)
        if caller_id is None:
            await query.message.reply_text(
                "No top caller found for chart.",
                reply_markup=delete_button_markup(user_id),
            )
            return
        await send_caller_mini_chart(context, chat_id, caller_id, requester_id=user_id)
    elif action == "admin_refresh_ath":
        stats = refresh_all_call_peaks(chat_id)
        if stats["calls"] == 0:
            await query.message.reply_text(
                "No tracked calls to refresh.",
                reply_markup=delete_button_markup(user_id),
            )
            return
        await query.message.reply_text(
            "⚡ ATH REFRESH COMPLETE\n────────────────\n"
            f"Calls scanned: {stats['calls']}\n"
            f"Live/Archive scanned: {stats.get('live_calls', 0)}/{stats.get('archived_calls', 0)}\n"
            f"Tokens scanned: {stats['tokens']}\n"
            f"Historical checked/updated: {stats.get('historical_checked', 0)}/{stats.get('historical_updated', 0)}\n"
            f"Records updated: {stats['updated']}",
            reply_markup=delete_button_markup(user_id),
        )
    elif action == "admin_kicklist":
        text = build_kick_list_text(chat_id)
        await query.message.reply_text(text, reply_markup=delete_button_markup(user_id))


async def chart_actions(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer()
    data = query.data

    if data.startswith("chart_group:"):
        try:
            target_chat_id = int(data.split(":", 1)[1])
        except (TypeError, ValueError):
            await query.message.reply_text("Invalid group chart request.")
            return
        await send_group_mini_chart(context, target_chat_id, time_arg="7d", requester_id=query.from_user.id)
        return

    if data == "chart_group":
        await send_group_mini_chart(context, query.message.chat_id, time_arg="7d", requester_id=query.from_user.id)
        return

    if data.startswith("chart_caller:"):
        try:
            _, target_chat_raw, caller_raw = data.split(":", 2)
            target_chat_id = int(target_chat_raw)
            caller_id = int(caller_raw)
        except (TypeError, ValueError):
            await query.message.reply_text("Invalid caller chart request.")
            return
        await send_caller_mini_chart(context, target_chat_id, caller_id, requester_id=query.from_user.id)
        return

    if data.startswith("chart_caller_"):
        try:
            caller_id = int(data.split("_")[-1])
        except ValueError:
            await query.message.reply_text("Invalid caller chart request.")
            return
        await send_caller_mini_chart(context, query.message.chat_id, caller_id, requester_id=query.from_user.id)


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
    app.add_handler(CommandHandler("linkgroup", link_group))
    app.add_handler(CommandHandler("unlinkgroup", unlink_group))
    app.add_handler(CommandHandler("togglealerts", toggle_alerts))
    app.add_handler(CommandHandler("caller", caller_profile))
    app.add_handler(CommandHandler("groupstats", group_stats))
    app.add_handler(CommandHandler("myscore", my_score))
    app.add_handler(CommandHandler("adminstats", admin_stats))
    app.add_handler(CommandHandler("adminpanel", admin_panel))
    app.add_handler(CommandHandler("cleardata", clear_data))
    app.add_handler(CommandHandler("delete", delete_call))

    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern=r"^lb_"))
    app.add_handler(CallbackQueryHandler(delete_bot_message, pattern=r"^delm:"))
    app.add_handler(CallbackQueryHandler(admin_actions, pattern=r"^admin_"))
    app.add_handler(CallbackQueryHandler(chart_actions, pattern=r"^chart_"))

    print("YabaiRankBot running")
    app.run_polling()


if __name__ == "__main__":
    main()
