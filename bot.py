import os
import re
import requests
import certifi
import math
from datetime import datetime, timedelta, timezone
from pymongo import MongoClient
from telegram import Update, InlineKeyboardMarkup, InlineKeyboardButton
from telegram.ext import Application, MessageHandler, CommandHandler, CallbackQueryHandler, filters, ContextTypes

# --- CONFIGURATION ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# Define what constitutes a "Win" or a "Hit" (e.g., 2.0 means 2x multiplier)
WIN_MULTIPLIER = 2.0 

if not TOKEN or not MONGO_URI:
    raise ValueError("Missing TELEGRAM_TOKEN or MONGO_URI environment variables!")

client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client['yabai_crypto_bot']
calls_collection = db['token_calls']
settings_collection = db['group_settings'] 

CA_REGEX = r'\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b'

def get_dexscreener_batch(cas_list):
    """Fetches up to 30 tokens at once from DexScreener to save API requests."""
    results = {}
    for i in range(0, len(cas_list), 30):
        chunk = cas_list[i:i+30]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(chunk)}"
        try:
            response = requests.get(url, timeout=10).json()
            if response and response.get('pairs'):
                for pair in response['pairs']:
                    address = pair.get('baseToken', {}).get('address')
                    fdv = pair.get('fdv', 0)
                    if address and fdv > 0:
                        addr_lower = address.lower()
                        if addr_lower not in results or fdv > results[addr_lower]:
                            results[addr_lower] = fdv
        except Exception as e:
            print(f"Batch fetch error: {e}")
    return results

async def toggle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    setting = settings_collection.find_one({"chat_id": chat_id})
    
    if not setting or setting.get("alerts", True) == True:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.message.reply_text("🔕 **Alerts OFF**: I will track CAs silently here.", parse_mode='Markdown')
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.message.reply_text("🔔 **Alerts ON**: I will announce every new tracked CA here.", parse_mode='Markdown')

async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    message = update.message.text
    user = update.message.from_user
    chat_id = update.message.chat_id
    
    found_cas = set(re.findall(CA_REGEX, message))
    
    for ca in found_cas:
        existing_call = calls_collection.find_one({"ca": ca, "chat_id": chat_id})
        if existing_call:
            continue 
        
        batch_data = get_dexscreener_batch([ca])
        mcap = batch_data.get(ca.lower())
        
        if mcap and mcap > 0:
            call_data = {
                "ca": ca,
                "chat_id": chat_id,
                "caller_id": user.id,
                "caller_name": user.first_name or "Unknown",
                "caller_username": user.username,
                "initial_mcap": mcap,
                "ath_mcap": mcap,
                "current_mcap": mcap,
                "timestamp": datetime.now(timezone.utc)
            }
            calls_collection.insert_one(call_data)
            
            setting = settings_collection.find_one({"chat_id": chat_id})
            if not setting or setting.get("alerts", True) == True:
                await update.message.reply_text(
                    f"🎯 **New Call Tracked!**\n\n"
                    f"🪙 CA: `{ca}`\n"
                    f"👤 Caller: {user.first_name}\n"
                    f"💰 Called at MCAP: ${mcap:,.2f}",
                    parse_mode='Markdown'
                )

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    chat_id = update.message.chat_id
    query = {"chat_id": chat_id}
    time_text = "All Time"

    if context.args:
        time_arg = context.args[0].lower()
        try:
            if time_arg.endswith('d'):
                days = int(time_arg[:-1])
                cutoff = datetime.now(timezone.utc) - timedelta(days=days)
                query["timestamp"] = {"$gte": cutoff}
                time_text = f"Last {days} Days"
            elif time_arg.endswith('h'):
                hours = int(time_arg[:-1])
                cutoff = datetime.now(timezone.utc) - timedelta(hours=hours)
                query["timestamp"] = {"$gte": cutoff}
                time_text = f"Last {hours} Hours"
        except ValueError:
            pass

    status_message = await update.message.reply_text(f"🔄 Fetching {time_text} leaderboard...")
    
    all_calls = list(calls_collection.find(query))
    if not all_calls:
        await status_message.edit_text(f"No data for {time_text} in this group!")
        return

    unique_cas = list(set([call['ca'] for call in all_calls]))
    latest_mcaps = get_dexscreener_batch(unique_cas)
    
    user_stats = {} 

    for call in all_calls:
        ca = call['ca']
        current_mcap = latest_mcaps.get(ca.lower(), call['current_mcap'])
        ath = max(call['ath_mcap'], current_mcap)
        
        calls_collection.update_one({"_id": call['_id']}, {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}})
        
        multiplier = ath / call['initial_mcap']
        caller = call['caller_name']
        
        if caller not in user_stats:
            user_stats[caller] = {'mults': [], 'wins': 0}
        
        user_stats[caller]['mults'].append(multiplier)
        if multiplier >= WIN_MULTIPLIER:
            user_stats[caller]['wins'] += 1

    leaderboard_data = []
    for user, data in user_stats.items():
        total_calls = len(data['mults'])
        leaderboard_data.append({
            'name': user, 
            'avg': sum(data['mults']) / total_calls, 
            'best': max(data['mults']), 
            'total_calls': total_calls,
            'hit_rate': (data['wins'] / total_calls) * 100
        })

    leaderboard_data.sort(key=lambda x: x['avg'], reverse=True)
    context.chat_data['leaderboard_data'] = leaderboard_data
    context.chat_data['leaderboard_time_text'] = time_text
    
    await render_leaderboard_page(status_message, context, page=0)

async def render_leaderboard_page(message_obj, context, page=0):
    data = context.chat_data.get('leaderboard_data', [])
    time_text = context.chat_data.get('leaderboard_time_text', "All Time")
    
    items_per_page = 10
    total_pages = math.ceil(len(data) / items_per_page)
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_data = data[start_idx:end_idx]

    text = f"🏆 **Yabai Callers ({time_text})** 🏆\n*Page {page + 1} of {total_pages}*\n\n"
    
    for idx, user in enumerate(page_data, start=start_idx + 1):
        text += (f"{idx}. **{user['name']}** ({user['total_calls']} calls)\n"
                 f"   📈 Avg: {user['avg']:.2f}x | 🔥 Best: {user['best']:.2f}x\n"
                 f"   🎯 Win Rate: {user['hit_rate']:.1f}% (>= {WIN_MULTIPLIER}x)\n\n")

    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"lb_{page-1}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"lb_{page+1}"))
        
    reply_markup = InlineKeyboardMarkup([buttons]) if buttons else None

    try:
        await message_obj.edit_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception:
        pass

async def paginate_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    query = update.callback_query
    await query.answer() 
    page = int(query.data.split('_')[1])
    if 'leaderboard_data' in context.chat_data:
        await render_leaderboard_page(query.message, context, page)
    else:
        await query.message.edit_text("Leaderboard data expired. Please run /leaderboard again.")

async def caller_profile(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please provide a name. Example: `/caller John`", parse_mode='Markdown')
        return

    target = " ".join(context.args).replace("@", "")
    chat_id = update.message.chat_id
    
    query = {
        "chat_id": chat_id,
        "$or": [
            {"caller_name": {"$regex": f"^{target}$", "$options": "i"}},
            {"caller_username": {"$regex": f"^{target}$", "$options": "i"}}
        ]
    }
    
    all_user_calls = list(calls_collection.find(query).sort("timestamp", -1))
    
    if not all_user_calls:
        await update.message.reply_text(f"I couldn't find any calls for '{target}' in this group.")
        return

    # Calculate overall win rate for this user
    total_calls = len(all_user_calls)
    wins = sum(1 for c in all_user_calls if (max(c['ath_mcap'], c.get('current_mcap', 0)) / c['initial_mcap']) >= WIN_MULTIPLIER)
    hit_rate = (wins / total_calls) * 100

    # Get latest prices for top 5 recent plays to update DB
    recent_calls = all_user_calls[:5]
    ca_list = [c['ca'] for c in recent_calls]
    batch_data = get_dexscreener_batch(ca_list)
    
    actual_name = recent_calls[0]['caller_name']
    
    text = (f"👤 **Profile: {actual_name}** 👤\n"
            f"📞 Total Calls: {total_calls}\n"
            f"🎯 Hit Rate (>= {WIN_MULTIPLIER}x): **{hit_rate:.1f}%**\n\n"
            f"**Recent Plays:**\n\n")
    
    for call in recent_calls:
        ca = call['ca']
        current_mcap = batch_data.get(ca.lower(), call['current_mcap'])
        ath = max(call['ath_mcap'], current_mcap)
        
        calls_collection.update_one({"_id": call['_id']}, {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}})
        
        initial = call['initial_mcap']
        call_date = call.get('timestamp', datetime.now(timezone.utc)).strftime('%Y-%m-%d')
        short_ca = f"{ca[:6]}...{ca[-4:]}"
        
        text += (f"🪙 `{short_ca}` ({call_date})\n"
                 f"🟢 Entry: ${initial:,.0f}\n"
                 f"🔥 ATH: **{(ath/initial):.2f}x** | 💰 Now: **{(current_mcap/initial):.2f}x**\n\n")

    await update.message.reply_text(text, parse_mode='Markdown')

async def group_stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Generates an overview of the entire group's performance."""
    chat_id = update.message.chat_id
    status_message = await update.message.reply_text("🔄 Analyzing group performance...")

    all_calls = list(calls_collection.find({"chat_id": chat_id}))
    if not all_calls:
        await status_message.edit_text("No calls tracked in this group yet!")
        return

    # Calculate Group Stats
    total_calls = len(all_calls)
    unique_callers = set()
    total_wins = 0
    total_mults = []
    best_call_x = 0
    best_call_caller = ""
    best_call_ca = ""

    for call in all_calls:
        unique_callers.add(call['caller_name'])
        
        # Calculate max multiplier for this call
        mult = max(call['ath_mcap'], call['current_mcap']) / call['initial_mcap']
        total_mults.append(mult)
        
        if mult >= WIN_MULTIPLIER:
            total_wins += 1
            
        if mult > best_call_x:
            best_call_x = mult
            best_call_caller = call['caller_name']
            best_call_ca = call['ca']

    group_hit_rate = (total_wins / total_calls) * 100
    group_avg_x = sum(total_mults) / total_calls
    short_best_ca = f"{best_call_ca[:6]}...{best_call_ca[-4:]}" if best_call_ca else "N/A"

    text = (
        f"📊 **Group Performance Overview** 📊\n\n"
        f"👥 **Total Callers:** {len(unique_callers)}\n"
        f"📞 **Total Calls Tracked:** {total_calls}\n"
        f"🎯 **Group Win Rate (>= {WIN_MULTIPLIER}x):** {group_hit_rate:.1f}%\n\n"
        f"📈 **Group Average:** {group_avg_x:.2f}x\n"
        f"🔥 **Best Call All-Time:** {best_call_x:.2f}x\n"
        f"   └ By {best_call_caller} (`{short_best_ca}`)"
    )
    
    await status_message.edit_text(text, parse_mode='Markdown')

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please provide a CA. Example: `/stats <CA>`", parse_mode='Markdown')
        return

    ca = context.args[0]
    call = calls_collection.find_one({"ca": ca, "chat_id": update.message.chat_id})

    if not call:
        await update.message.reply_text("I don't have that CA tracked in this group's database yet!")
        return

    batch_data = get_dexscreener_batch([ca])
    current_mcap = batch_data.get(ca.lower())
    
    if current_mcap:
        ath = max(call['ath_mcap'], current_mcap)
        calls_collection.update_one({"_id": call['_id']}, {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}})
        
        current_x = current_mcap / call['initial_mcap']
        ath_x = ath / call['initial_mcap']

        text = (
            f"📊 **Stats for Token**\n`{ca}`\n\n"
            f"👤 Called by: {call['caller_name']}\n"
            f"🟢 Called at: ${call['initial_mcap']:,.2f}\n"
            f"🔥 ATH: ${ath:,.2f} (**{ath_x:.2f}x**)\n"
            f"💰 Current: ${current_mcap:,.2f} (**{current_x:.2f}x**)"
        )
        await update.message.reply_text(text, parse_mode='Markdown')
    else:
        await update.message.reply_text("Failed to fetch current data from DexScreener.")

def main():
    app = Application.builder().token(TOKEN).build()
    
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_ca))
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    app.add_handler(CommandHandler("stats", stats))
    app.add_handler(CommandHandler("togglealerts", toggle_alerts))
    app.add_handler(CommandHandler("caller", caller_profile)) 
    app.add_handler(CommandHandler("groupstats", group_stats)) # New Command!
    
    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern='^lb_'))
    
    print("YabaiRankBot is running successfully with performance tracking!")
    app.run_polling()

if __name__ == "__main__":
    main()