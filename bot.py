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
    """Turns the 'New Call Tracked' notification on or off per group."""
    chat_id = update.message.chat_id
    setting = settings_collection.find_one({"chat_id": chat_id})
    
    if not setting or setting.get("alerts", True) == True:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.message.reply_text("🔕 **Alerts OFF**: I will track CAs silently here.", parse_mode='Markdown')
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.message.reply_text("🔔 **Alerts ON**: I will announce every new tracked CA here.", parse_mode='Markdown')

async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listens to group messages, detects CAs, and saves the initial call with Chat ID and Timestamp."""
    message = update.message.text
    user = update.message.from_user
    chat_id = update.message.chat_id
    
    found_cas = set(re.findall(CA_REGEX, message))
    
    for ca in found_cas:
        # ISOLATION: Check if this CA was already called IN THIS SPECIFIC CHAT
        existing_call = calls_collection.find_one({"ca": ca, "chat_id": chat_id})
        if existing_call:
            continue 
        
        batch_data = get_dexscreener_batch([ca])
        mcap = batch_data.get(ca.lower())
        
        if mcap and mcap > 0:
            call_data = {
                "ca": ca,
                "chat_id": chat_id, # Isolates data per group
                "caller_id": user.id,
                "caller_name": user.first_name or "Unknown",
                "caller_username": user.username, # Saved for easier searching
                "initial_mcap": mcap,
                "ath_mcap": mcap,
                "current_mcap": mcap,
                "timestamp": datetime.now(timezone.utc) # Saves exact time of call
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
    """Generates the leaderboard, isolated by group and filtered by time if requested."""
    chat_id = update.message.chat_id
    query = {"chat_id": chat_id} # ISOLATION: Only fetch this group's calls
    time_text = "All Time"

    # TIMEFRAME PARSING: Handle things like /leaderboard 10d or /leaderboard 24h
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
            pass # If they type something invalid, default to all-time

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
            user_stats[caller] = []
        user_stats[caller].append(multiplier)

    leaderboard_data = []
    for user, mults in user_stats.items():
        leaderboard_data.append({
            'name': user, 
            'avg': sum(mults) / len(mults), 
            'best': max(mults), 
            'total_calls': len(mults)
        })

    leaderboard_data.sort(key=lambda x: x['avg'], reverse=True)
    context.chat_data['leaderboard_data'] = leaderboard_data
    context.chat_data['leaderboard_time_text'] = time_text
    
    await render_leaderboard_page(status_message, context, page=0)

async def render_leaderboard_page(message_obj, context, page=0):
    """Draws the leaderboard page."""
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
                 f"   📈 Avg: {user['avg']:.2f}x | 🔥 Best: {user['best']:.2f}x\n")

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
    """Pulls the latest plays and stats for a specific user in this group."""
    if not context.args:
        await update.message.reply_text("Please provide a name. Example: `/caller John`", parse_mode='Markdown')
        return

    # Join args in case the name has a space, and remove the @ symbol if they tagged them
    target = " ".join(context.args).replace("@", "")
    chat_id = update.message.chat_id
    
    # Search for this user in this specific group (case-insensitive)
    query = {
        "chat_id": chat_id,
        "$or": [
            {"caller_name": {"$regex": f"^{target}$", "$options": "i"}},
            {"caller_username": {"$regex": f"^{target}$", "$options": "i"}}
        ]
    }
    
    # Get their 5 most recent calls
    user_calls = list(calls_collection.find(query).sort("timestamp", -1).limit(5))
    
    if not user_calls:
        await update.message.reply_text(f"I couldn't find any calls for '{target}' in this group.")
        return

    # Update latest prices for these specific 5 calls
    ca_list = [c['ca'] for c in user_calls]
    batch_data = get_dexscreener_batch(ca_list)
    
    actual_name = user_calls[0]['caller_name']
    text = f"👤 **Recent Plays for {actual_name}** 👤\n\n"
    
    for call in user_calls:
        ca = call['ca']
        current_mcap = batch_data.get(ca.lower(), call['current_mcap'])
        ath = max(call['ath_mcap'], current_mcap)
        
        # Silently update DB
        calls_collection.update_one({"_id": call['_id']}, {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}})
        
        initial = call['initial_mcap']
        call_date = call.get('timestamp', datetime.now(timezone.utc)).strftime('%Y-%m-%d')
        
        # Format the CA to be short so the message looks clean (e.g., 0x1234...abcd)
        short_ca = f"{ca[:6]}...{ca[-4:]}"
        
        text += (f"🪙 `{short_ca}` (Called on {call_date})\n"
                 f"🟢 Entry: ${initial:,.0f}\n"
                 f"🔥 ATH: **{(ath/initial):.2f}x** | 💰 Now: **{(current_mcap/initial):.2f}x**\n\n")

    await update.message.reply_text(text, parse_mode='Markdown')

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please provide a CA. Example: `/stats <CA>`", parse_mode='Markdown')
        return

    ca = context.args[0]
    # ISOLATION: Check stats for how it was called in THIS group
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
    app.add_handler(CommandHandler("caller", caller_profile)) # The new specific caller profile command
    
    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern='^lb_'))
    
    print("YabaiRankBot is running successfully with isolated groups and timeframes!")
    app.run_polling()

if __name__ == "__main__":
    main()