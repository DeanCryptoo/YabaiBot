import os
import re
import requests
import certifi
import math
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
settings_collection = db['group_settings'] # New collection for settings

CA_REGEX = r'\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b'

def get_dexscreener_batch(cas_list):
    """Fetches up to 30 tokens at once from DexScreener to save API requests."""
    results = {}
    # Split the list into chunks of 30
    for i in range(0, len(cas_list), 30):
        chunk = cas_list[i:i+30]
        url = f"https://api.dexscreener.com/latest/dex/tokens/{','.join(chunk)}"
        try:
            response = requests.get(url, timeout=10).json()
            if response and response.get('pairs'):
                for pair in response['pairs']:
                    address = pair.get('baseToken', {}).get('address')
                    fdv = pair.get('fdv', 0)
                    # Use lowercase keys to avoid case-sensitivity issues
                    if address and fdv > 0:
                        addr_lower = address.lower()
                        # Only save the highest liquidity pair's FDV
                        if addr_lower not in results or fdv > results[addr_lower]:
                            results[addr_lower] = fdv
        except Exception as e:
            print(f"Batch fetch error: {e}")
    return results

async def toggle_alerts(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Turns the 'New Call Tracked' notification on or off."""
    chat_id = update.message.chat_id
    setting = settings_collection.find_one({"chat_id": chat_id})
    
    # If currently missing or True, toggle to False
    if not setting or setting.get("alerts", True) == True:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": False}}, upsert=True)
        await update.message.reply_text("🔕 **Alerts OFF**: I will now track CAs silently in the background.", parse_mode='Markdown')
    else:
        settings_collection.update_one({"chat_id": chat_id}, {"$set": {"alerts": True}}, upsert=True)
        await update.message.reply_text("🔔 **Alerts ON**: I will announce every new tracked CA in the chat.", parse_mode='Markdown')

async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listens to group messages, detects CAs, and saves the initial call."""
    message = update.message.text
    user = update.message.from_user
    chat_id = update.message.chat_id
    
    found_cas = set(re.findall(CA_REGEX, message))
    
    for ca in found_cas:
        existing_call = calls_collection.find_one({"ca": ca})
        if existing_call:
            continue 
        
        # We only need single requests for brand new tokens
        batch_data = get_dexscreener_batch([ca])
        mcap = batch_data.get(ca.lower())
        
        if mcap and mcap > 0:
            call_data = {
                "ca": ca,
                "caller_id": user.id,
                "caller_name": user.first_name or user.username,
                "initial_mcap": mcap,
                "ath_mcap": mcap,
                "current_mcap": mcap
            }
            calls_collection.insert_one(call_data)
            
            # Check settings to see if we should announce it
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
    """Compiles the leaderboard, calculates averages, and generates page 1."""
    status_message = await update.message.reply_text("🔄 Batch-fetching latest prices from DexScreener. Please wait...")
    
    all_calls = list(calls_collection.find())
    if not all_calls:
        await status_message.edit_text("No data yet! Post some CAs to get started.")
        return

    # 1. Gather all unique CAs and fetch updated prices in batches
    unique_cas = list(set([call['ca'] for call in all_calls]))
    latest_mcaps = get_dexscreener_batch(unique_cas)
    
    user_stats = {} # Format: { 'Name': [multiplier1, multiplier2] }

    # 2. Update DB and group data by caller
    for call in all_calls:
        ca = call['ca']
        ca_lower = ca.lower()
        
        current_mcap = latest_mcaps.get(ca_lower, call['current_mcap'])
        ath = max(call['ath_mcap'], current_mcap)
        
        # Save new ATH to database
        calls_collection.update_one(
            {"_id": call['_id']},
            {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}}
        )
        
        multiplier = ath / call['initial_mcap']
        caller = call['caller_name']
        
        if caller not in user_stats:
            user_stats[caller] = []
        user_stats[caller].append(multiplier)

    # 3. Calculate Average and Best for each user
    leaderboard_data = []
    for user, mults in user_stats.items():
        avg_x = sum(mults) / len(mults)
        best_x = max(mults)
        leaderboard_data.append({
            'name': user, 
            'avg': avg_x, 
            'best': best_x, 
            'total_calls': len(mults)
        })

    # Sort primarily by Average X
    leaderboard_data.sort(key=lambda x: x['avg'], reverse=True)
    
    # Save the data in context so the pagination buttons can read it
    context.chat_data['leaderboard_data'] = leaderboard_data
    
    # Render the first page
    await render_leaderboard_page(status_message, leaderboard_data, page=0)

async def render_leaderboard_page(message_obj, data, page=0):
    """Helper function to draw the leaderboard text and buttons."""
    items_per_page = 10
    total_pages = math.ceil(len(data) / items_per_page)
    
    start_idx = page * items_per_page
    end_idx = start_idx + items_per_page
    page_data = data[start_idx:end_idx]

    text = f"🏆 **Yabai Callers (Sorted by Avg X)** 🏆\n*Page {page + 1} of {total_pages}*\n\n"
    
    for idx, user in enumerate(page_data, start=start_idx + 1):
        text += (f"{idx}. **{user['name']}** ({user['total_calls']} calls)\n"
                 f"   📈 Avg: {user['avg']:.2f}x | 🔥 Best: {user['best']:.2f}x\n")

    # Create Pagination Buttons
    buttons = []
    if page > 0:
        buttons.append(InlineKeyboardButton("◀️ Prev", callback_data=f"lb_{page-1}"))
    if page < total_pages - 1:
        buttons.append(InlineKeyboardButton("Next ▶️", callback_data=f"lb_{page+1}"))
        
    reply_markup = InlineKeyboardMarkup([buttons]) if buttons else None

    # Try to edit the message. (Catches error if text is exactly the same)
    try:
        await message_obj.edit_text(text, reply_markup=reply_markup, parse_mode='Markdown')
    except Exception:
        pass

async def paginate_leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Handles button clicks for leaderboard pages."""
    query = update.callback_query
    await query.answer() # Tell Telegram we received the click
    
    # Parse the requested page number from callback_data (e.g., "lb_1")
    page = int(query.data.split('_')[1])
    data = context.chat_data.get('leaderboard_data')
    
    if data:
        await render_leaderboard_page(query.message, data, page)
    else:
        await query.message.edit_text("Leaderboard data expired. Please run /leaderboard again.")

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    if not context.args:
        await update.message.reply_text("Please provide a CA. Example: `/stats <CA>`", parse_mode='Markdown')
        return

    ca = context.args[0]
    call = calls_collection.find_one({"ca": ca})

    if not call:
        await update.message.reply_text("I don't have that CA tracked in my database yet!")
        return

    # Single lookup
    batch_data = get_dexscreener_batch([ca])
    current_mcap = batch_data.get(ca.lower())
    
    if current_mcap:
        ath = max(call['ath_mcap'], current_mcap)
        calls_collection.update_one(
            {"_id": call['_id']},
            {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}}
        )
        
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
    
    # Handler for the pagination buttons
    app.add_handler(CallbackQueryHandler(paginate_leaderboard, pattern='^lb_'))
    
    print("YabaiRankBot is running successfully!")
    app.run_polling()

if __name__ == "__main__":
    main()