import os
import re
import requests
import certifi
from pymongo import MongoClient
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

# --- CONFIGURATION (Securely using Environment Variables) ---
TOKEN = os.getenv("TELEGRAM_TOKEN")
MONGO_URI = os.getenv("MONGO_URI")

# Safety check to ensure variables are loaded
if not TOKEN or not MONGO_URI:
    raise ValueError("Missing TELEGRAM_TOKEN or MONGO_URI environment variables! Please set them in Render.")

# Connect to MongoDB with certifi to prevent SSL handshake errors
client = MongoClient(MONGO_URI, tlsCAFile=certifi.where())
db = client['yabai_crypto_bot']
calls_collection = db['token_calls']

# Regex to detect EVM (0x...) and Solana/Pump.fun CAs (Base58, 32-44 chars)
CA_REGEX = r'\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b'

def get_dexscreener_data(ca):
    """Fetches token data from DexScreener API."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
        # Added a 10-second timeout so the bot doesn't freeze if DexScreener is slow
        response = requests.get(url, timeout=10).json()
        if response.get('pairs'):
            return response['pairs'][0].get('fdv', 0)
    except Exception as e:
        print(f"Error fetching DexScreener data: {e}")
    return None

async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listens to group messages, detects CAs, and saves the initial call."""
    message = update.message.text
    user = update.message.from_user
    
    found_cas = set(re.findall(CA_REGEX, message))
    
    for ca in found_cas:
        existing_call = calls_collection.find_one({"ca": ca})
        if existing_call:
            continue 
        
        mcap = get_dexscreener_data(ca)
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
            await update.message.reply_text(
                f"🎯 **New Call Tracked!**\n\n"
                f"🪙 CA: `{ca}`\n"
                f"👤 Caller: {user.first_name}\n"
                f"💰 Called at MCAP: ${mcap:,.2f}",
                parse_mode='Markdown'
            )

async def leaderboard(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Updates active CAs and displays the leaderboard."""
    # Send a temporary message while we fetch data
    status_message = await update.message.reply_text("🔄 Updating latest prices from DexScreener...")
    
    all_calls = calls_collection.find()
    user_scores = {}

    for call in all_calls:
        ca = call['ca']
        current_mcap = get_dexscreener_data(ca)
        
        if current_mcap:
            ath = max(call['ath_mcap'], current_mcap)
            calls_collection.update_one(
                {"_id": call['_id']},
                {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}}
            )
            
            multiplier = ath / call['initial_mcap']
            caller = call['caller_name']
            
            if caller not in user_scores or multiplier > user_scores[caller]:
                user_scores[caller] = multiplier

    sorted_users = sorted(user_scores.items(), key=lambda x: x[1], reverse=True)
    
    if not sorted_users:
        await status_message.edit_text("No data yet! Post some CAs to get started.")
        return

    text = "🏆 **Yabai Callers Leaderboard** 🏆\n\n"
    for rank, (name, best_x) in enumerate(sorted_users, 1):
        text += f"{rank}. {name} - Best Call: **{best_x:.2f}x**\n"
        
    # Edit the "Updating..." message to show the final leaderboard
    await status_message.edit_text(text, parse_mode='Markdown')

async def stats(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Checks the stats of a specific CA passed by the user."""
    if not context.args:
        await update.message.reply_text("Please provide a CA. Example: `/stats <CA>`", parse_mode='Markdown')
        return

    ca = context.args[0]
    call = calls_collection.find_one({"ca": ca})

    if not call:
        await update.message.reply_text("I don't have that CA tracked in my database yet!")
        return

    current_mcap = get_dexscreener_data(ca)
    
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
    
    print("YabaiRankBot is running successfully!")
    app.run_polling()

if __name__ == "__main__":
    main()