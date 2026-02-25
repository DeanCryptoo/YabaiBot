import re
import requests
from pymongo import MongoClient
from telegram import Update
from telegram.ext import Application, MessageHandler, CommandHandler, filters, ContextTypes

# --- CONFIGURATION ---
TOKEN = "8668201692:AAH_dbN4O0hpryj8MWq0a-LA3712eD-z5QE"
MONGO_URI = "mongodb+srv://ReklessDean:Incripted11!@cluster0.zytbbgk.mongodb.net/?appName=Cluster0"

# Connect to MongoDB
client = MongoClient(MONGO_URI)
db = client['yabai_crypto_bot']
calls_collection = db['token_calls']

# Regex to detect EVM (0x...) and Solana/Pump.fun CAs (Base58, 32-44 chars)
CA_REGEX = r'\b(0x[a-fA-F0-9]{40}|[1-9A-HJ-NP-Za-km-z]{32,44})\b'

def get_dexscreener_data(ca):
    """Fetches token data from DexScreener API."""
    try:
        url = f"https://api.dexscreener.com/latest/dex/tokens/{ca}"
        response = requests.get(url).json()
        if response.get('pairs'):
            # Return the FDV (Market Cap) of the most liquid pair
            return response['pairs'][0].get('fdv', 0)
    except Exception as e:
        print(f"Error fetching DexScreener data: {e}")
    return None

async def track_ca(update: Update, context: ContextTypes.DEFAULT_TYPE):
    """Listens to group messages, detects CAs, and saves the initial call."""
    message = update.message.text
    user = update.message.from_user
    
    # Find all CAs in the message
    found_cas = set(re.findall(CA_REGEX, message))
    
    for ca in found_cas:
        # Check if this CA was already called by someone else
        existing_call = calls_collection.find_one({"ca": ca})
        if existing_call:
            continue # Already tracked, skip to give original caller credit
        
        # Fetch current MCAP
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
    await update.message.reply_text("🔄 Updating latest prices from DexScreener. Please wait...")
    
    # Update all tracked tokens to find ATH and current multipliers
    all_calls = calls_collection.find()
    user_scores = {}

    for call in all_calls:
        ca = call['ca']
        current_mcap = get_dexscreener_data(ca)
        
        if current_mcap:
            # Update ATH if the current is higher
            ath = max(call['ath_mcap'], current_mcap)
            
            calls_collection.update_one(
                {"_id": call['_id']},
                {"$set": {"current_mcap": current_mcap, "ath_mcap": ath}}
            )
            
            # Calculate the X multiplier (ATH / Initial)
            multiplier = ath / call['initial_mcap']
            caller = call['caller_name']
            
            # Record the highest multiplier per user
            if caller not in user_scores or multiplier > user_scores[caller]:
                user_scores[caller] = multiplier

    # Sort users by best multipliers
    sorted_users = sorted(user_scores.items(), key=lambda x: x[1], reverse=True)
    
    if not sorted_users:
        await update.message.reply_text("No data yet! Post some CAs.")
        return

    # Build the Leaderboard message
    text = "🏆 **Yabai Callers Leaderboard** 🏆\n\n"
    for rank, (name, best_x) in enumerate(sorted_users, 1):
        text += f"{rank}. {name} - Best Call: **{best_x:.2f}x**\n"
        
    await update.message.reply_text(text, parse_mode='Markdown')

def main():
    app = Application.builder().token(TOKEN).build()
    
    # Listen to all text messages to scrape CAs
    app.add_handler(MessageHandler(filters.TEXT & ~filters.COMMAND, track_ca))
    # Command to show leaderboard
    app.add_handler(CommandHandler("leaderboard", leaderboard))
    
    print("YabaiRankBot is running...")
    app.run_polling()

if __name__ == "__main__":
    main()