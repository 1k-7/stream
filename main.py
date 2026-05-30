import logging
logging.basicConfig(level=logging.INFO)
import os
import asyncio
import time
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client, filters
import uvicorn

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "YOUR_API_ID"))
API_HASH = os.environ.get("API_HASH", "YOUR_API_HASH")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "YOUR_BOT_TOKEN")
DOMAIN = os.environ.get("DOMAIN", "https://yourdomain.com")  # Include https://
DOWNLOAD_DIR = "./downloads"
CLEANUP_INTERVAL = 3600  # Check files every hour
FILE_MAX_AGE = 86400     # Delete files older than 24 hours (in seconds)

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Initialize FastAPI & Templates
app = FastAPI()
templates = Jinja2Templates(directory="templates")

# Initialize Pyrogram Bot Client
bot = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)

# --- TELEGRAM BOT HANDLERS ---
@bot.on_message(filters.command("start"))
async def start_cmd(client, message):
    await message.reply_text("👋 Hello! Send or forward any video to me, and I will generate a high-speed streaming link for you.")

@bot.on_message(filters.video | filters.document)
async def handle_video(client, message):
    # Verify if document is an actual video format
    media = message.video or message.document
    if message.document and not message.document.mime_type.startswith("video/"):
        return

    status_msg = await message.reply_text("📥 *Processing media... Downloading to high-speed stream server.*")
    
    # Secure clean filename using message ID
    file_name = f"{message.id}.mp4"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        # Download the file locally
        await message.download(file_name=file_path)
        
        # Structure URLs
        stream_url = f"{DOMAIN}/player/{message.id}"
        download_url = f"{DOMAIN}/file/{message.id}"
        
        await status_msg.edit_text(
            f"🎬 *Your Video is Ready!*\n\n"
            f"🔗 *Stream Link:* {stream_url}\n"
            f"📥 *Direct Download Link:* {download_url}\n\n"
            f"⚠️ _Note: Links expire automatically after 24 hours._",
            disable_web_page_preview=True,
            parse_mode=get_parse_mode_enum() # Pyrogram default uses markdown
        )
    except Exception as e:
        await status_msg.edit_text(f"❌ An error occurred during processing: {str(e)}")

def get_parse_mode_enum():
    from pyrogram.enums import ParseMode
    return ParseMode.MARKDOWN

# --- WEB SERVER ROUTES ---
@app.get("/player/{file_id}", response_class=HTMLResponse)
async def video_player(request: Request, file_id: str):
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.mp4")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File has expired or does not exist.")
    
    video_url = f"{DOMAIN}/file/{file_id}"
    return templates.TemplateResponse("player.html", {"request": request, "video_url": video_url})

@app.get("/file/{file_id}")
async def get_file(file_id: str):
    file_path = os.path.join(DOWNLOAD_DIR, f"{file_id}.mp4")
    if not os.path.exists(file_path):
        raise HTTPException(status_code=404, detail="File not found.")
    
    # FileResponse natively handles HTTP Range Requests out of the box
    return FileResponse(file_path, media_type="video/mp4", filename=f"{file_id}.mp4")

# --- BACKGROUND CLEANUP TASK ---
async def auto_cleanup_task():
    while True:
        now = time.time()
        for filename in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.isfile(file_path):
                if os.stat(file_path).st_mtime < (now - FILE_MAX_AGE):
                    try:
                        os.remove(file_path)
                        print(f"Cleaned up expired file: {filename}")
                    except Exception as e:
                        print(f"Error deleting file {filename}: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL)

# --- APP RUNNER ---
async def main():
    # Start Telegram Bot Client
    await bot.start()
    print("Bot started successfully!")
    
    # Fire up background storage cleaner
    asyncio.create_task(auto_cleanup_task())
    
    # Start Web Server
    config = uvicorn.Config(app, host="0.0.0.0", port=8000, log_level="info")
    server = uvicorn.Server(config)
    await server.serve()

if __name__ == "__main__":
    asyncio.run(main())
