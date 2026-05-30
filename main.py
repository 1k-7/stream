import os
import asyncio
import time
import logging
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException
from fastapi.responses import HTMLResponse, FileResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client, filters
from pyrogram.enums import ParseMode
from pyrogram.handlers import MessageHandler
import uvicorn

# --- LOGGING SETUP ---
logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s - %(name)s - %(levelname)s - %(message)s"
)
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "https://yourdomain.com")
DOWNLOAD_DIR = "./downloads"
CLEANUP_INTERVAL = 3600
FILE_MAX_AGE = 86400

os.makedirs(DOWNLOAD_DIR, exist_ok=True)

# Global bot instance (initialized later in the active loop)
bot = None

# --- BACKGROUND TASKS ---
async def auto_cleanup_task():
    while True:
        now = time.time()
        for filename in os.listdir(DOWNLOAD_DIR):
            file_path = os.path.join(DOWNLOAD_DIR, filename)
            if os.path.isfile(file_path):
                if os.stat(file_path).st_mtime < (now - FILE_MAX_AGE):
                    try:
                        os.remove(file_path)
                        logger.info(f"Cleaned up expired file: {filename}")
                    except Exception as e:
                        logger.error(f"Error deleting file {filename}: {e}")
        await asyncio.sleep(CLEANUP_INTERVAL)

# --- TELEGRAM BOT HANDLERS ---
async def start_cmd(client, message):
    logger.info(f"Received /start command from user ID: {message.from_user.id}")
    await message.reply_text(
        "👋 Hello! Send or forward any video to me, and I will generate a high-speed streaming link for you."
    )

async def handle_video(client, message):
    media = message.video or message.document
    if message.document and not message.document.mime_type.startswith("video/"):
        return

    logger.info(f"Received media file from user ID: {message.from_user.id}")
    status_msg = await message.reply_text("📥 *Processing media... Downloading to high-speed stream server.*")
    
    file_name = f"{message.id}.mp4"
    file_path = os.path.join(DOWNLOAD_DIR, file_name)
    
    try:
        await message.download(file_name=file_path)
        logger.info(f"Successfully downloaded: {file_name}")
        
        stream_url = f"{DOMAIN}/player/{message.id}"
        download_url = f"{DOMAIN}/file/{message.id}"
        
        await status_msg.edit_text(
            f"🎬 *Your Video is Ready!*\n\n"
            f"🔗 *Stream Link:* {stream_url}\n"
            f"📥 *Direct Download Link:* {download_url}\n\n"
            f"⚠️ _Note: Links expire automatically after 24 hours._",
            disable_web_page_preview=True,
            parse_mode=ParseMode.MARKDOWN
        )
    except Exception as e:
        logger.error(f"Media processing failed: {e}")
        await status_msg.edit_text(f"❌ An error occurred during processing: {str(e)}")

# --- LIFESPAN MANAGER ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot
    logger.info("Initializing Pyrogram Client within Uvicorn loop...")
    
    # Initialize inside the active loop
    bot = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN)
    
    # Manually bind handlers to the active client
    bot.add_handler(MessageHandler(start_cmd, filters.command("start")))
    bot.add_handler(MessageHandler(handle_video, filters.video | filters.document))

    await bot.start()
    logger.info("Pyrogram successfully connected and dispatcher is running.")
    
    cleanup_task = asyncio.create_task(auto_cleanup_task())
    
    yield 
    
    logger.info("Stopping Pyrogram Client...")
    cleanup_task.cancel()
    await bot.stop()

# Initialize FastAPI
app = FastAPI(lifespan=lifespan)
templates = Jinja2Templates(directory="templates")

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
    
    return FileResponse(file_path, media_type="video/mp4", filename=f"{file_id}.mp4")

# --- APP RUNNER ---
if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
