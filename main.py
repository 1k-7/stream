import os
import math
import logging
from urllib.parse import quote
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Header
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
import uvicorn

# --- LOGGING SETUP ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(name)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIGURATION ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "https://yourdomain.com")
WORKER_CHANNEL = int(os.environ.get("WORKER_CHANNEL", "0"))
WORKER_TOKENS = os.environ.get("WORKER_TOKENS", "")

# Absolute path for templates
BASE_DIR = os.path.dirname(os.path.abspath(__file__))

# Ensure sessions directory exists so Docker mapping doesn't fail
os.makedirs(os.path.join(BASE_DIR, "sessions"), exist_ok=True)

# --- GLOBALS ---
bot = None
workers = []
worker_index = 0

# --- TELEGRAM BOT HANDLERS ---
async def start_cmd(client, message):
    await message.reply_text("👋 Hello! Send me a video, and I'll route it through the streaming cluster.")

async def handle_video(client, message):
    media = message.video or message.document
    if message.document and not message.document.mime_type.startswith("video/"):
        return

    status_msg = await message.reply_text("📥 *Indexing media into streaming cluster...*")
    
    try:
        # Forward the file to the shared worker channel
        forwarded_msg = await message.forward(WORKER_CHANNEL)
        
        stream_url = f"{DOMAIN}/player/{forwarded_msg.id}"
        download_url = f"{DOMAIN}/file/{forwarded_msg.id}"
        
        await status_msg.edit_text(
            f"🎬 *Stream Ready!*\n\n"
            f"🔗 *Stream Link:* {stream_url}\n"
            f"📥 *Download Link:* {download_url}\n\n"
            f"⚙️ _Powered by Distributed Worker Pool_",
            disable_web_page_preview=True
        )
    except Exception as e:
        logger.error(f"Media routing failed: {e}")
        await status_msg.edit_text(f"❌ Error routing media: {str(e)}")

# --- STREAMING CORE ---
def get_worker():
    global worker_index
    if not workers:
        return bot
    worker = workers[worker_index % len(workers)]
    worker_index += 1
    return worker

async def stream_generator(client, message_id, offset, limit):
    try:
        msg = await client.get_messages(WORKER_CHANNEL, message_id)
        if not msg or not (msg.video or msg.document):
            yield b""
            return

        async for chunk in client.stream_media(msg, limit=limit, offset=offset):
            yield chunk
    except Exception as e:
        logger.error(f"Stream generation error: {e}")
        yield b""

# --- FASTAPI LIFESPAN & ROUTES ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot
    logger.info("Starting Main Bot...")
    
    # Initialize Main Bot with persistent SQLite storage
    bot = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="sessions")
    
    bot.add_handler(MessageHandler(start_cmd, filters.command("start")))
    bot.add_handler(MessageHandler(handle_video, filters.video | filters.document))

    await bot.start()
    
    # Safe check to see if the bot has cached the channel peer without crashing the server if it hasn't
    try:
        await bot.get_chat(WORKER_CHANNEL)
    except Exception as e:
        logger.warning(f"Awaiting manual cache: Please send a regular text message to your WORKER_CHANNEL to cache the peer. ({e})")

    # Initialize Worker Bots with persistent SQLite storage
    tokens = [t.strip() for t in WORKER_TOKENS.split(",") if t.strip()]
    for token in tokens:
        try:
            bot_id = token.split(":")[0]
            w_client = Client(f"worker_{bot_id}", api_id=API_ID, api_hash=API_HASH, bot_token=token, workdir="sessions")
            await w_client.start()
            workers.append(w_client)
        except Exception as e:
            logger.error(f"Failed to start worker bot {token[:10]}... : {e}")

    logger.info(f"Successfully initialized {len(workers)} worker bots in the pool.")
    
    yield 
    
    logger.info("Shutting down cluster...")
    for w in workers:
        await w.stop()
    await bot.stop()

app = FastAPI(lifespan=lifespan)

# Bind Jinja2 to the absolute path
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

@app.get("/player/{file_id}", response_class=HTMLResponse)
async def video_player(request: Request, file_id: int):
    video_url = f"{DOMAIN}/file/{file_id}"
    return templates.TemplateResponse(
        request=request, 
        name="player.html", 
        context={"video_url": video_url}
    )

@app.get("/file/{file_id}")
async def stream_file(request: Request, file_id: int, range: str = Header(None)):
    try:
        msg = await bot.get_messages(WORKER_CHANNEL, file_id)
        if not msg or not msg.media:
            raise HTTPException(status_code=404, detail="File not found in worker channel.")
            
        media = msg.video or msg.document
        file_size = media.file_size
        file_name = getattr(media, "file_name", f"{file_id}.mp4")

        start = 0
        end = file_size - 1
        status_code = 200

        if range:
            status_code = 206
            range_header = range.replace("bytes=", "").split("-")
            start = int(range_header[0]) if range_header[0] else 0
            end = int(range_header[1]) if len(range_header) > 1 and range_header[1] else file_size - 1

        content_length = (end - start) + 1
        worker = get_worker()
        
        chunk_size = 1024 * 1024
        offset_chunks = math.floor(start / chunk_size)
        limit_chunks = math.ceil(content_length / chunk_size)

        # URL-Encode filename to securely pass non-Latin characters in HTTP headers
        encoded_name = quote(file_name)

        headers = {
            "Content-Range": f"bytes {start}-{end}/{file_size}",
            "Accept-Ranges": "bytes",
            "Content-Length": str(content_length),
            "Content-Type": media.mime_type or "video/mp4",
            "Content-Disposition": f"inline; filename*=utf-8''{encoded_name}",
        }

        return StreamingResponse(
            stream_generator(worker, file_id, offset=offset_chunks, limit=limit_chunks),
            status_code=status_code,
            headers=headers
        )

    except Exception as e:
        logger.error(f"Streaming endpoint error: {e}")
        raise HTTPException(status_code=500, detail="Internal server error while streaming.")

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000, log_level="info")
