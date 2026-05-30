import os
import math
import logging
import asyncio
from urllib.parse import quote
from collections import OrderedDict
from contextlib import asynccontextmanager
from fastapi import FastAPI, Request, HTTPException, Header, Response
from fastapi.responses import HTMLResponse, StreamingResponse
from fastapi.templating import Jinja2Templates
from fastapi.middleware.cors import CORSMiddleware
from pyrogram import Client, filters
from pyrogram.handlers import MessageHandler
from pyrogram.errors import FloodWait, FileReferenceExpired
import uvicorn

# --- LOGGING ---
logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
logger = logging.getLogger(__name__)

# --- CONFIG ---
API_ID = int(os.environ.get("API_ID", "0"))
API_HASH = os.environ.get("API_HASH", "")
BOT_TOKEN = os.environ.get("BOT_TOKEN", "")
DOMAIN = os.environ.get("DOMAIN", "https://yourdomain.com")
WORKER_CHANNEL = int(os.environ.get("WORKER_CHANNEL", "0"))
WORKER_TOKENS = os.environ.get("WORKER_TOKENS", "")

BASE_DIR = os.path.dirname(os.path.abspath(__file__))
os.makedirs(os.path.join(BASE_DIR, "sessions"), exist_ok=True)

bot = None
workers = []
worker_index = 0

# --- SMART SLIDING WINDOW CACHE ---
class ChunkCache:
    def __init__(self, max_chunks=256):
        self.cache = OrderedDict()
        self.max_chunks = max_chunks
        self.lock = asyncio.Lock()

    async def get(self, key):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
                return self.cache[key]
            return None

    async def set(self, key, value):
        async with self.lock:
            if key in self.cache:
                self.cache.move_to_end(key)
            else:
                self.cache[key] = value
                if len(self.cache) > self.max_chunks:
                    self.cache.popitem(last=False)

global_chunk_cache = ChunkCache()
metadata_cache = {}

# --- CORE STREAMING LOGIC WITH BACKPRESSURE ---
def get_worker():
    global worker_index
    if not workers:
        return bot
    worker = workers[worker_index % len(workers)]
    worker_index += 1
    return worker

async def fetch_metadata(file_id: int):
    if file_id in metadata_cache:
        return metadata_cache[file_id]

    msg = await bot.get_messages(WORKER_CHANNEL, file_id)
    if not msg or not getattr(msg, "media", None):
        raise HTTPException(status_code=404, detail="File not found.")
        
    media = msg.video or msg.document
    meta = {
        "file_size": media.file_size,
        "file_name": getattr(media, "file_name", f"{file_id}.mp4"),
        "mime_type": media.mime_type or "video/mp4",
        "msg_obj": msg,
        "file_id_str": media.file_id  # Extract the raw string with the baked-in token
    }
    metadata_cache[file_id] = meta
    return meta

async def managed_stream_generator(client, file_id: int, start_byte, end_byte, request: Request):
    chunk_size = 1024 * 1024
    start_chunk = start_byte // chunk_size
    end_chunk = end_byte // chunk_size
    
    # Grab the initial target file ID string from cache
    meta = await fetch_metadata(file_id)
    target_file_id_str = meta["file_id_str"]

    try:
        for current_chunk_idx in range(start_chunk, end_chunk + 1):
            if await request.is_disconnected():
                logger.info(f"Client disconnected. Aborting stream for file {file_id}.")
                raise asyncio.CancelledError("Client disconnected")

            cache_key = f"{file_id}_{current_chunk_idx}"
            chunk_data = await global_chunk_cache.get(cache_key)

            if not chunk_data:
                retries = 3
                refresh_attempts = 0
                while retries > 0:
                    chunk_data = b""
                    try:
                        # PASS THE RAW STRING, NOT THE MESSAGE OBJECT
                        async for part in client.stream_media(target_file_id_str, limit=1, offset=current_chunk_idx):
                            chunk_data += part
                        
                        if chunk_data:
                            await global_chunk_cache.set(cache_key, chunk_data)
                            break
                    except FloodWait as fw:
                        logger.warning(f"Rate limited. Waiting {fw.value}s...")
                        await asyncio.sleep(fw.value)
                        retries -= 1
                    except FileReferenceExpired:
                        if refresh_attempts >= 2:
                            logger.error(f"Circuit breaker tripped: File ref for {file_id} constantly expiring.")
                            break
                        refresh_attempts += 1
                        logger.warning(f"File reference expired for {file_id}. Main Bot fetching fresh token string...")
                        
                        # Main bot fetches the fresh message to guarantee a valid token
                        fresh_msg = await bot.get_messages(WORKER_CHANNEL, file_id)
                        fresh_media = getattr(fresh_msg, "video", None) or getattr(fresh_msg, "document", None)
                        
                        if fresh_media:
                            # Update the target string with the newly baked token
                            target_file_id_str = fresh_media.file_id 
                            # Update the global cache
                            if file_id in metadata_cache:
                                metadata_cache[file_id]["msg_obj"] = fresh_msg
                                metadata_cache[file_id]["file_id_str"] = target_file_id_str
                            continue 
                        else:
                            logger.error(f"Failed to fetch fresh message for {file_id}. Message deleted?")
                            break
                    except Exception as e:
                        logger.error(f"Error pulling chunk {current_chunk_idx}: {e}")
                        break

            if not chunk_data:
                logger.error(f"Failed to fetch chunk {current_chunk_idx} from Telegram.")
                raise RuntimeError("Missing chunk data")

            chunk_start_byte = current_chunk_idx * chunk_size
            slice_start = max(0, start_byte - chunk_start_byte)
            slice_end = min(len(chunk_data), end_byte - chunk_start_byte + 1)

            yield chunk_data[slice_start:slice_end]

    except asyncio.CancelledError:
        logger.info("Stream playback task cancelled by client request.")
        raise
    except Exception as e:
        logger.error(f"Streaming loop failure: {e}")
        raise  

# --- FASTAPI SETUP & LIFESPAN ---
@asynccontextmanager
async def lifespan(app: FastAPI):
    global bot
    logger.info("Initializing stream node master controller...")
    bot = Client("stream_bot", api_id=API_ID, api_hash=API_HASH, bot_token=BOT_TOKEN, workdir="sessions")
    await bot.start()
    
    tokens = [t.strip() for t in WORKER_TOKENS.split(",") if t.strip()]
    for token in tokens:
        bot_id = token.split(":")[0]
        w_client = Client(f"worker_{bot_id}", api_id=API_ID, api_hash=API_HASH, bot_token=token, workdir="sessions")
        await w_client.start()
        workers.append(w_client)

    logger.info(f"Cluster Online: {len(workers)} edge worker processes handling I/O pools.")
    yield 
    for w in workers:
        await w.stop()
    await bot.stop()

app = FastAPI(lifespan=lifespan)
app.add_middleware(CORSMiddleware, allow_origins=["*"], allow_credentials=True, allow_methods=["*"], allow_headers=["*"])
templates = Jinja2Templates(directory=os.path.join(BASE_DIR, "templates"))

# --- PUBLIC ROUTES ---
@app.get("/player/{file_id}", response_class=HTMLResponse)
async def video_player(request: Request, file_id: int):
    return templates.TemplateResponse(
        request=request, name="player.html", context={"video_url": f"{DOMAIN}/file/{file_id}"}
    )

@app.head("/file/{file_id}")
async def head_file(file_id: int):
    meta = await fetch_metadata(file_id)
    return Response(
        headers={
            "Accept-Ranges": "bytes",
            "Content-Length": str(meta["file_size"]),
            "Content-Type": meta["mime_type"],
            "Content-Disposition": f"inline; filename*=utf-8''{quote(meta['file_name'])}",
        }
    )

@app.get("/file/{file_id}")
async def stream_file(request: Request, file_id: int, range: str = Header(None)):
    meta = await fetch_metadata(file_id)
    file_size = meta["file_size"]
    
    start, end = 0, file_size - 1
    status_code = 200

    if range:
        status_code = 206
        range_header = range.replace("bytes=", "").split("-")
        start = int(range_header[0]) if range_header[0] else 0
        end = int(range_header[1]) if len(range_header) > 1 and range_header[1] else file_size - 1

    content_length = (end - start) + 1

    headers = {
        "Content-Range": f"bytes {start}-{end}/{file_size}",
        "Accept-Ranges": "bytes",
        "Content-Length": str(content_length),
        "Content-Type": meta["mime_type"],
        "Content-Disposition": f"inline; filename*=utf-8''{quote(meta['file_name'])}",
    }

    return StreamingResponse(
        managed_stream_generator(get_worker(), file_id, start, end, request),
        status_code=status_code,
        headers=headers
    )

if __name__ == "__main__":
    uvicorn.run("main:app", host="0.0.0.0", port=8000)
