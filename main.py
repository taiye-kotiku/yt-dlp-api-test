from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, validator
from typing import Union
import subprocess
import os
import logging
from datetime import datetime
from pathlib import Path

app = FastAPI(title="yt-dlp API for n8n")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_BASE = os.getenv("DOWNLOAD_BASE", "/downloads")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))

COOKIES_PATH = os.path.join(BASE_DIR, "cookies.txt")


# === MODELS ===
class DownloadRequest(BaseModel):
    url: str
    platform: str
    chatId: Union[str, int]
    deliveryMode: str

    @validator('chatId', pre=True)
    def coerce_chat_id(cls, v):
        return str(v)


class DownloadResponse(BaseModel):
    success: bool
    filePath: str = None
    savedPath: str = None
    fileName: str = None
    fileSize: int = None
    error: str = None
    deliveryMode: str = None
    chatId: str = None
    url: str = None
    platform: str = None


# === HELPERS ===
def get_save_path(platform: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join(DOWNLOAD_BASE, platform, date_str)
    Path(folder).mkdir(parents=True, exist_ok=True)
    return folder


def normalize_youtube_url(url: str) -> str:
    if "youtube.com/shorts/" in url:
        video_id = url.split("/shorts/")[1].split("?")[0]
        return f"https://www.youtube.com/watch?v={video_id}"
    return url


def run_ytdlp(url: str, platform: str, output_template: str) -> tuple:
    """Shared yt-dlp runner. Returns (file_path, error)"""
    url = normalize_youtube_url(url)

    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--merge-output-format", "mp4",
        "--max-filesize", f"{MAX_FILE_SIZE_MB}M",
        "--output", output_template,
        "--print", "after_move:filepath",
        "--no-warnings",
        "--restrict-filenames",
        "--user-agent", "Mozilla/5.0 (Windows NT 10.0; Win64; x64)",
        "--add-header", "Accept-Language:en-US,en;q=0.9",
    ]

    if platform == "youtube":
        logger.info(f"Using cookies: {COOKIES_PATH} | Exists: {os.path.exists(COOKIES_PATH)}")
        cmd += [
            "-f", "bv*+ba/b",
            "--cookies", COOKIES_PATH,
            "--extractor-args", "youtube:player_client=android,web"
        ]
    elif platform == "twitter":
        cmd += [
            "-f", "bv*+ba/b",
            "--extractor-args", "twitter:api=graphql"
        ]
    else:
        cmd += ["-f", "bv*+ba/b"]

    cmd.append(url)

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if result.returncode != 0:
            error_msg = result.stderr.strip() or "Unknown yt-dlp error"
            logger.error(f"yt-dlp failed: {error_msg}")
            return None, error_msg

        stdout_lines = result.stdout.strip().split("\n")
        file_path = stdout_lines[-1] if stdout_lines else None

        if not file_path or not os.path.exists(file_path):
            return None, "File not found after download"

        return file_path, None

    except subprocess.TimeoutExpired:
        return None, "Download timed out (120s limit exceeded)"
    except Exception as e:
        logger.exception("Unexpected error")
        return None, str(e)


# === ROUTES ===

@app.post("/download", response_model=DownloadResponse)
async def download_video(req: DownloadRequest):
    """Original endpoint — returns JSON with file path"""
    logger.info(f"Download request: {req.url} | platform={req.platform} | mode={req.deliveryMode}")

    save_folder = get_save_path(req.platform)
    output_template = os.path.join(save_folder, "%(title)s.%(ext)s")

    file_path, error = run_ytdlp(req.url, req.platform, output_template)

    if error:
        return DownloadResponse(
            success=False,
            error=error,
            chatId=req.chatId,
            url=req.url,
            platform=req.platform,
            deliveryMode=req.deliveryMode
        )

    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)
    logger.info(f"Download success: {file_path} ({file_size} bytes)")

    return DownloadResponse(
        success=True,
        filePath=file_path,
        savedPath=file_path,
        fileName=file_name,
        fileSize=file_size,
        deliveryMode=req.deliveryMode,
        chatId=req.chatId,
        url=req.url,
        platform=req.platform
    )


@app.post("/download-stream")
async def download_stream(req: DownloadRequest):
    """New endpoint — downloads video and streams binary back to n8n for Telegram delivery"""
    logger.info(f"Stream request: {req.url} | platform={req.platform}")

    save_folder = get_save_path(req.platform)
    output_template = os.path.join(save_folder, "%(title)s.%(ext)s")

    file_path, error = run_ytdlp(req.url, req.platform, output_template)

    if error:
        return JSONResponse(status_code=400, content={"success": False, "error": error})

    file_name = os.path.basename(file_path)
    logger.info(f"Streaming: {file_path}")

    return FileResponse(
        path=file_path,
        media_type="video/mp4",
        filename=file_name,
        headers={
            "X-File-Name": file_name,
            "X-Platform": req.platform,
            "X-Chat-Id": str(req.chatId),
            "X-Delivery-Mode": req.deliveryMode,
        }
    )


@app.get("/")
async def root():
    return {"message": "yt-dlp API is running"}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "yt-dlp-api"}