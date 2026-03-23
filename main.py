from fastapi import FastAPI
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
DOWNLOAD_BASE = os.getenv("DOWNLOAD_BASE", "/downloads")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))  # Telegram limit


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


def get_save_path(platform: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join(DOWNLOAD_BASE, platform, date_str)
    Path(folder).mkdir(parents=True, exist_ok=True)
    return folder


@app.post("/download", response_model=DownloadResponse)
async def download_video(req: DownloadRequest):
    logger.info(f"Download request: {req.url} | platform={req.platform} | mode={req.deliveryMode}")

    save_folder = get_save_path(req.platform)
    output_template = os.path.join(save_folder, "%(title)s.%(ext)s")

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

    # === PLATFORM LOGIC ===
    if req.platform == "twitter":
        cmd += [
            "-f", "bv*+ba/b",
            "--extractor-args", "twitter:api=graphql"
        ]

    elif req.platform == "youtube":
        cmd += [
            # ✅ Flexible format selection (fixes Shorts issue)
            "-f", "bv*+ba/b",
            # ✅ Better compatibility for Shorts & restricted videos
            "--extractor-args", "youtube:player_client=android,web"
        ]

    else:
        # fallback for unknown platforms
        cmd += ["-f", "bv*+ba/b"]

    cmd.append(req.url)

    try:
        result = subprocess.run(
            cmd,
            capture_output=True,
            text=True,
            timeout=120
        )

        if result.returncode != 0:
            error_msg = result.stderr.strip() or "Unknown yt-dlp error"
            logger.error(f"yt-dlp failed: {error_msg}")

            return DownloadResponse(
                success=False,
                error=error_msg,
                chatId=req.chatId,
                url=req.url,
                platform=req.platform,
                deliveryMode=req.deliveryMode
            )

        # Extract file path
        stdout_lines = result.stdout.strip().split("\n")
        file_path = stdout_lines[-1] if stdout_lines else None

        if not file_path or not os.path.exists(file_path):
            logger.error("File not found after download")
            return DownloadResponse(
                success=False,
                error="File not found after download",
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

    except subprocess.TimeoutExpired:
        logger.error("Download timed out")
        return DownloadResponse(
            success=False,
            error="Download timed out (120s limit exceeded)",
            chatId=req.chatId,
            url=req.url,
            platform=req.platform,
            deliveryMode=req.deliveryMode
        )

    except Exception as e:
        logger.exception("Unexpected error")
        return DownloadResponse(
            success=False,
            error=str(e),
            chatId=req.chatId,
            url=req.url,
            platform=req.platform,
            deliveryMode=req.deliveryMode
        )


@app.get("/")
async def root():
    return {"message": "yt-dlp API is running"}


@app.get("/health")
async def health():
    return {"status": "ok", "service": "yt-dlp-api"}