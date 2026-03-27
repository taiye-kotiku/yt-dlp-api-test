from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, validator
from typing import Union, Optional, List
import subprocess
import os
import base64
import tempfile
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
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024


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
    filePath: Optional[str] = None
    savedPath: Optional[str] = None
    fileName: Optional[str] = None
    fileSize: Optional[int] = None
    error: Optional[str] = None
    errorCode: Optional[str] = None
    deliveryMode: Optional[str] = None
    chatId: Optional[str] = None
    url: Optional[str] = None
    platform: Optional[str] = None
    cookieIndex: Optional[int] = None


# === COOKIE MANAGEMENT ===

COOKIE_ENV_MAP = {
    "twitter": [
        "TWITTER_COOKIES_1_B64",
        "TWITTER_COOKIES_2_B64",
        "TWITTER_COOKIES_3_B64",
    ],
    "youtube": [
        "YOUTUBE_COOKIES_1_B64",
        "YOUTUBE_COOKIES_2_B64",
        "YOUTUBE_COOKIES_3_B64",
    ],
    "facebook": [
        "FACEBOOK_COOKIES_1_B64",
        "FACEBOOK_COOKIES_2_B64",
        "FACEBOOK_COOKIES_3_B64",
    ],
    "instagram": [
        "INSTAGRAM_COOKIES_1_B64",
        "INSTAGRAM_COOKIES_2_B64",
        "INSTAGRAM_COOKIES_3_B64",
    ],
    "tiktok": [
        "TIKTOK_COOKIES_1_B64",
        "TIKTOK_COOKIES_2_B64",
        "TIKTOK_COOKIES_3_B64",
    ],
}


def get_cookie_files(platform: str) -> List[str]:
    """
    Decode all available base64 cookie env vars for a platform
    into temp files. Returns list of file paths.
    """
    env_keys = COOKIE_ENV_MAP.get(platform, [])
    cookie_files = []

    for key in env_keys:
        b64_value = os.getenv(key)
        if not b64_value:
            continue

        try:
            decoded = base64.b64decode(b64_value)
            tmp = tempfile.NamedTemporaryFile(
                delete=False,
                suffix=".txt",
                prefix=f"cookies_{platform}_",
            )
            tmp.write(decoded)
            tmp.close()
            cookie_files.append(tmp.name)
            logger.info(f"Loaded cookie from {key}")
        except Exception as e:
            logger.warning(f"Failed to decode {key}: {e}")

    return cookie_files


def cleanup_cookie_files(cookie_files: List[str]):
    """Remove temp cookie files after use."""
    for f in cookie_files:
        try:
            os.unlink(f)
        except OSError:
            pass


# === HELPERS ===

def get_save_path(platform: str) -> str:
    date_str = datetime.now().strftime("%Y-%m-%d")
    folder = os.path.join(DOWNLOAD_BASE, platform, date_str)
    Path(folder).mkdir(parents=True, exist_ok=True)
    return folder


def normalize_url(url: str, platform: str) -> str:
    """Normalize URLs for known platform quirks."""

    # YouTube Shorts → standard watch URL
    if platform == "youtube" and "youtube.com/shorts/" in url:
        video_id = url.split("/shorts/")[1].split("?")[0]
        return f"https://www.youtube.com/watch?v={video_id}"

    # Facebook mobile → desktop
    if platform == "facebook" and "m.facebook.com" in url:
        url = url.replace("m.facebook.com", "www.facebook.com")

    # Instagram reels/p normalization (strip tracking params)
    if platform == "instagram":
        if "?" in url:
            url = url.split("?")[0]

    # TikTok mobile share links stay as-is (yt-dlp handles redirects)

    return url


def detect_platform(url: str) -> str:
    """
    Auto-detect platform from URL.
    Returns platform string or 'unknown'.
    """
    url_lower = url.lower()

    if any(d in url_lower for d in ["twitter.com", "x.com", "t.co"]):
        return "twitter"
    elif any(d in url_lower for d in ["youtube.com", "youtu.be"]):
        return "youtube"
    elif any(d in url_lower for d in ["facebook.com", "fb.watch", "fb.com"]):
        return "facebook"
    elif any(d in url_lower for d in ["instagram.com", "instagr.am"]):
        return "instagram"
    elif any(d in url_lower for d in ["tiktok.com", "vm.tiktok.com"]):
        return "tiktok"

    return "unknown"


def build_ytdlp_cmd(
    url: str,
    platform: str,
    output_template: str,
    cookie_file: Optional[str] = None,
) -> list:
    """Build the yt-dlp command with platform-specific flags."""

    cmd = [
        "yt-dlp",
        "--no-playlist",
        "--max-filesize", f"{MAX_FILE_SIZE_MB}M",
        "--output", output_template,
        "--print", "after_move:filepath",
        "--no-warnings",
        "--restrict-filenames",
        "--no-overwrites",
    ]

    # Cookie file (if available)
    if cookie_file:
        cmd += ["--cookies", cookie_file]

    # === PLATFORM-SPECIFIC CONFIG ===

    if platform == "youtube":
        cmd += [
            "-f", "best[ext=mp4]/best",
            "--extractor-args", "youtube:player_client=android,web",
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
            "--geo-bypass",
            "--no-check-certificates",
        ]

    elif platform == "twitter":
        cmd += [
            "-f", "best",
            "--extractor-args", "twitter:api=graphql",
        ]

    elif platform == "facebook":
        cmd += [
            "-f", "best[ext=mp4]/best",
            "--no-check-certificates",
        ]

    elif platform == "instagram":
        cmd += [
            "-f", "best[ext=mp4]/best",
            "--no-check-certificates",
        ]

    elif platform == "tiktok":
        cmd += [
            "-f", "best[ext=mp4]/best",
            "--no-check-certificates",
            "--user-agent",
            "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 "
            "(KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36",
        ]

    else:
        # Generic fallback — let yt-dlp figure it out
        cmd += ["-f", "best"]

    cmd.append(url)
    return cmd


def run_ytdlp_single(
    url: str,
    platform: str,
    output_template: str,
    cookie_file: Optional[str] = None,
) -> tuple:
    """
    Run yt-dlp once with a specific cookie file.
    Returns (file_path, error_string).
    """
    cmd = build_ytdlp_cmd(url, platform, output_template, cookie_file)

    logger.info(f"Running: {' '.join(cmd[:6])}... (cookie={'yes' if cookie_file else 'no'})")

    try:
        result = subprocess.run(cmd, capture_output=True, text=True, timeout=120)

        if result.returncode != 0:
            error_msg = result.stderr.strip() or "Unknown yt-dlp error"

            # Check for specific error patterns
            if "File is larger than max-filesize" in error_msg:
                return None, "FILE_TOO_LARGE"
            if "HTTP Error 403" in error_msg or "403" in error_msg:
                return None, "AUTH_FAILED"
            if "HTTP Error 404" in error_msg:
                return None, "NOT_FOUND"
            if "Private video" in error_msg:
                return None, "PRIVATE_VIDEO"
            if "Sign in to confirm" in error_msg or "age" in error_msg.lower():
                return None, "AGE_RESTRICTED"
            if "is not a valid URL" in error_msg:
                return None, "INVALID_URL"

            logger.error(f"yt-dlp failed: {error_msg}")
            return None, error_msg

        stdout_lines = result.stdout.strip().split("\n")
        file_path = stdout_lines[-1] if stdout_lines else None

        if not file_path or not os.path.exists(file_path):
            return None, "File not found after download"

        # Double-check file size (safety net)
        file_size = os.path.getsize(file_path)
        if file_size > MAX_FILE_SIZE_BYTES:
            os.remove(file_path)
            return None, "FILE_TOO_LARGE"

        if file_size == 0:
            os.remove(file_path)
            return None, "Downloaded file is empty"

        return file_path, None

    except subprocess.TimeoutExpired:
        return None, "TIMEOUT"
    except Exception as e:
        logger.exception("Unexpected error in run_ytdlp_single")
        return None, str(e)


def run_ytdlp(url: str, platform: str, output_template: str) -> tuple:
    """
    Run yt-dlp with cookie rotation.
    Tries each cookie file in order, falls back to no cookies.
    Returns (file_path, error_string, cookie_index_used).
    """
    url = normalize_url(url, platform)
    cookie_files = get_cookie_files(platform)

    # Non-retryable errors — don't waste cookies
    non_retryable = {
        "FILE_TOO_LARGE",
        "NOT_FOUND",
        "INVALID_URL",
        "TIMEOUT",
    }

    last_error = None

    if cookie_files:
        for i, cookie_file in enumerate(cookie_files):
            logger.info(
                f"Attempt {i + 1}/{len(cookie_files)} for {platform} "
                f"with cookie file {i + 1}"
            )

            file_path, error = run_ytdlp_single(
                url, platform, output_template, cookie_file
            )

            if file_path:
                logger.info(f"Success with cookie {i + 1}")
                cleanup_cookie_files(cookie_files)
                return file_path, None, i + 1

            last_error = error
            logger.warning(f"Cookie {i + 1} failed: {error}")

            # Don't rotate on non-retryable errors
            if error in non_retryable:
                logger.info(f"Non-retryable error '{error}', stopping rotation")
                cleanup_cookie_files(cookie_files)
                return None, error, i + 1

        # All cookies exhausted — try without cookies as last resort
        logger.info("All cookies failed, trying without cookies...")
        cleanup_cookie_files(cookie_files)

    # No cookies available OR all cookies failed → try without
    file_path, error = run_ytdlp_single(url, platform, output_template, None)

    if file_path:
        logger.info("Success without cookies")
        return file_path, None, 0

    return None, error or last_error or "All download attempts failed", -1


# === ERROR MESSAGES ===

ERROR_MESSAGES = {
    "FILE_TOO_LARGE": (
        f"Video exceeds {MAX_FILE_SIZE_MB}MB Telegram limit. "
        f"Try a shorter video or use 'Save to Folder' mode."
    ),
    "AUTH_FAILED": (
        "Authentication failed. The video may require login "
        "or cookies have expired."
    ),
    "NOT_FOUND": "Video not found. The link may be broken or deleted.",
    "PRIVATE_VIDEO": (
        "This video is private. I can't access it even with cookies."
    ),
    "AGE_RESTRICTED": (
        "This video is age-restricted and requires valid login cookies."
    ),
    "INVALID_URL": "This doesn't look like a valid video URL.",
    "TIMEOUT": "Download timed out (120s). The video may be too long.",
}


def get_error_message(error_code: str) -> str:
    return ERROR_MESSAGES.get(error_code, f"Download failed: {error_code}")


# === ROUTES ===

@app.post("/download", response_model=DownloadResponse)
async def download_video(req: DownloadRequest):
    platform = req.platform or detect_platform(req.url)
    logger.info(f"Download request: {req.url} | platform={platform}")

    save_folder = get_save_path(platform)
    output_template = os.path.join(save_folder, "%(title)s.%(ext)s")

    file_path, error, cookie_index = run_ytdlp(req.url, platform, output_template)

    if error:
        return DownloadResponse(
            success=False,
            error=get_error_message(error),
            errorCode=error,
            chatId=req.chatId,
            url=req.url,
            platform=platform,
            deliveryMode=req.deliveryMode,
            cookieIndex=cookie_index,
        )

    file_size = os.path.getsize(file_path)
    file_name = os.path.basename(file_path)

    return DownloadResponse(
        success=True,
        filePath=file_path,
        savedPath=file_path,
        fileName=file_name,
        fileSize=file_size,
        deliveryMode=req.deliveryMode,
        chatId=req.chatId,
        url=req.url,
        platform=platform,
        cookieIndex=cookie_index,
    )


@app.post("/download-stream")
async def download_stream(req: DownloadRequest):
    platform = req.platform or detect_platform(req.url)
    logger.info(f"Stream request: {req.url} | platform={platform}")

    save_folder = get_save_path(platform)
    output_template = os.path.join(save_folder, "%(title)s.%(ext)s")

    file_path, error, cookie_index = run_ytdlp(req.url, platform, output_template)

    if error:
        return JSONResponse(
            status_code=400,
            content={
                "success": False,
                "error": get_error_message(error),
                "errorCode": error,
                "platform": platform,
                "cookieIndex": cookie_index,
            },
        )

    file_name = os.path.basename(file_path)
    file_size = os.path.getsize(file_path)

    return FileResponse(
        path=file_path,
        media_type="video/mp4",
        filename=file_name,
        headers={
            "X-File-Name": file_name,
            "X-Platform": platform,
            "X-Chat-Id": str(req.chatId),
            "X-Delivery-Mode": req.deliveryMode,
            "X-File-Size": str(file_size),
            "X-Cookie-Index": str(cookie_index),
        },
    )


@app.get("/health")
async def health():
    """Health check with cookie status."""
    cookie_status = {}
    for platform, keys in COOKIE_ENV_MAP.items():
        available = sum(1 for k in keys if os.getenv(k))
        cookie_status[platform] = f"{available}/{len(keys)}"

    return {
        "status": "ok",
        "maxFileSizeMB": MAX_FILE_SIZE_MB,
        "cookies": cookie_status,
        "supportedPlatforms": list(COOKIE_ENV_MAP.keys()),
    }


@app.get("/")
async def root():
    return {
        "message": "yt-dlp API is running",
        "platforms": ["twitter", "youtube", "facebook", "instagram", "tiktok"],
    }
