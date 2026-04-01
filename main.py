from fastapi import FastAPI
from fastapi.responses import FileResponse, JSONResponse
from pydantic import BaseModel, validator
from typing import Union, Optional, List, Tuple
import subprocess
import os
import base64
import tempfile
import logging
import json
import re
from datetime import datetime
from pathlib import Path
from urllib.parse import urljoin

import requests

app = FastAPI(title="yt-dlp API for n8n")

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

# === CONFIG ===
BASE_DIR = os.path.dirname(os.path.abspath(__file__))
DOWNLOAD_BASE = os.getenv("DOWNLOAD_BASE", "/mnt/nas/video_downloader")
MAX_FILE_SIZE_MB = int(os.getenv("MAX_FILE_SIZE_MB", "50"))
MAX_FILE_SIZE_BYTES = MAX_FILE_SIZE_MB * 1024 * 1024
REQUEST_HEADERS = {
    "User-Agent": (
        "Mozilla/5.0 (Windows NT 10.0; Win64; x64) "
        "AppleWebKit/537.36 (KHTML, like Gecko) "
        "Chrome/120.0.0.0 Safari/537.36"
    )
}


# === MODELS ===
class DownloadRequest(BaseModel):
    url: str
    platform: str
    chatId: Union[str, int]
    deliveryMode: str

    @validator("chatId", pre=True)
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
        "YOUTUBE_COOKIES_B64",
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
    "reddit": [
        "REDDIT_COOKIES_1_B64",
        "REDDIT_COOKIES_2_B64",
        "REDDIT_COOKIES_3_B64",
    ],
    "direct": [],
}


def get_cookie_files(platform: str) -> List[str]:
    env_keys = COOKIE_ENV_MAP.get(platform, [])
    cookie_files = []
    seen_values = set()

    for key in env_keys:
        b64_value = os.getenv(key)
        if not b64_value:
            continue

        if b64_value in seen_values:
            continue
        seen_values.add(b64_value)

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
    if platform == "youtube" and "youtube.com/shorts/" in url:
        video_id = url.split("/shorts/")[1].split("?")[0]
        return f"https://www.youtube.com/watch?v={video_id}"

    if platform == "facebook" and "m.facebook.com" in url:
        url = url.replace("m.facebook.com", "www.facebook.com")

    if platform == "instagram" and "?" in url:
        url = url.split("?")[0]

    return url


def is_direct_video_url(url: str) -> bool:
    base = url.lower().split("?")[0]
    return any(base.endswith(ext) for ext in [".mp4", ".mov", ".webm", ".m4v", ".m3u8"])


def detect_platform(url: str) -> str:
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
    elif any(d in url_lower for d in ["reddit.com", "redd.it", "old.reddit.com"]):
        return "reddit"
    elif any(d in url_lower for d in [
        "cdn.videy.co",
        "hoesfree.online",
        "github.io"
    ]) or is_direct_video_url(url_lower):
        return "direct"

    return "unknown"


def classify_ytdlp_error(error_msg: str) -> str:
    lower_error = error_msg.lower()

    if "file is larger than max-filesize" in lower_error:
        return "FILE_TOO_LARGE"
    if "requested format is not available" in lower_error:
        return "FORMAT_NOT_AVAILABLE"
    if "http error 403" in lower_error:
        return "AUTH_FAILED"
    if "http error 404" in lower_error:
        return "NOT_FOUND"
    if "private video" in lower_error:
        return "PRIVATE_VIDEO"
    if "sign in to confirm your age" in lower_error:
        return "AGE_RESTRICTED"
    if "age-restricted" in lower_error:
        return "AGE_RESTRICTED"
    if "this video may be inappropriate for some users" in lower_error:
        return "AGE_RESTRICTED"
    if "sign in to confirm you’re not a bot" in lower_error or "sign in to confirm you're not a bot" in lower_error:
        return "BOT_PROTECTION"
    if "login required" in lower_error:
        return "AUTH_FAILED"
    if "is not a valid url" in lower_error:
        return "INVALID_URL"
    if "unsupported url" in lower_error:
        return "INVALID_URL"
    if "unable to extract" in lower_error:
        return "EXTRACTION_FAILED"
    if "video unavailable" in lower_error:
        return "NOT_FOUND"
    if "forbidden" in lower_error:
        return "AUTH_FAILED"

    return error_msg


def run_subprocess(cmd: list, timeout: int = 120):
    logger.info(f"Running yt-dlp command: {' '.join(cmd)}")
    return subprocess.run(cmd, capture_output=True, text=True, timeout=timeout)


def extract_file_path(stdout: str) -> Optional[str]:
    stdout_lines = [line.strip() for line in stdout.strip().split("\n") if line.strip()]
    if not stdout_lines:
        return None
    return stdout_lines[-1]


def find_latest_media_file(folder: str) -> Optional[str]:
    try:
        p = Path(folder)
        if not p.exists():
            return None

        candidates = []
        for ext in ["*.mp4", "*.mov", "*.webm", "*.m4v", "*.mkv", "*.mp3", "*.m4a", "*.ts"]:
            candidates.extend(list(p.glob(ext)))

        if not candidates:
            return None

        latest = max(candidates, key=lambda x: x.stat().st_mtime)
        return str(latest)
    except Exception as e:
        logger.warning(f"Failed scanning folder for downloaded files: {e}")
        return None


def validate_downloaded_file(file_path: str) -> Tuple[Optional[str], Optional[str]]:
    if not file_path or not os.path.exists(file_path):
        return None, "File not found after download"

    file_size = os.path.getsize(file_path)

    if file_size > MAX_FILE_SIZE_BYTES:
        try:
            os.remove(file_path)
        except OSError:
            pass
        return None, "FILE_TOO_LARGE"

    if file_size == 0:
        try:
            os.remove(file_path)
        except OSError:
            pass
        return None, "Downloaded file is empty"

    return file_path, None


def build_base_cmd(output_template: str, cookie_file: Optional[str] = None) -> list:
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
    if cookie_file:
        cmd += ["--cookies", cookie_file]
    return cmd


def build_probe_cmd(url: str, platform: str, cookie_file: Optional[str] = None) -> list:
    cmd = [
        "yt-dlp",
        "--dump-single-json",
        "--no-warnings",
    ]
    if cookie_file:
        cmd += ["--cookies", cookie_file]

    if platform == "youtube":
        cmd += [
            "--extractor-args", "youtube:player_client=android,web",
            "--user-agent", REQUEST_HEADERS["User-Agent"],
            "--geo-bypass",
            "--no-check-certificates",
        ]
    elif platform in ["reddit", "direct"]:
        cmd += [
            "--user-agent", REQUEST_HEADERS["User-Agent"],
        ]

    cmd.append(url)
    return cmd


def probe_video(url: str, platform: str, cookie_file: Optional[str] = None) -> Tuple[Optional[dict], Optional[str]]:
    try:
        cmd = build_probe_cmd(url, platform, cookie_file)
        result = run_subprocess(cmd, timeout=60)

        if result.returncode != 0:
            error_msg = result.stderr.strip() or result.stdout.strip() or "Probe failed"
            logger.error(f"Probe stderr: {result.stderr}")
            logger.error(f"Probe stdout: {result.stdout}")
            return None, classify_ytdlp_error(error_msg)

        try:
            data = json.loads(result.stdout)
            return data, None
        except Exception as e:
            logger.warning(f"Probe JSON parse failed: {e}")
            logger.warning(f"Probe output: {result.stdout[:500]}")
            return None, "EXTRACTION_FAILED"

    except subprocess.TimeoutExpired:
        return None, "TIMEOUT"
    except Exception as e:
        logger.exception("Unexpected probe error")
        return None, str(e)


def extract_direct_media_from_page(url: str) -> Optional[str]:
    try:
        logger.info(f"Trying HTML extraction fallback for wrapper URL: {url}")
        r = requests.get(url, headers=REQUEST_HEADERS, timeout=20)
        if r.status_code >= 400:
            logger.warning(f"Wrapper fetch failed with status {r.status_code}")
            return None

        html = r.text

        patterns = [
            r'''<video[^>]+src=["']([^"']+)["']''',
            r'''<source[^>]+src=["']([^"']+)["']''',
            r'''["'](https?:\/\/[^"']+\.(?:mp4|m3u8|webm|mov|m4v)[^"']*)["']''',
            r'''["'](\/[^"']+\.(?:mp4|m3u8|webm|mov|m4v)[^"']*)["']''',
            r'''[?&]v=([^&]+\.mp4)''',
        ]

        for pattern in patterns:
            match = re.search(pattern, html, re.IGNORECASE)
            if match:
                candidate = match.group(1)
                if not candidate.startswith("http"):
                    candidate = urljoin(url, candidate)
                logger.info(f"Extracted direct media URL from wrapper: {candidate}")
                return candidate

        parsed_v_param = re.search(r'[?&]v=([^&]+\.mp4)', url, re.IGNORECASE)
        if parsed_v_param:
            candidate = parsed_v_param.group(1)
            if candidate.startswith("http"):
                return candidate

        logger.warning("No direct media source found in wrapper HTML")
        return None

    except Exception as e:
        logger.warning(f"Wrapper extraction failed: {e}")
        return None


def build_strategy_commands(
    url: str,
    platform: str,
    output_template: str,
    cookie_file: Optional[str] = None,
) -> List[list]:
    base = build_base_cmd(output_template, cookie_file)

    if platform == "youtube":
        return [
            base + [
                "-f", "best",
                "--extractor-args", "youtube:player_client=android,web",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                "--geo-bypass",
                "--no-check-certificates",
                url,
            ],
            base + [
                "-f", "best[ext=mp4]/best",
                "--extractor-args", "youtube:player_client=web",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                "--geo-bypass",
                "--no-check-certificates",
                url,
            ],
            base + [
                "-f", "b",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [url],
        ]

    if platform == "twitter":
        return [
            base + ["-f", "best", "--extractor-args", "twitter:api=graphql", url],
            base + ["-f", "best", url],
            base + [url],
        ]

    if platform == "facebook":
        return [
            base + ["-f", "best[ext=mp4]/best", "--no-check-certificates", url],
            base + ["-f", "best", url],
            base + [url],
        ]

    if platform == "instagram":
        return [
            base + ["-f", "best[ext=mp4]/best", "--no-check-certificates", url],
            base + ["-f", "best", url],
            base + [url],
        ]

    if platform == "tiktok":
        return [
            base + [
                "-f", "best[ext=mp4]/best",
                "--no-check-certificates",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [
                "-f", "best",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [url],
        ]

    if platform == "reddit":
        return [
            base + [
                "-f", "bestvideo+bestaudio/best",
                "--merge-output-format", "mp4",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [
                "-f", "best",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [url],
        ]

    if platform == "direct":
        return [
            base + [
                "-f", "best",
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [
                "--user-agent", REQUEST_HEADERS["User-Agent"],
                url,
            ],
            base + [url],
        ]

    return [
        base + ["-f", "best", url],
        base + [url],
    ]


def run_ytdlp_single(
    url: str,
    platform: str,
    output_template: str,
    cookie_file: Optional[str] = None,
) -> tuple:
    original_url = url

    if platform == "direct" and not is_direct_video_url(url):
        extracted = extract_direct_media_from_page(url)
        if extracted:
            url = extracted
            logger.info(f"Wrapper URL converted to direct media URL: {url}")

    probe_data, probe_error = probe_video(url, platform, cookie_file)

    if probe_error:
        logger.warning(f"Probe failed for {platform}: {probe_error}")
        if probe_error in {
            "AUTH_FAILED",
            "BOT_PROTECTION",
            "AGE_RESTRICTED",
            "PRIVATE_VIDEO",
            "NOT_FOUND",
            "INVALID_URL",
        }:
            if platform == "direct" and url == original_url:
                extracted = extract_direct_media_from_page(original_url)
                if extracted and extracted != original_url:
                    logger.info("Retrying direct probe with extracted wrapper media URL")
                    url = extracted
                    probe_data, probe_error = probe_video(url, platform, cookie_file)
                    if not probe_error:
                        logger.info("Probe succeeded after wrapper extraction")
                    else:
                        logger.warning(f"Probe still failed after wrapper extraction: {probe_error}")
                        return None, probe_error
                else:
                    return None, probe_error
            else:
                return None, probe_error
    else:
        logger.info(
            f"Probe success: title={probe_data.get('title')} "
            f"formats={len(probe_data.get('formats', []))}"
        )

    commands = build_strategy_commands(url, platform, output_template, cookie_file)
    last_error = probe_error
    output_folder = str(Path(output_template).parent)

    for attempt_index, cmd in enumerate(commands, start=1):
        try:
            logger.info(
                f"Strategy {attempt_index}/{len(commands)} for platform={platform} "
                f"(cookie={'yes' if cookie_file else 'no'})"
            )

            result = run_subprocess(cmd, timeout=120)

            if result.returncode == 0:
                file_path = extract_file_path(result.stdout)

                # Fallback: if stdout doesn't return usable final path, scan folder
                if not file_path or not os.path.exists(file_path):
                    logger.warning("Stdout path missing or invalid, scanning folder for latest media file...")
                    file_path = find_latest_media_file(output_folder)

                validated_path, validation_error = validate_downloaded_file(file_path)
                if validated_path:
                    return validated_path, None

                last_error = validation_error
                logger.warning(f"Downloaded file failed validation: {validation_error}")
                continue

            error_msg = result.stderr.strip() or result.stdout.strip() or "Unknown yt-dlp error"
            classified = classify_ytdlp_error(error_msg)

            logger.error(f"yt-dlp stderr: {result.stderr}")
            logger.error(f"yt-dlp stdout: {result.stdout}")
            logger.warning(f"Strategy {attempt_index} failed with: {classified}")

            last_error = classified

            if classified in {
                "AGE_RESTRICTED",
                "PRIVATE_VIDEO",
                "AUTH_FAILED",
                "BOT_PROTECTION",
                "NOT_FOUND",
                "INVALID_URL",
                "TIMEOUT",
                "FILE_TOO_LARGE",
            }:
                return None, classified

            continue

        except subprocess.TimeoutExpired:
            logger.warning(f"Strategy {attempt_index} timed out")
            return None, "TIMEOUT"
        except Exception as e:
            logger.exception("Unexpected error in run_ytdlp_single")
            last_error = str(e)

    return None, last_error or "All format strategies failed"


def run_ytdlp(url: str, platform: str, output_template: str) -> tuple:
    url = normalize_url(url, platform)
    cookie_files = get_cookie_files(platform)

    non_retryable = {
        "FILE_TOO_LARGE",
        "NOT_FOUND",
        "INVALID_URL",
        "TIMEOUT",
    }

    last_error = None

    if cookie_files:
        for i, cookie_file in enumerate(cookie_files):
            logger.info(f"Attempt {i + 1}/{len(cookie_files)} for {platform} with cookie file {i + 1}")

            file_path, error = run_ytdlp_single(url, platform, output_template, cookie_file)

            if file_path:
                logger.info(f"Success with cookie {i + 1}")
                cleanup_cookie_files(cookie_files)
                return file_path, None, i + 1

            last_error = error
            logger.warning(f"Cookie {i + 1} failed: {error}")

            if error in non_retryable:
                logger.info(f"Non-retryable error '{error}', stopping rotation")
                cleanup_cookie_files(cookie_files)
                return None, error, i + 1

        logger.info("All cookies failed, trying without cookies...")
        cleanup_cookie_files(cookie_files)

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
    "AUTH_FAILED": "Authentication failed. The video may require login or cookies have expired.",
    "BOT_PROTECTION": "The platform blocked this request as suspicious. Fresh cookies are required.",
    "NOT_FOUND": "Video not found. The link may be broken, deleted, or unavailable.",
    "PRIVATE_VIDEO": "This video is private. I can't access it without valid cookies.",
    "AGE_RESTRICTED": "This video is age-restricted and requires valid login cookies.",
    "INVALID_URL": "This doesn't look like a valid video URL.",
    "TIMEOUT": "Download timed out (120s). The video may be too long.",
    "FORMAT_NOT_AVAILABLE": "No compatible downloadable format was found for this video.",
    "EXTRACTION_FAILED": "Could not extract this video. The platform may be blocking access.",
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
    cookie_status = {}
    for platform, keys in COOKIE_ENV_MAP.items():
        available = sum(1 for k in keys if os.getenv(k))
        cookie_status[platform] = f"{available}/{len(keys)}"

    return {
        "status": "ok",
        "downloadBase": DOWNLOAD_BASE,
        "maxFileSizeMB": MAX_FILE_SIZE_MB,
        "cookies": cookie_status,
        "supportedPlatforms": list(COOKIE_ENV_MAP.keys()),
    }


@app.get("/")
async def root():
    return {
        "message": "yt-dlp API is running",
        "platforms": ["twitter", "youtube", "facebook", "instagram", "tiktok", "reddit", "direct"],
    }
