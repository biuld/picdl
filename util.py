import logging
import asyncio
from pathlib import Path
from typing import Callable, TypeVar, Awaitable
import json
from logging.handlers import RotatingFileHandler
import aiofiles
import aiofiles.os

from rich.console import Console
from rich.logging import RichHandler

# Configure logging with Rich
console = Console()
FORMAT = "%(levelname)s - %(name)s - %(message)s"
log_formatter = logging.Formatter(FORMAT)
logging.basicConfig(
    level="INFO",
    format=FORMAT,
    handlers=[
        RichHandler(console=console, log_time_format="%Y-%m-%d %H:%M:%S"),
        RotatingFileHandler(
            "application.log", maxBytes=5*1024*1024, backupCount=1, encoding="utf-8"
        ),
    ]
)
file_handler = next(handler for handler in logging.getLogger().handlers if isinstance(handler, RotatingFileHandler))
file_handler.setFormatter(log_formatter)
log = logging.getLogger("picdl")

# Set logging level for httpx and httpcore to CRITICAL to suppress their logs.
logging.getLogger("httpx").setLevel(logging.CRITICAL)
logging.getLogger("httpcore").setLevel(logging.CRITICAL)

R = TypeVar('R')

def path(t: str) -> Path:
    """
    Converts a string path to an absolute Path object.
    """
    p = Path(t)
    if not p.is_absolute():
        p = p.resolve()
    return p

def get_media_path(uid: str, base_dir: Path, filename: str) -> Path:
    """
    Constructs the full path for a media file and ensures the directory exists.
    """
    target_dir = base_dir / uid
    target_dir.mkdir(parents=True, exist_ok=True)
    return target_dir / filename


async def save(uid: str, base_dir: Path, filename: str, data: bytes) -> int:
    """
    Saves binary data to a file asynchronously using aiofiles.
    """
    # Use get_media_path to construct the full file path
    dest_file = get_media_path(uid, base_dir, filename)
    if not dest_file.exists():
        async with aiofiles.open(dest_file, 'wb') as f:
            await f.write(data)
        log.info(f"{dest_file} saved")
    return 1

def _get_resume_file_path(base_dir: Path) -> Path:
    """
    Returns the path to the resume state file.
    """
    return base_dir / "resume_state.json"

async def read_resume_state(base_dir: Path) -> set[str]:
    """
    Reads the completed UIDs from the resume state file.
    """
    resume_file = _get_resume_file_path(base_dir)
    if not resume_file.exists():
        return set()
    try:
        async with aiofiles.open(resume_file, 'r', encoding='utf-8') as f:
            content = await f.read()
            return set(json.loads(content))
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON in resume file: {resume_file}. Starting fresh.")
        return set()

async def write_resume_state(base_dir: Path, completed_uids: set[str]) -> None:
    """
    Writes the completed UIDs to the resume state file.
    """
    resume_file = _get_resume_file_path(base_dir)
    async with aiofiles.open(resume_file, 'w', encoding='utf-8') as f:
        await f.write(json.dumps(list(completed_uids), indent=4))

async def add_completed_uid(base_dir: Path, uid: str) -> None:
    """
    Adds a single UID to the completed set and saves the state.
    """
    completed_uids = await read_resume_state(base_dir)
    completed_uids.add(uid)
    await write_resume_state(base_dir, completed_uids)


async def delete_resume_state(base_dir: Path) -> None:
    """Deletes the resume state file."""
    resume_file_path = _get_resume_file_path(base_dir)
    if resume_file_path.exists():
        await aiofiles.os.remove(resume_file_path)
        log.info(f"Deleted resume state file: {resume_file_path}")
    else:
        log.info(f"Resume state file not found, no need to delete: {resume_file_path}")


async def retry[T](func: Callable[[], Awaitable[T]], times: int = 0) -> T | None:
    """
    Retries an async function up to 3 times with a delay.
    """
    try:
        return await func()
    except Exception as e:
        if times >= 3:
            log.info(f"encountered {e}, giving up after 3 retries")
            return None
        log.info(f"encountered {e}, retrying...")
        await asyncio.sleep(times * 5) # Use asyncio.sleep for async delay
        return await retry(func, times + 1)
