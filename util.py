import logging
import asyncio
from pathlib import Path
from typing import Callable, TypeVar, Awaitable
import json

from rich.console import Console
from rich.logging import RichHandler

# Configure logging with Rich
console = Console()
FORMAT = "%(message)s" # Reverted format to only message
logging.basicConfig(
    level="INFO",
    format=FORMAT,
    handlers=[RichHandler(console=console, log_time_format="%Y-%m-%d %H:%M:%S")] 
)
log = logging.getLogger("weibo-dl")

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
    Saves binary data to a file asynchronously using asyncio.to_thread.
    """
    # Use asyncio.to_thread to run blocking file I/O in a separate thread
    def _blocking_save():
        # Use the get_media_path to construct the full file path
        dest_file = get_media_path(uid, base_dir, filename)
        if not dest_file.exists():
            dest_file.write_bytes(data)
            log.info(f"{dest_file} saved")
        return 1
    return await asyncio.to_thread(_blocking_save)

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
        # Run blocking file I/O in a separate thread
        def _blocking_read():
            with open(resume_file, 'r', encoding='utf-8') as f:
                return set(json.load(f))
        return await asyncio.to_thread(_blocking_read)
    except json.JSONDecodeError:
        log.warning(f"Invalid JSON in resume file: {resume_file}. Starting fresh.")
        return set()

async def write_resume_state(base_dir: Path, completed_uids: set[str]) -> None:
    """
    Writes the completed UIDs to the resume state file.
    """
    resume_file = _get_resume_file_path(base_dir)
    # Run blocking file I/O in a separate thread
    def _blocking_write():
        with open(resume_file, 'w', encoding='utf-8') as f:
            json.dump(list(completed_uids), f, indent=4)
    await asyncio.to_thread(_blocking_write)

async def add_completed_uid(base_dir: Path, uid: str) -> None:
    """
    Adds a single UID to the completed set and saves the state.
    """
    completed_uids = await read_resume_state(base_dir)
    completed_uids.add(uid)
    await write_resume_state(base_dir, completed_uids)


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
