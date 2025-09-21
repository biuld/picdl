import asyncio
import typer
from datetime import datetime
from functools import wraps
from apscheduler.schedulers.asyncio import AsyncIOScheduler # type: ignore
from util import log, path, read_resume_state, add_completed_uid, delete_resume_state # Added resume functions
from weibo.api import get_album, sina_visitor_system


app = typer.Typer()

async def _run_walk_uids_job(p: str) -> None:
    """
    Core logic for walking through UIDs and downloading albums.
    This function can be called by both the 'walk' command and the scheduler.
    """
    dir_path = path(p)
    if not dir_path.exists():
        log.error(f"Directory does not exist: {dir_path}") # Replaced console.print with log.error
        return
    if not any(dir_path.iterdir()):
        log.warning(
            f"{p} is empty, try to create an empty folder under {p}, then name it after your uid. " # Replaced console.print with log.warning
            "All the images will be downloaded into it!"
        )
        return

    completed_uids = await read_resume_state(dir_path)
    log.info(f"Loaded {len(completed_uids)} completed UIDs from resume state.")

    all_uids_in_dir = [
        item.name
        for item in dir_path.iterdir()
        if item.is_dir() and not item.name.startswith("SKIP_")
    ]
    uids_to_process = sorted([uid for uid in all_uids_in_dir if uid not in completed_uids])

    if not uids_to_process:
        log.info("All UIDs already processed or no UIDs to process. Exiting.")
        return

    log.info(f"Processing {len(uids_to_process)} UIDs: {uids_to_process}")

    cookies = await sina_visitor_system()

    for uid in uids_to_process:
        log.info(f"fetching {uid}")
        # Create target directory for UID, if it doesn't exist.
        target_uid_dir = dir_path / uid
        target_uid_dir.mkdir(parents=True, exist_ok=True)
        total_count = await get_album(uid, dir_path, cookies) # Changed target_uid_dir to dir_path
        log.info(f"{total_count} files downloaded for {uid}")
        await add_completed_uid(dir_path, uid)
        await asyncio.sleep(60)
    log.info("finished term")
    await delete_resume_state(dir_path)


@app.command("walk")
@lambda f: wraps(f)(lambda *a, **kw: asyncio.run(f(*a, **kw)))
async def walk_uids(
    p: str = typer.Argument(..., help="Path to the directory containing UID subdirectories.")
) -> None:
    """
    Walks through UIDs inside the given path and downloads albums.
    """
    await _run_walk_uids_job(p)


@app.command("download")
@lambda f: wraps(f)(lambda *a, **kw: asyncio.run(f(*a, **kw)))
async def download_uid(
    uid: str = typer.Argument(..., help="The user ID to download images for."),
    p: str = typer.Argument(..., help="Path to the directory where images will be downloaded.")
) -> None:
    """
    Downloads all images for a specific user ID into the given path.
    """
    dir_path = path(p)
    if not dir_path.exists():
        log.error(f"Directory does not exist: {p}") # Replaced console.print with log.error
        raise typer.Exit(code=1)
    cookies = await sina_visitor_system()
    log.info(f"fetching {uid}")
    cnt = await get_album(uid, dir_path, cookies)
    log.info(f"{uid}: {cnt} in total")
    log.info(f"Successfully downloaded {cnt} images for UID {uid} into {p}") # Replaced console.print with log.info


@app.command("schedule")
@lambda f: wraps(f)(lambda *a, **kw: asyncio.run(f(*a, **kw)))
async def schedule_downloads(
    p: str = typer.Argument(..., help="Path to the directory containing UID subdirectories.")
) -> None:
    """
    Walks through UIDs inside the given path periodically (once a day).
    """
    log.info(f"Scheduling downloads for path: {p}...") # Replaced console.print with log.info
    scheduler = AsyncIOScheduler()
    # Schedule the job to run immediately, then once a day
    scheduler.add_job(_run_walk_uids_job, "interval", days=1, args=[p], next_run_time=datetime.now())
    scheduler.start()
    log.info("Scheduler started. Press Ctrl+C to exit.") # Replaced console.print with log.info

    # Keep the main thread alive to allow the scheduler to run
    try:
        while True:
            await asyncio.sleep(3600)  # Sleep for an hour, or indefinitely
    except (KeyboardInterrupt, SystemExit):
        scheduler.shutdown()
        log.info("Scheduler shut down.") # Replaced console.print with log.info
        raise typer.Exit()


if __name__ == "__main__":
    try:
        app()
    except typer.Exit as e:
        log.error(f"Application exited with code: {e.exit_code}") # Replaced console.print with log.error
    except Exception as e:
        log.error(f"An unexpected error occurred: {e}", exc_info=True)
