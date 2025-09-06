import re
import json
import time
import httpx  # Changed from requests
from pathlib import Path
from typing import Any
import asyncio  # Added for asyncio.gather

from util import retry, save, get_media_path, log # Added log


# Regular expression to extract JSON from visitor system response
_REGX = re.compile(r"(?<=\()[\s\S]*(?=\))")
_VISITOR_URL = "https://passport.weibo.com/visitor/visitor"
_GEN_VISITOR_URL = "https://passport.weibo.com/visitor/genvisitor"


async def _gen_visitor() -> dict[str, Any] | None:  # Added async
    """
    Generates a visitor ID and returns the JSON response.
    """
    async with httpx.AsyncClient() as client:  # Use httpx.AsyncClient
        resp = await client.post(
            _GEN_VISITOR_URL, data={"cb": "gen_callback"}
        )  # Await client.post
    if resp.status_code != 200:
        log.error(f"Failed to generate visitor: {resp.status_code}") # Replaced console.print with log.error
        return None
    match = _REGX.search(resp.text)
    if match:
        return json.loads(match.group(0))
    return None


async def _incarnate(t: str) -> list[tuple[str, str]]:  # Added async
    """
    Incarnates the visitor ID to get SUB and SUBP cookies.
    """
    async with httpx.AsyncClient() as client:  # Use httpx.AsyncClient
        resp = await client.get(
            f"{_VISITOR_URL}?a=incarnate&t={t}&cb=cross_domain"
        )  # Await client.get
    if resp.status_code != 200:
        log.error(f"Failed to incarnate visitor: {resp.status_code}") # Replaced console.print with log.error
        return []

    cookies = []
    # httpx handles Set-Cookie headers differently, let's process them
    for cookie_header in resp.headers.get_list("set-cookie"):
        for part in cookie_header.split(";"):
            if "=" in part:
                k, v = part.split("=", 1)
                k, v = k.strip(), v.strip()
                if k in ["SUB", "SUBP"]:
                    cookies.append((k, v))
    return cookies


async def sina_visitor_system() -> list[tuple[str, str]]:  # Added async
    """
    Gets the necessary cookies from the Sina Visitor System.
    """
    visitor_data = await _gen_visitor()  # Await _gen_visitor
    if visitor_data and "data" in visitor_data and "tid" in visitor_data["data"]:
        tid = visitor_data["data"]["tid"]
        return await _incarnate(tid)  # Await _incarnate
    return []


async def download_media(uid: str, p: Path, obj: dict[str, Any]) -> int:  # Added async
    """
    Downloads a single image or video.
    """
    cnt = 0
    pid = obj.get("pid") or obj.get("pic_id")

    if pid:
        # Download photo
        image_filename = f"{pid}.jpg"
        image_path = get_media_path(uid, p, image_filename) # Get the full path for the image
        if not image_path.exists(): # Check if image already exists
            image_url = f"https://wx2.sinaimg.cn/large/{image_filename}"
            image_data = await get_raw_bytes(image_url)  # Await get_raw_bytes
            if image_data:
                cnt += await save(
                    uid, p, image_filename, image_data
                )
        else:
            cnt += 1 # Count existing file as downloaded

        # Download video if livephoto
        if obj.get("type") == "livephoto" and "video" in obj:
            video_filename = f"{pid}.mov"
            video_path = get_media_path(uid, p, video_filename) # Get the full path for the video
            if not video_path.exists(): # Check if video already exists
                video_url = obj["video"]
                video_data = await get_raw_bytes(video_url)  # Await get_raw_bytes
                if video_data:
                    cnt += await save(
                        uid, p, video_filename, video_data
                    )
            else:
                cnt += 1 # Count existing file as downloaded
    return cnt


async def get_image_wall(  # Added async
    uid: str, p: Path, since_id: str, cookies: list[tuple[str, str]]
) -> tuple[str, int]:
    """
    Gets a batch of images from the user's image wall.
    """
    headers = {
        "Cookie": "; ".join([f"{k}={v}" for k, v in cookies]),
        "Referer": f"https://weibo.com/u/{uid}?tabtype=album",
    }
    url = f"https://weibo.com/ajax/profile/getImageWall?uid={uid}&sinceid={since_id}"

    async with httpx.AsyncClient() as client:  # Use httpx.AsyncClient
        resp = await client.get(url, headers=headers)  # Await client.get

    if resp.status_code != 200:
        log.error(
            f"Failed to get image wall for {uid} (since_id: {since_id}): {resp.status_code}" # Replaced console.print with log.error
        )
        return since_id, 0

    body = resp.json()
    data = body.get("data")
    if not data or "list" not in data:
        return "0", 0  # No more data

    total_downloaded = 0
    # Process images concurrently
    # The original Scala code used .par, which implies parallel execution.
    # In Python, we can use asyncio.gather for concurrent execution of coroutines.
    # However, download_media involves file I/O which is blocking.
    # For now, let's keep it sequential and reconsider concurrency if it becomes a bottleneck.
    # If we truly want parallel downloads, we would need to use ThreadPoolExecutor for blocking I/O.
    # But for now, let's stick to async for network calls.
    download_tasks = []
    for pic in data["list"]:
        if pic:
            download_tasks.append(download_media(uid, p, pic))

    if download_tasks:
        results = await asyncio.gather(*download_tasks)
        total_downloaded = sum(results)

    next_since_id = data.get("since_id", "0")
    if next_since_id == "0" and "bottom_tips_text" in body:
        log.info(body["bottom_tips_text"]) # Replaced console.print with log.info

    return str(next_since_id), total_downloaded


async def get_album(
    uid: str,
    p: Path,
    cookies: list[tuple[str, str]],
    since_id: str = "0",
    total_count: int = 0,
) -> int:
    """
    Gets all images for a given user ID iteratively.
    """
    current_since_id = since_id
    current_total_count = total_count
    batch_num = 0  # Initialize batch counter

    while True:
        batch_num += 1 # Increment batch counter
        start_time = time.perf_counter()
        result = await retry(
            lambda: get_image_wall(uid, p, current_since_id, cookies)
        )
        end_time = time.perf_counter()

        if result is None:
            log.warning(
                f"Batch #{batch_num} failed after 3 retries, continuing..." # Replaced console.print with log.warning
            )
            break  # Exit loop on persistent failure

        next_id, count = result
        log.info(
            f"Finished batch #{batch_num}, {count} files in {(end_time - start_time):.2f} sec" # Replaced console.print with log.info
        )
        current_total_count += count

        if next_id == "0":
            break  # No more data, exit loop
        else:
            await asyncio.sleep(5)  # Sleep for 5 seconds before the next iteration
            current_since_id = next_id

    return current_total_count


async def get_raw_bytes(url: str) -> bytes | None:  # Added async
    """
    Fetches raw bytes from a given URL.
    """
    try:
        async with httpx.AsyncClient() as client:  # Use httpx.AsyncClient
            resp = await client.get(url, follow_redirects=True)  # Await client.get
            resp.raise_for_status()  # Raise an exception for HTTP errors
            return resp.content
    except httpx.RequestError as e:  # Changed exception type
        log.error(f"Failed to fetch {url}: {e}") # Replaced console.print with log.error
        return None
