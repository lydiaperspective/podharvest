import asyncio
import calendar
import html
import json
import logging
import os
import re
import sqlite3
import threading
import time
import xml.etree.ElementTree as ET
from contextlib import asynccontextmanager
from datetime import datetime, timezone
from pathlib import Path
from typing import Any
from urllib.parse import quote_plus, urlparse

import feedparser
import httpx
from fastapi import FastAPI, File, HTTPException, Query, UploadFile
from fastapi.responses import FileResponse, PlainTextResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s [%(levelname)s] %(message)s",
)
log = logging.getLogger("podharvest")

CONFIG_DIR = Path(os.getenv("CONFIG_DIR", "/config"))
PODCASTS_DIR = Path(os.getenv("PODCASTS_DIR", "/podcasts"))
DB_PATH = CONFIG_DIR / "podharvest.db"
STATIC_DIR = Path(__file__).parent / "static"

USER_AGENT = "PodHarvest/1.0 (+https://github.com/local/podharvest)"

DEFAULT_SETTINGS: dict[str, Any] = {
    "primary_index_url": "https://podcast.4thepeople.live",
    "enable_primary_index": True,
    "enable_itunes_fallback": True,
    "auto_download": True,
    "poll_interval_minutes": 60,
    "initial_episodes": 1,
    "max_episodes_per_show": 0,
    "retention_days": 0,
    "folder_template": "{podcast}",
    "filename_template": "{date} - {title}.{ext}",
    "save_cover_art": True,
    "write_nfo": False,
    "download_timeout_seconds": 300,
}

db_lock = threading.Lock()
poll_lock = asyncio.Lock()
last_poll_epoch: float = 0.0
poll_status: dict[str, Any] = {
    "running": False,
    "last_run": None,
    "last_message": "Idle",
}

# Keyed by audio_url for live progress tracking across both subscribed and preview episodes
download_progress: dict[str, dict[str, Any]] = {}


def get_db() -> sqlite3.Connection:
    CONFIG_DIR.mkdir(parents=True, exist_ok=True)
    conn = sqlite3.connect(DB_PATH, check_same_thread=False)
    conn.row_factory = sqlite3.Row
    conn.execute("PRAGMA journal_mode=WAL;")
    conn.execute("PRAGMA foreign_keys=ON;")
    return conn


def init_db() -> None:
    with db_lock:
        conn = get_db()
        try:
            conn.executescript(
                """
                CREATE TABLE IF NOT EXISTS settings (
                    key TEXT PRIMARY KEY,
                    value TEXT NOT NULL
                );

                CREATE TABLE IF NOT EXISTS podcasts (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    title TEXT NOT NULL,
                    author TEXT DEFAULT '',
                    description TEXT DEFAULT '',
                    feed_url TEXT NOT NULL UNIQUE,
                    artwork_url TEXT DEFAULT '',
                    folder_name TEXT NOT NULL,
                    auto_download INTEGER NOT NULL DEFAULT 1,
                    keep_latest INTEGER NOT NULL DEFAULT 0,
                    created_at TEXT NOT NULL,
                    last_checked TEXT
                );

                CREATE TABLE IF NOT EXISTS episodes (
                    id INTEGER PRIMARY KEY AUTOINCREMENT,
                    podcast_id INTEGER NOT NULL REFERENCES podcasts(id) ON DELETE CASCADE,
                    guid TEXT NOT NULL,
                    title TEXT NOT NULL,
                    description TEXT DEFAULT '',
                    published_at TEXT DEFAULT '',
                    published_ts INTEGER NOT NULL DEFAULT 0,
                    audio_url TEXT NOT NULL,
                    file_path TEXT DEFAULT '',
                    file_size INTEGER NOT NULL DEFAULT 0,
                    duration TEXT DEFAULT '',
                    status TEXT NOT NULL DEFAULT 'new',
                    error_msg TEXT DEFAULT '',
                    UNIQUE(podcast_id, guid)
                );
                """
            )
            for k, v in DEFAULT_SETTINGS.items():
                conn.execute(
                    "INSERT OR IGNORE INTO settings (key, value) VALUES (?, ?)",
                    (k, json.dumps(v)),
                )
            conn.commit()
        finally:
            conn.close()


def normalize_feed_url(url: str) -> str:
    return (url or "").strip().rstrip("/").lower()


def get_subscribed_feed_urls() -> set[str]:
    with db_lock:
        conn = get_db()
        try:
            rows = conn.execute("SELECT feed_url FROM podcasts").fetchall()
            return {normalize_feed_url(r["feed_url"]) for r in rows if r["feed_url"]}
        finally:
            conn.close()


def format_duration_hm(raw_duration: str) -> str:
    raw = str(raw_duration or "").strip()
    if not raw:
        return ""

    total_seconds = 0
    if ":" in raw:
        parts = [p.strip() for p in raw.split(":")]
        try:
            if len(parts) == 3:
                total_seconds = int(float(parts[0])) * 3600 + int(float(parts[1])) * 60 + int(float(parts[2]))
            elif len(parts) == 2:
                total_seconds = int(float(parts[0])) * 60 + int(float(parts[1]))
            else:
                return raw
        except ValueError:
            return raw
    else:
        try:
            total_seconds = int(float(raw))
        except ValueError:
            return raw

    if total_seconds <= 0:
        return ""

    total_minutes = round(total_seconds / 60)
    if total_minutes == 0:
        return "0h 1m"

    hours = total_minutes // 60
    minutes = total_minutes % 60
    if hours > 0:
        return f"{hours}h {minutes}m"
    return f"{minutes}m"


def load_settings() -> dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            rows = conn.execute("SELECT key, value FROM settings").fetchall()
            merged = dict(DEFAULT_SETTINGS)
            for row in rows:
                try:
                    merged[row["key"]] = json.loads(row["value"])
                except json.JSONDecodeError:
                    merged[row["key"]] = row["value"]
            return merged
        finally:
            conn.close()


def save_settings(updates: dict[str, Any]) -> dict[str, Any]:
    current = load_settings()
    for k, v in updates.items():
        if k in DEFAULT_SETTINGS:
            current[k] = v
    with db_lock:
        conn = get_db()
        try:
            for k, v in current.items():
                conn.execute(
                    "INSERT OR REPLACE INTO settings (key, value) VALUES (?, ?)",
                    (k, json.dumps(v)),
                )
            conn.commit()
        finally:
            conn.close()
    return current


def sanitize_segment(name: str, fallback: str = "Unknown") -> str:
    cleaned = html.unescape(name or "").strip()
    cleaned = re.sub(r'[<>:"/\\|?*\x00-\x1f]', "", cleaned)
    cleaned = re.sub(r"\s+", " ", cleaned).strip(" .")
    if not cleaned:
        cleaned = fallback
    return cleaned[:120]


def guess_extension(audio_url: str, mime_type: str = "") -> str:
    mime_map = {
        "audio/mpeg": "mp3",
        "audio/mp3": "mp3",
        "audio/mp4": "m4a",
        "audio/x-m4a": "m4a",
        "audio/aac": "m4a",
        "audio/ogg": "ogg",
        "audio/opus": "opus",
        "audio/wav": "wav",
    }
    if mime_type and mime_type.lower() in mime_map:
        return mime_map[mime_type.lower()]
    path = urlparse(audio_url).path.lower()
    for ext in ("mp3", "m4a", "ogg", "opus", "wav", "aac"):
        if path.endswith(f".{ext}"):
            return ext
    return "mp3"


def build_episode_target_path(
    settings: dict[str, Any],
    podcast_title: str,
    episode_title: str,
    published_ts: int,
    ext: str,
) -> Path:
    dt = (
        datetime.fromtimestamp(published_ts, tz=timezone.utc)
        if published_ts > 0
        else datetime.now(timezone.utc)
    )
    tokens = {
        "podcast": sanitize_segment(podcast_title, "Podcast"),
        "title": sanitize_segment(episode_title, "Episode"),
        "year": dt.strftime("%Y"),
        "month": dt.strftime("%m"),
        "day": dt.strftime("%d"),
        "date": dt.strftime("%Y-%m-%d"),
        "ext": sanitize_segment(ext, "mp3"),
    }

    folder_tpl = settings.get("folder_template") or "{podcast}"
    file_tpl = settings.get("filename_template") or "{date} - {title}.{ext}"

    try:
        raw_folder = folder_tpl.format(**tokens)
    except Exception:
        raw_folder = tokens["podcast"]

    try:
        raw_file = file_tpl.format(**tokens)
    except Exception:
        raw_file = f"{tokens['date']} - {tokens['title']}.{tokens['ext']}"

    safe_parts = [
        sanitize_segment(part, "Podcast")
        for part in Path(raw_folder).parts
        if part not in ("", ".", "..", "/")
    ]
    if not safe_parts:
        safe_parts = [tokens["podcast"]]

    safe_filename = sanitize_segment(raw_file, f"episode.{tokens['ext']}")
    if not safe_filename.lower().endswith(f".{tokens['ext'].lower()}"):
        safe_filename = f"{safe_filename}.{tokens['ext']}"

    target = PODCASTS_DIR.joinpath(*safe_parts, safe_filename)
    base_resolved = PODCASTS_DIR.resolve()
    target_resolved = target.parent.resolve() / target.name
    if not target_resolved.is_relative_to(base_resolved):
        raise ValueError("Resolved episode path escapes PODCASTS_DIR")
    return target


async def fetch_and_parse_feed(feed_url: str) -> dict[str, Any]:
    async with httpx.AsyncClient(
        timeout=25.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        resp = await client.get(feed_url)
        resp.raise_for_status()
        raw_bytes = resp.content

    parsed = feedparser.parse(raw_bytes)
    feed_info = parsed.get("feed", {})
    title = feed_info.get("title") or urlparse(feed_url).netloc
    author = feed_info.get("author") or feed_info.get("itunes_author") or ""
    description = feed_info.get("subtitle") or feed_info.get("summary") or ""
    artwork = ""
    if isinstance(feed_info.get("image"), dict):
        artwork = feed_info["image"].get("href", "")
    if not artwork:
        artwork = feed_info.get("itunes_image", {}).get("href", "") if isinstance(feed_info.get("itunes_image"), dict) else ""

    episodes = []
    for entry in parsed.get("entries", []):
        audio_url = ""
        mime_type = ""
        for enc in entry.get("enclosures", []):
            href = enc.get("href") or enc.get("url") or ""
            etype = enc.get("type", "")
            if href and (etype.startswith("audio/") or any(href.lower().split("?")[0].endswith(x) for x in (".mp3", ".m4a", ".ogg", ".opus", ".aac", ".wav"))):
                audio_url = href
                mime_type = etype
                break
        if not audio_url:
            for link in entry.get("links", []):
                if link.get("rel") == "enclosure" and link.get("href"):
                    audio_url = link["href"]
                    mime_type = link.get("type", "")
                    break
        if not audio_url:
            continue

        guid = entry.get("id") or entry.get("guid") or audio_url
        ep_title = entry.get("title") or "Untitled Episode"
        ep_desc = entry.get("summary") or entry.get("description") or ""
        pub_parsed = entry.get("published_parsed") or entry.get("updated_parsed")
        if pub_parsed:
            pub_ts = int(calendar.timegm(pub_parsed))
            pub_iso = datetime.fromtimestamp(pub_ts, tz=timezone.utc).strftime("%Y-%m-%d")
        else:
            pub_ts = int(time.time())
            pub_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d")

        episodes.append(
            {
                "guid": str(guid),
                "title": str(ep_title),
                "description": re.sub(r"<[^>]+>", "", str(ep_desc))[:1000],
                "published_at": pub_iso,
                "published_ts": pub_ts,
                "audio_url": str(audio_url),
                "mime_type": str(mime_type),
                "duration": format_duration_hm(str(entry.get("itunes_duration", ""))),
            }
        )

    return {
        "title": str(title),
        "author": str(author),
        "description": re.sub(r"<[^>]+>", "", str(description))[:1000],
        "feed_url": feed_url,
        "artwork_url": str(artwork),
        "episodes": episodes,
    }


async def search_4thepeople(query: str, base_url: str) -> list[dict[str, Any]]:
    base = base_url.rstrip("/")
    encoded = quote_plus(query)
    candidate_urls = [
        f"{base}/api/search?q={encoded}",
        f"{base}/api/podcasts/search?q={encoded}",
        f"{base}/api/v1/search?q={encoded}",
    ]
    async with httpx.AsyncClient(
        timeout=6.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT, "Accept": "application/json"},
    ) as client:
        for url in candidate_urls:
            try:
                resp = await client.get(url)
                if resp.status_code != 200:
                    continue
                ctype = resp.headers.get("content-type", "")
                if "json" not in ctype:
                    continue
                data = resp.json()
                raw_items = []
                if isinstance(data, list):
                    raw_items = data
                elif isinstance(data, dict):
                    for key in ("feeds", "results", "podcasts", "items", "data"):
                        if isinstance(data.get(key), list):
                            raw_items = data[key]
                            break
                results = []
                for item in raw_items:
                    if not isinstance(item, dict):
                        continue
                    feed_url = (
                        item.get("feedUrl")
                        or item.get("feed_url")
                        or item.get("url")
                        or item.get("rss")
                        or ""
                    )
                    title = item.get("title") or item.get("name") or item.get("collectionName") or ""
                    if not feed_url or not title:
                        continue
                    results.append(
                        {
                            "title": title,
                            "author": item.get("author") or item.get("artistName") or "",
                            "feed_url": feed_url,
                            "artwork_url": item.get("artwork") or item.get("artworkUrl600") or item.get("image") or "",
                            "description": item.get("description") or "",
                            "source": "4thepeople.live",
                        }
                    )
                if results:
                    return results
            except Exception as exc:
                log.debug("4thepeople endpoint %s skipped: %s", url, exc)
    return []


async def search_itunes(query: str) -> list[dict[str, Any]]:
    url = f"https://itunes.apple.com/search?media=podcast&entity=podcast&limit=25&term={quote_plus(query)}"
    async with httpx.AsyncClient(
        timeout=10.0,
        follow_redirects=True,
        headers={"User-Agent": USER_AGENT},
    ) as client:
        resp = await client.get(url)
        resp.raise_for_status()
        data = resp.json()

    results = []
    for item in data.get("results", []):
        feed_url = item.get("feedUrl")
        if not feed_url:
            continue
        results.append(
            {
                "title": item.get("collectionName") or item.get("trackName") or "Untitled",
                "author": item.get("artistName") or "",
                "feed_url": feed_url,
                "artwork_url": item.get("artworkUrl600") or item.get("artworkUrl100") or "",
                "description": item.get("primaryGenreName") or "",
                "source": "Apple iTunes",
            }
        )
    return results


async def ensure_cover_art(show_folder: Path, artwork_url: str) -> None:
    if not artwork_url:
        return
    cover_path = show_folder / "folder.jpg"
    if cover_path.exists():
        return
    try:
        show_folder.mkdir(parents=True, exist_ok=True)
        tmp_path = show_folder / "folder.jpg.part"
        async with httpx.AsyncClient(
            timeout=20.0,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            resp = await client.get(artwork_url)
            resp.raise_for_status()
            tmp_path.write_bytes(resp.content)
            os.replace(tmp_path, cover_path)
    except Exception as exc:
        log.warning("Failed to save cover art for %s: %s", show_folder, exc)


def write_episode_nfo(audio_path: Path, podcast_title: str, episode: dict[str, Any]) -> None:
    nfo_path = audio_path.with_suffix(".nfo")
    if nfo_path.exists():
        return
    root = ET.Element("episodedetails")
    ET.SubElement(root, "title").text = episode.get("title", "")
    ET.SubElement(root, "showtitle").text = podcast_title
    ET.SubElement(root, "aired").text = episode.get("published_at", "")
    ET.SubElement(root, "plot").text = episode.get("description", "")
    tree = ET.ElementTree(root)
    ET.indent(tree, space="  ")
    tree.write(nfo_path, encoding="utf-8", xml_declaration=True)


async def stream_episode_to_disk(
    *,
    audio_url: str,
    podcast_title: str,
    podcast_artwork: str,
    episode_meta: dict[str, Any],
    episode_id: int | None = None,
) -> dict[str, Any]:
    settings = load_settings()
    ext = guess_extension(audio_url)
    target_path = build_episode_target_path(
        settings=settings,
        podcast_title=podcast_title,
        episode_title=episode_meta.get("title", "Episode"),
        published_ts=int(episode_meta.get("published_ts") or 0),
        ext=ext,
    )
    target_path.parent.mkdir(parents=True, exist_ok=True)
    tmp_path = target_path.with_name(f".{target_path.name}.part")

    download_progress[audio_url] = {
        "episode_id": episode_id,
        "audio_url": audio_url,
        "status": "downloading",
        "downloaded_bytes": 0,
        "total_bytes": 0,
        "percent": 0,
        "file_path": "",
        "file_size": 0,
        "error_msg": "",
        "updated_at": time.time(),
    }

    timeout = float(settings.get("download_timeout_seconds", 300))
    try:
        async with httpx.AsyncClient(
            timeout=timeout,
            follow_redirects=True,
            headers={"User-Agent": USER_AGENT},
        ) as client:
            async with client.stream("GET", audio_url) as resp:
                resp.raise_for_status()
                total_bytes = int(resp.headers.get("content-length") or 0)
                downloaded_bytes = 0
                download_progress[audio_url]["total_bytes"] = total_bytes

                with open(tmp_path, "wb") as f:
                    async for chunk in resp.aiter_bytes(chunk_size=65536):
                        f.write(chunk)
                        downloaded_bytes += len(chunk)
                        pct = (
                            min(99, int((downloaded_bytes * 100) / total_bytes))
                            if total_bytes > 0
                            else None
                        )
                        download_progress[audio_url].update(
                            {
                                "downloaded_bytes": downloaded_bytes,
                                "total_bytes": total_bytes,
                                "percent": pct,
                                "updated_at": time.time(),
                            }
                        )

        os.replace(tmp_path, target_path)
        size = target_path.stat().st_size

        if settings.get("save_cover_art", True):
            await ensure_cover_art(target_path.parent, podcast_artwork)
        if settings.get("write_nfo", False):
            write_episode_nfo(target_path, podcast_title, episode_meta)

        rel_path = str(target_path.relative_to(PODCASTS_DIR))
        with db_lock:
            conn = get_db()
            try:
                if episode_id is not None:
                    conn.execute(
                        """
                        UPDATE episodes
                        SET status = 'downloaded', file_path = ?, file_size = ?, error_msg = ''
                        WHERE id = ?
                        """,
                        (rel_path, size, episode_id),
                    )
                else:
                    conn.execute(
                        """
                        UPDATE episodes
                        SET status = 'downloaded', file_path = ?, file_size = ?, error_msg = ''
                        WHERE audio_url = ?
                        """,
                        (rel_path, size, audio_url),
                    )
                conn.commit()
            finally:
                conn.close()

        download_progress[audio_url] = {
            "episode_id": episode_id,
            "audio_url": audio_url,
            "status": "downloaded",
            "downloaded_bytes": size,
            "total_bytes": size,
            "percent": 100,
            "file_path": rel_path,
            "file_size": size,
            "error_msg": "",
            "updated_at": time.time(),
        }
        log.info("Downloaded: %s -> %s", episode_meta.get("title"), rel_path)
        return {
            "id": episode_id,
            "audio_url": audio_url,
            "status": "downloaded",
            "file_path": rel_path,
            "file_size": size,
        }

    except Exception as exc:
        if tmp_path.exists():
            tmp_path.unlink(missing_ok=True)
        err = str(exc)[:300]
        if episode_id is not None:
            with db_lock:
                conn = get_db()
                try:
                    conn.execute(
                        "UPDATE episodes SET status = 'failed', error_msg = ? WHERE id = ?",
                        (err, episode_id),
                    )
                    conn.commit()
                finally:
                    conn.close()
        download_progress[audio_url] = {
            "episode_id": episode_id,
            "audio_url": audio_url,
            "status": "failed",
            "downloaded_bytes": 0,
            "total_bytes": 0,
            "percent": 0,
            "file_path": "",
            "file_size": 0,
            "error_msg": err,
            "updated_at": time.time(),
        }
        log.error("Download failed for %s: %s", episode_meta.get("title"), err)
        return {
            "id": episode_id,
            "audio_url": audio_url,
            "status": "failed",
            "error_msg": err,
        }


async def download_episode_by_id(episode_id: int) -> dict[str, Any]:
    with db_lock:
        conn = get_db()
        try:
            row = conn.execute(
                """
                SELECT e.*, p.title AS podcast_title, p.artwork_url AS podcast_artwork
                FROM episodes e
                JOIN podcasts p ON p.id = e.podcast_id
                WHERE e.id = ?
                """,
                (episode_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Episode not found")
            ep = dict(row)
            conn.execute(
                "UPDATE episodes SET status = 'downloading', error_msg = '' WHERE id = ?",
                (episode_id,),
            )
            conn.commit()
        finally:
            conn.close()

    return await stream_episode_to_disk(
        audio_url=ep["audio_url"],
        podcast_title=ep["podcast_title"],
        podcast_artwork=ep["podcast_artwork"],
        episode_meta=ep,
        episode_id=episode_id,
    )


def remove_episode_file(rel_path: str) -> None:
    if not rel_path:
        return
    full_path = (PODCASTS_DIR / rel_path).resolve()
    if full_path.is_relative_to(PODCASTS_DIR.resolve()) and full_path.is_file():
        full_path.unlink(missing_ok=True)
        nfo = full_path.with_suffix(".nfo")
        if nfo.is_file():
            nfo.unlink(missing_ok=True)


def run_retention_cleanup() -> dict[str, int]:
    settings = load_settings()
    retention_days = int(settings.get("retention_days") or 0)
    global_max = int(settings.get("max_episodes_per_show") or 0)
    now_ts = int(time.time())
    deleted_count = 0

    with db_lock:
        conn = get_db()
        try:
            podcasts = conn.execute("SELECT id, keep_latest FROM podcasts").fetchall()
            for pod in podcasts:
                pod_id = pod["id"]
                keep_limit = int(pod["keep_latest"] or 0) or global_max
                downloaded = conn.execute(
                    """
                    SELECT id, published_ts, file_path
                    FROM episodes
                    WHERE podcast_id = ? AND status = 'downloaded'
                    ORDER BY published_ts DESC, id DESC
                    """,
                    (pod_id,),
                ).fetchall()

                for idx, ep in enumerate(downloaded):
                    too_many = keep_limit > 0 and idx >= keep_limit
                    too_old = (
                        retention_days > 0
                        and ep["published_ts"] > 0
                        and (now_ts - ep["published_ts"]) > retention_days * 86400
                    )
                    if too_many or too_old:
                        remove_episode_file(ep["file_path"])
                        conn.execute(
                            "UPDATE episodes SET status = 'deleted', file_path = '', file_size = 0 WHERE id = ?",
                            (ep["id"],),
                        )
                        deleted_count += 1
            conn.commit()
        finally:
            conn.close()

    return {"deleted_episodes": deleted_count}


async def sync_single_podcast(podcast_id: int, is_initial: bool = False) -> int:
    settings = load_settings()
    with db_lock:
        conn = get_db()
        try:
            pod = conn.execute("SELECT * FROM podcasts WHERE id = ?", (podcast_id,)).fetchone()
            if not pod:
                return 0
            pod_dict = dict(pod)
        finally:
            conn.close()

    feed_data = await fetch_and_parse_feed(pod_dict["feed_url"])
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
    to_download: list[int] = []

    with db_lock:
        conn = get_db()
        try:
            conn.execute(
                """
                UPDATE podcasts
                SET last_checked = ?,
                    artwork_url = CASE WHEN artwork_url = '' THEN ? ELSE artwork_url END
                WHERE id = ?
                """,
                (now_iso, feed_data["artwork_url"], podcast_id),
            )
            inserted_ids: list[int] = []
            for ep in feed_data["episodes"]:
                cur = conn.execute(
                    """
                    INSERT OR IGNORE INTO episodes
                    (podcast_id, guid, title, description, published_at, published_ts, audio_url, duration, status)
                    VALUES (?, ?, ?, ?, ?, ?, ?, ?, 'new')
                    """,
                    (
                        podcast_id,
                        ep["guid"],
                        ep["title"],
                        ep["description"],
                        ep["published_at"],
                        ep["published_ts"],
                        ep["audio_url"],
                        ep["duration"],
                    ),
                )
                if cur.rowcount > 0 and cur.lastrowid:
                    inserted_ids.append(cur.lastrowid)
            conn.commit()

            if is_initial:
                init_count = int(settings.get("initial_episodes") or 0)
                if init_count > 0:
                    rows = conn.execute(
                        """
                        SELECT id FROM episodes
                        WHERE podcast_id = ? AND status = 'new'
                        ORDER BY published_ts DESC, id DESC
                        LIMIT ?
                        """,
                        (podcast_id, init_count),
                    ).fetchall()
                    to_download = [r["id"] for r in rows]
            elif settings.get("auto_download", True) and pod_dict["auto_download"]:
                to_download = inserted_ids
        finally:
            conn.close()

    for ep_id in to_download:
        await download_episode_by_id(ep_id)

    return len(to_download)


async def poll_all_podcasts() -> None:
    global last_poll_epoch
    if poll_lock.locked():
        return
    async with poll_lock:
        poll_status["running"] = True
        poll_status["last_message"] = "Checking feeds..."
        downloaded_total = 0
        try:
            with db_lock:
                conn = get_db()
                try:
                    pod_ids = [r["id"] for r in conn.execute("SELECT id FROM podcasts").fetchall()]
                finally:
                    conn.close()

            for pid in pod_ids:
                try:
                    downloaded_total += await sync_single_podcast(pid, is_initial=False)
                except Exception as exc:
                    log.error("Error syncing podcast %s: %s", pid, exc)

            cleanup = run_retention_cleanup()
            last_poll_epoch = time.time()
            poll_status["last_run"] = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")
            poll_status["last_message"] = (
                f"Checked {len(pod_ids)} shows · Downloaded {downloaded_total} new · Cleaned {cleanup['deleted_episodes']}"
            )
        finally:
            poll_status["running"] = False


async def scheduler_loop() -> None:
    global last_poll_epoch
    last_poll_epoch = time.time()
    while True:
        await asyncio.sleep(20)
        settings = load_settings()
        interval_sec = max(1, int(settings.get("poll_interval_minutes") or 60)) * 60
        if time.time() - last_poll_epoch >= interval_sec:
            await poll_all_podcasts()


@asynccontextmanager
async def lifespan(app: FastAPI):
    PODCASTS_DIR.mkdir(parents=True, exist_ok=True)
    init_db()
    task = asyncio.create_task(scheduler_loop())
    yield
    task.cancel()


app = FastAPI(title="PodHarvest", lifespan=lifespan)


class SubscribeRequest(BaseModel):
    feed_url: str
    title: str = ""
    author: str = ""
    description: str = ""
    artwork_url: str = ""


class PodcastUpdateRequest(BaseModel):
    auto_download: bool | None = None
    keep_latest: int | None = None


class PreviewDownloadRequest(BaseModel):
    podcast_title: str
    podcast_artwork: str = ""
    feed_url: str = ""
    guid: str = ""
    title: str
    description: str = ""
    published_at: str = ""
    published_ts: int = 0
    audio_url: str
    duration: str = ""


@app.get("/api/status")
async def api_status():
    return poll_status


@app.get("/api/downloads/progress")
async def api_downloads_progress():
    now = time.time()
    # Prune completed/failed entries older than 10 minutes
    stale = [
        k
        for k, v in download_progress.items()
        if v.get("status") != "downloading" and (now - float(v.get("updated_at") or 0)) > 600
    ]
    for k in stale:
        download_progress.pop(k, None)
    return download_progress


@app.post("/api/poll")
async def api_trigger_poll():
    asyncio.create_task(poll_all_podcasts())
    return {"ok": True, "message": "Feed check started"}


@app.get("/api/search")
async def api_search(q: str = Query(..., min_length=1)):
    query = q.strip()
    subscribed_set = get_subscribed_feed_urls()

    def attach_sub_flag(items: list[dict[str, Any]]) -> list[dict[str, Any]]:
        for item in items:
            item["subscribed"] = normalize_feed_url(item.get("feed_url", "")) in subscribed_set
        return items

    if query.startswith(("http://", "https://")):
        feed = await fetch_and_parse_feed(query)
        return {
            "source": "Direct RSS",
            "results": attach_sub_flag(
                [
                    {
                        "title": feed["title"],
                        "author": feed["author"],
                        "feed_url": feed["feed_url"],
                        "artwork_url": feed["artwork_url"],
                        "description": feed["description"],
                        "source": "Direct RSS",
                    }
                ]
            ),
        }

    settings = load_settings()
    if settings.get("enable_primary_index", True):
        primary_url = settings.get("primary_index_url", "https://podcast.4thepeople.live")
        primary_results = await search_4thepeople(query, primary_url)
        if primary_results:
            return {"source": "4thepeople.live", "results": attach_sub_flag(primary_results)}

    if settings.get("enable_itunes_fallback", True):
        itunes_results = await search_itunes(query)
        return {"source": "Apple iTunes", "results": attach_sub_flag(itunes_results)}

    return {"source": "None", "results": []}


@app.get("/api/preview")
async def api_preview(feed_url: str = Query(...)):
    feed = await fetch_and_parse_feed(feed_url)
    subscribed_set = get_subscribed_feed_urls()
    settings = load_settings()

    preview_eps = feed["episodes"][:25]
    audio_urls = [ep["audio_url"] for ep in preview_eps]

    db_by_url: dict[str, dict[str, Any]] = {}
    if audio_urls:
        with db_lock:
            conn = get_db()
            try:
                placeholders = ",".join("?" for _ in audio_urls)
                rows = conn.execute(
                    f"SELECT id, audio_url, status, file_path, file_size FROM episodes WHERE audio_url IN ({placeholders})",
                    audio_urls,
                ).fetchall()
                for r in rows:
                    db_by_url[r["audio_url"]] = dict(r)
            finally:
                conn.close()

    enriched = []
    for ep in preview_eps:
        aurl = ep["audio_url"]
        db_row = db_by_url.get(aurl)
        prog = download_progress.get(aurl)

        status = "new"
        file_path = ""
        file_size = 0
        ep_id = db_row["id"] if db_row else None

        if prog and prog.get("status") == "downloading":
            status = "downloading"
        elif db_row and db_row["status"] == "downloaded" and db_row["file_path"]:
            candidate = (PODCASTS_DIR / db_row["file_path"]).resolve()
            if candidate.is_relative_to(PODCASTS_DIR.resolve()) and candidate.is_file():
                status = "downloaded"
                file_path = db_row["file_path"]
                file_size = candidate.stat().st_size

        if status == "new":
            try:
                ext = guess_extension(aurl, ep.get("mime_type", ""))
                target = build_episode_target_path(
                    settings=settings,
                    podcast_title=feed["title"],
                    episode_title=ep["title"],
                    published_ts=ep["published_ts"],
                    ext=ext,
                )
                if target.is_file():
                    status = "downloaded"
                    file_path = str(target.relative_to(PODCASTS_DIR))
                    file_size = target.stat().st_size
            except Exception:
                pass

        enriched.append(
            {
                **ep,
                "episode_id": ep_id,
                "status": status,
                "file_path": file_path,
                "file_size": file_size,
            }
        )

    return {
        "title": feed["title"],
        "author": feed["author"],
        "description": feed["description"],
        "artwork_url": feed["artwork_url"],
        "feed_url": feed["feed_url"],
        "subscribed": normalize_feed_url(feed["feed_url"]) in subscribed_set,
        "episodes": enriched,
    }


@app.post("/api/preview/download")
async def api_preview_download(req: PreviewDownloadRequest):
    aurl = req.audio_url.strip()
    existing_prog = download_progress.get(aurl)
    if existing_prog and existing_prog.get("status") == "downloading":
        return {"ok": True, "status": "downloading", "audio_url": aurl}

    # Check if this episode already belongs to a subscribed podcast in DB
    episode_id: int | None = None
    with db_lock:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT id FROM episodes WHERE audio_url = ? LIMIT 1", (aurl,)
            ).fetchone()
            if row:
                episode_id = row["id"]
                conn.execute(
                    "UPDATE episodes SET status = 'downloading', error_msg = '' WHERE id = ?",
                    (episode_id,),
                )
                conn.commit()
        finally:
            conn.close()

    download_progress[aurl] = {
        "episode_id": episode_id,
        "audio_url": aurl,
        "status": "downloading",
        "downloaded_bytes": 0,
        "total_bytes": 0,
        "percent": 0,
        "file_path": "",
        "file_size": 0,
        "error_msg": "",
        "updated_at": time.time(),
    }

    episode_meta = {
        "guid": req.guid or aurl,
        "title": req.title,
        "description": req.description,
        "published_at": req.published_at,
        "published_ts": req.published_ts,
        "audio_url": aurl,
        "duration": req.duration,
    }
    asyncio.create_task(
        stream_episode_to_disk(
            audio_url=aurl,
            podcast_title=req.podcast_title,
            podcast_artwork=req.podcast_artwork,
            episode_meta=episode_meta,
            episode_id=episode_id,
        )
    )
    return {"ok": True, "status": "downloading", "audio_url": aurl}


@app.get("/api/podcasts")
async def api_list_podcasts():
    with db_lock:
        conn = get_db()
        try:
            rows = conn.execute(
                """
                SELECT p.*,
                    COUNT(e.id) AS total_episodes,
                    SUM(CASE WHEN e.status = 'downloaded' THEN 1 ELSE 0 END) AS downloaded_episodes,
                    COALESCE(SUM(CASE WHEN e.status = 'downloaded' THEN e.file_size ELSE 0 END), 0) AS total_bytes
                FROM podcasts p
                LEFT JOIN episodes e ON e.podcast_id = p.id
                GROUP BY p.id
                ORDER BY p.title COLLATE NOCASE ASC
                """
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


@app.post("/api/podcasts")
async def api_subscribe(req: SubscribeRequest):
    raw_feed_url = req.feed_url.strip()
    feed = await fetch_and_parse_feed(raw_feed_url)
    title = req.title.strip() or feed["title"]
    author = req.author.strip() or feed["author"]
    description = req.description.strip() or feed["description"]
    artwork_url = req.artwork_url.strip() or feed["artwork_url"]
    folder_name = sanitize_segment(title, "Podcast")
    now_iso = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M UTC")

    with db_lock:
        conn = get_db()
        try:
            existing = conn.execute(
                "SELECT id FROM podcasts WHERE LOWER(RTRIM(feed_url, '/')) = ?",
                (normalize_feed_url(raw_feed_url),),
            ).fetchone()
            if existing:
                raise HTTPException(status_code=409, detail="Already subscribed to this feed")
            cur = conn.execute(
                """
                INSERT INTO podcasts (title, author, description, feed_url, artwork_url, folder_name, created_at)
                VALUES (?, ?, ?, ?, ?, ?, ?)
                """,
                (title, author, description, raw_feed_url, artwork_url, folder_name, now_iso),
            )
            podcast_id = cur.lastrowid
            conn.commit()
        finally:
            conn.close()

    asyncio.create_task(sync_single_podcast(podcast_id, is_initial=True))
    return {"id": podcast_id, "title": title, "feed_url": raw_feed_url}


@app.patch("/api/podcasts/{podcast_id}")
async def api_update_podcast(podcast_id: int, req: PodcastUpdateRequest):
    with db_lock:
        conn = get_db()
        try:
            pod = conn.execute("SELECT * FROM podcasts WHERE id = ?", (podcast_id,)).fetchone()
            if not pod:
                raise HTTPException(status_code=404, detail="Podcast not found")
            if req.auto_download is not None:
                conn.execute(
                    "UPDATE podcasts SET auto_download = ? WHERE id = ?",
                    (1 if req.auto_download else 0, podcast_id),
                )
            if req.keep_latest is not None:
                conn.execute(
                    "UPDATE podcasts SET keep_latest = ? WHERE id = ?",
                    (max(0, req.keep_latest), podcast_id),
                )
            conn.commit()
        finally:
            conn.close()
    return {"ok": True}


@app.post("/api/podcasts/{podcast_id}/sync")
async def api_sync_podcast(podcast_id: int):
    count = await sync_single_podcast(podcast_id, is_initial=False)
    return {"ok": True, "downloaded": count}


@app.delete("/api/podcasts/{podcast_id}")
async def api_unsubscribe(podcast_id: int, delete_files: bool = Query(False)):
    with db_lock:
        conn = get_db()
        try:
            pod = conn.execute("SELECT * FROM podcasts WHERE id = ?", (podcast_id,)).fetchone()
            if not pod:
                raise HTTPException(status_code=404, detail="Podcast not found")
            if delete_files:
                eps = conn.execute(
                    "SELECT file_path FROM episodes WHERE podcast_id = ? AND file_path != ''",
                    (podcast_id,),
                ).fetchall()
                for ep in eps:
                    remove_episode_file(ep["file_path"])
                show_dir = (PODCASTS_DIR / pod["folder_name"]).resolve()
                if show_dir.is_relative_to(PODCASTS_DIR.resolve()) and show_dir.is_dir():
                    cover = show_dir / "folder.jpg"
                    if cover.is_file():
                        cover.unlink(missing_ok=True)
                    try:
                        show_dir.rmdir()
                    except OSError:
                        pass
            conn.execute("DELETE FROM podcasts WHERE id = ?", (podcast_id,))
            conn.commit()
        finally:
            conn.close()
    return {"ok": True}


@app.get("/api/podcasts/{podcast_id}/episodes")
async def api_list_episodes(podcast_id: int):
    with db_lock:
        conn = get_db()
        try:
            rows = conn.execute(
                """
                SELECT * FROM episodes
                WHERE podcast_id = ?
                ORDER BY published_ts DESC, id DESC
                """,
                (podcast_id,),
            ).fetchall()
            return [dict(r) for r in rows]
        finally:
            conn.close()


@app.post("/api/episodes/{episode_id}/download")
async def api_download_episode(episode_id: int):
    with db_lock:
        conn = get_db()
        try:
            row = conn.execute(
                "SELECT id, audio_url FROM episodes WHERE id = ?",
                (episode_id,),
            ).fetchone()
            if not row:
                raise HTTPException(status_code=404, detail="Episode not found")
            aurl = row["audio_url"]
            existing_prog = download_progress.get(aurl)
            if existing_prog and existing_prog.get("status") == "downloading":
                return {"id": episode_id, "audio_url": aurl, "status": "downloading"}

            conn.execute(
                "UPDATE episodes SET status = 'downloading', error_msg = '' WHERE id = ?",
                (episode_id,),
            )
            conn.commit()
        finally:
            conn.close()

    download_progress[aurl] = {
        "episode_id": episode_id,
        "audio_url": aurl,
        "status": "downloading",
        "downloaded_bytes": 0,
        "total_bytes": 0,
        "percent": 0,
        "file_path": "",
        "file_size": 0,
        "error_msg": "",
        "updated_at": time.time(),
    }
    asyncio.create_task(download_episode_by_id(episode_id))
    return {"id": episode_id, "audio_url": aurl, "status": "downloading"}


@app.delete("/api/episodes/{episode_id}/file")
async def api_delete_episode_file(episode_id: int):
    with db_lock:
        conn = get_db()
        try:
            ep = conn.execute("SELECT file_path, audio_url FROM episodes WHERE id = ?", (episode_id,)).fetchone()
            if not ep:
                raise HTTPException(status_code=404, detail="Episode not found")
            remove_episode_file(ep["file_path"])
            download_progress.pop(ep["audio_url"], None)
            conn.execute(
                "UPDATE episodes SET status = 'deleted', file_path = '', file_size = 0 WHERE id = ?",
                (episode_id,),
            )
            conn.commit()
        finally:
            conn.close()
    return {"ok": True}


@app.get("/api/files")
async def api_list_files():
    PODCASTS_DIR.mkdir(parents=True, exist_ok=True)
    base_resolved = PODCASTS_DIR.resolve()
    files = []
    total_bytes = 0
    for path in sorted(base_resolved.rglob("*")):
        if path.is_file() and not path.name.startswith("."):
            stat = path.stat()
            total_bytes += stat.st_size
            files.append(
                {
                    "rel_path": str(path.relative_to(base_resolved)),
                    "name": path.name,
                    "folder": str(path.parent.relative_to(base_resolved)),
                    "size": stat.st_size,
                    "modified": datetime.fromtimestamp(stat.st_mtime, tz=timezone.utc).strftime("%Y-%m-%d %H:%M"),
                }
            )
    return {
        "root": str(PODCASTS_DIR),
        "total_bytes": total_bytes,
        "file_count": len(files),
        "files": files,
    }


@app.delete("/api/files")
async def api_delete_raw_file(rel_path: str = Query(...)):
    target = (PODCASTS_DIR / rel_path).resolve()
    base = PODCASTS_DIR.resolve()
    if not target.is_relative_to(base) or not target.is_file():
        raise HTTPException(status_code=404, detail="File not found")
    target.unlink(missing_ok=True)
    nfo = target.with_suffix(".nfo")
    if nfo.is_file():
        nfo.unlink(missing_ok=True)

    for k, v in list(download_progress.items()):
        if v.get("file_path") == rel_path:
            download_progress.pop(k, None)

    with db_lock:
        conn = get_db()
        try:
            conn.execute(
                "UPDATE episodes SET status = 'deleted', file_path = '', file_size = 0 WHERE file_path = ?",
                (rel_path,),
            )
            conn.commit()
        finally:
            conn.close()
    return {"ok": True}


@app.post("/api/cleanup")
async def api_cleanup():
    return run_retention_cleanup()


@app.get("/api/settings")
async def api_get_settings():
    return load_settings()


@app.put("/api/settings")
async def api_put_settings(updates: dict[str, Any]):
    return save_settings(updates)


@app.get("/api/opml/export")
async def api_export_opml():
    with db_lock:
        conn = get_db()
        try:
            pods = conn.execute("SELECT title, feed_url FROM podcasts ORDER BY title").fetchall()
        finally:
            conn.close()

    opml = ET.Element("opml", version="2.0")
    head = ET.SubElement(opml, "head")
    ET.SubElement(head, "title").text = "PodHarvest Subscriptions"
    body = ET.SubElement(opml, "body")
    for p in pods:
        ET.SubElement(
            body,
            "outline",
            type="rss",
            text=p["title"],
            title=p["title"],
            xmlUrl=p["feed_url"],
        )
    xml_bytes = ET.tostring(opml, encoding="utf-8", xml_declaration=True)
    return PlainTextResponse(
        content=xml_bytes.decode("utf-8"),
        media_type="text/x-opml",
        headers={"Content-Disposition": 'attachment; filename="podharvest.opml"'},
    )


@app.post("/api/opml/import")
async def api_import_opml(file: UploadFile = File(...)):
    raw = await file.read()
    root = ET.fromstring(raw)
    added = 0
    for outline in root.findall(".//outline"):
        feed_url = outline.attrib.get("xmlUrl") or outline.attrib.get("url")
        if not feed_url:
            continue
        title = outline.attrib.get("title") or outline.attrib.get("text") or feed_url
        try:
            await api_subscribe(SubscribeRequest(feed_url=feed_url, title=title))
            added += 1
        except HTTPException:
            pass
    return {"ok": True, "imported": added}


app.mount("/static", StaticFiles(directory=STATIC_DIR), name="static")


@app.get("/")
async def index():
    return FileResponse(STATIC_DIR / "index.html")
