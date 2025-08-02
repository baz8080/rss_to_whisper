import hashlib
import logging
import os
import re
import sys
import time
import uuid
from pathlib import Path
from typing import List, TypeVar, Generator

logger = logging.getLogger(__name__)


T = TypeVar("T")


def is_valid_uuid(uuid_str: str) -> bool:
    try:
        uuid.UUID(uuid_str)
        return True
    except ValueError:
        return False


def get_hash(_content: str) -> str:
    ha = hashlib.md5()
    ha.update(_content.encode("utf-8"))
    digest = ha.hexdigest()
    return str(digest)


def is_writable(path: str | Path) -> bool:
    return os.access(path, os.W_OK)


def escape_filename(_filename: str | None) -> str:
    _escaped: str = ""

    if _filename:
        _escaped = "".join([x if x.isalnum() else "-" for x in _filename])
        _escaped = re.sub(r"-{2,}", "-", _escaped)
        _escaped = _escaped[:-1] if _escaped.endswith("-") else _escaped

    return _escaped


def time_to_seconds(time_str: str | None) -> int:
    # https://stackoverflow.com/a/6402934
    _seconds = 0

    if time_str:
        try:
            _seconds = sum(
                float(x) * 60**i for i, x in enumerate(reversed(time_str.split(":")))
            )
        except ValueError:
            logger.error(f"Couldn't parse {_seconds} into seconds")

    return _seconds


def create_path(_parent_path: str | Path, _directory_name: str):
    if not _parent_path or not _directory_name:
        return None

    if isinstance(_parent_path, str):
        _parent_path = Path(_parent_path)

    _path_to_create = _parent_path / escape_filename(_directory_name)

    if not _path_to_create.exists():
        _path_to_create.mkdir(parents=True)

    return _path_to_create


def chunk(_list: List[T], size: int) -> Generator[List[T], None, None]:
    if size <= 0:
        raise ValueError("chunk size must be a positive integer")

    for i in range(0, len(_list), size):
        yield _list[i : i + size]


def get_episode_dict(
    podcast_metadata,
    episode_data,
    transcript: str,
    collections: list,
    relative_mp3_path,
):
    episode_dict = None

    _id = get_hash(transcript)

    episode_audio_link = [
        d["href"] for d in episode_data.links if d["rel"] == "enclosure"
    ]
    if episode_audio_link and len(episode_audio_link) > 0:
        episode_audio_link = episode_audio_link[0]
    else:
        logger.error("Skipping episode because it has no MP3")
        return episode_dict

    try:
        podcast_title = podcast_metadata.title
        podcast_link = getattr(podcast_metadata, "link", None)
        podcast_language = getattr(podcast_metadata, "language", None)
        podcast_copyright = getattr(podcast_metadata, "rights", None)
        podcast_author = getattr(podcast_metadata, "author", None)
        all_tags = []

        podcast_tags = getattr(podcast_metadata, "tags", None)
        if podcast_tags:
            all_tags += [d["term"] for d in podcast_tags]

        podcast_image = getattr(podcast_metadata, "image", None)
        if podcast_image:
            podcast_image = podcast_image.href

        podcast_type = getattr(podcast_metadata, "itunes_type", None)

        episode_title = episode_data.title

        episode_published_on = time.strftime("%Y-%m-%d", episode_data.published_parsed)
        episode_web_link = getattr(episode_data, "link", None)

        episode_image = getattr(episode_data, "image", None)
        if episode_image:
            episode_image = getattr(episode_image, "href", None)

        episode_summary = getattr(episode_data, "summary", None)
        episode_subtitle = getattr(episode_data, "subtitle", None)
        episode_authors = getattr(episode_data, "authors", None)
        episode_number = getattr(episode_data, "itunes_episode", None)
        episode_season = getattr(episode_data, "itunes_season", None)
        episode_type = getattr(episode_data, "itunes_episodetype", None)

        episode_keywords = getattr(episode_data, "itunes_keywords", None)
        if episode_keywords:
            all_tags += episode_keywords

        episode_duration = getattr(episode_data, "itunes_duration", None)
        if episode_duration and ":" in episode_duration:
            episode_duration = time_to_seconds(episode_duration)

        episode_tags = getattr(episode_data, "tags", None)
        if episode_tags:
            all_tags += [d["term"] for d in episode_tags]

        all_tags = [tag.lower() for tag in all_tags]
        all_tags = list(dict.fromkeys(all_tags))

        episode_dict = {
            "_id": _id,
            "_index": "podcasts",
            "podcast_collections": collections,
            "podcast_title": podcast_title,
            "podcast_link": podcast_link,
            "podcast_language": podcast_language,
            "podcast_copyright": podcast_copyright,
            "podcast_author": podcast_author,
            "podcast_image": podcast_image,
            "podcast_type": podcast_type,
            "episode_title": episode_title,
            "all_tags": all_tags,
            "episode_published_on": episode_published_on,
            "episode_audio_link": episode_audio_link,
            "episode_web_link": episode_web_link,
            "episode_image": episode_image,
            "episode_summary": episode_summary,
            "episode_subtitle": episode_subtitle,
            "episode_authors": episode_authors,
            "episode_number": episode_number,
            "episode_season": episode_season,
            "episode_type": episode_type,
            "episode_duration": episode_duration,
            "episode_transcript": transcript,
            "episode_relative_mp3_path": relative_mp3_path,
        }

        return episode_dict

    except AttributeError as e:
        logger.error("Error getting podcast metadata")
        logger.error(e)
        return None


def initialise_logging(_logger, _verbose: bool):
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter("%(asctime)s - %(levelname)s - %(message)s")
    handler.setFormatter(formatter)
    _logger.addHandler(handler)

    if _verbose:
        _logger.level = logging.DEBUG
        handler.setLevel(logging.DEBUG)
    else:
        _logger.level = logging.INFO
        handler.setLevel(logging.INFO)
