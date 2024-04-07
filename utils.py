import hashlib
import os
import re
import uuid
from urllib.parse import urlparse


def is_valid_uuid(uuid_str: str):
    try:
        uuid.UUID(uuid_str)
        return True
    except ValueError:
        return False


def get_partial_guid(_uuid_str: str, _content: str):
    if _uuid_str and is_valid_uuid(_uuid_str):
        return _uuid_str[0:8]
    elif _content:
        digest = get_hash(_content)
        return digest[0:8]
    else:
        return "noguid"


def get_hash(_content: str) -> str:
    ha = hashlib.md5()
    ha.update(_content.encode("utf-8"))
    digest = ha.hexdigest()
    return str(digest)


def is_writable(path):
    return os.access(path, os.W_OK)


def escape_filename(_filename: str):
    _escaped = "".join([x if x.isalnum() else "-" for x in _filename])
    _escaped = re.sub(r'-{2,}', '-', _escaped)
    _escaped = _escaped[:-1] if _escaped.endswith('-') else _escaped

    return _escaped


def time_to_seconds(time_str: str):
    # https://stackoverflow.com/a/6402934
    return sum(float(x) * 60 ** i for i, x in enumerate(reversed(time_str.split(':'))))


def get_file_part(_url: str):
    _parsed_url = urlparse(_url)
    _path = _parsed_url.path
    _filename = os.path.basename(_path)

    return _filename


def create_path(_parent_path, _directory_name: str):
    if not _parent_path or _directory_name:
        return None

    _path_to_create = _parent_path / escape_filename(_directory_name)

    if not _path_to_create.exists():
        _path_to_create.mkdir(parents=True)

    return _path_to_create


def default_feeds():
    return [
        # "http://feeds.libsyn.com/60664",  # Ask a spaceman
        # "https://omny.fm/shows/daniel-and-jorge-explain-the-universe/playlists/podcast.rss",
        # "https://podcasts.files.bbci.co.uk/b00snr0w.rss",  # Infinite monkey cage
        # "https://thecosmicsavannah.com/feed/podcast/",
        # "https://audioboom.com/channels/5014098.rss",  # Supermassive podcast
        # "https://omny.fm/shows/planetary-radio-space-exploration-astronomy-and-sc/playlists/podcast.rss",
        # "https://www.nasa.gov/feeds/podcasts/curious-universe",
        "https://www.nasa.gov/feeds/podcasts/gravity-assist",
        # "https://rss.art19.com/sean-carrolls-mindscape",
        # "http://titaniumphysics.libsyn.com/rss",
        # "https://www.spreaker.com/show/2458531/episodes/feed",  # Spacetime pod
        # "https://www.abc.net.au/feeds/8294152/podcast.xml",  # Cosmic vertigo
        # "https://astronomycast.libsyn.com/rss",
        # "https://feed.podbean.com/conversationsattheperimeter/feed.xml",
        # "https://feeds.fireside.fm/universetoday/rss",
        # "https://feeds.soundcloud.com/users/soundcloud:users:210527670/sounds.rss"  # Interplanetary
    ]