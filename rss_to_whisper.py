import os
import time
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import whisper
from whisper.utils import WriteTXT, WriteJSON, WriteTSV, WriteSRT
from collections import namedtuple

model_name = "tiny.en"
model = whisper.load_model(model_name)


def get_feed(url):
    _feed_response = requests.get(url)
    if _feed_response.ok:
        _feed = feedparser.parse(_feed_response.text)
        return _feed
    else:
        print(f"Feed failed to load {_feed_response.status_code}")
        return None


def create_pod_path(title):
    _pod_path = Path.home() / "rss_to_whisper" / title

    if not _pod_path.exists():
        _pod_path.mkdir(parents=True)

    return _pod_path


def create_episode_path(_pod_path, _episode_id):
    _episode_path = _pod_path / _episode_id

    if not _episode_path.exists():
        _episode_path.mkdir(parents=True)

    return _episode_path


def get_mp3_link(_pod_links):
    for _link in _pod_links:
        if _link.type == "audio/mpeg":
            return _link

    return None


def get_mp3_info(_pod_links, _episode_path):
    for _link in _pod_links:
        if _link.type == "audio/mpeg":
            MP3 = namedtuple("MP3", ["link", "file_name", "file_path", "length"])

            _href = _link.href
            _parsed_url = urlparse(_href)
            _filename = os.path.basename(_parsed_url.path)

            return MP3(link=_href, file_name=_filename, file_path=_episode_path / _filename, length=int(_link.length))

    return None


def get_file_part(_url):
    _parsed_url = urlparse(_url)
    _path = _parsed_url.path
    _filename = os.path.basename(_path)

    return _filename


def download_file_if_required(_mp3_info):
    if not mp3_info.file_path.exists() or os.path.getsize(mp3_info.file_path) != mp3_info.length:
        print(f"Downloading... {_mp3_info.file_name}")
        _file_response = requests.get(_mp3_info.link)
        if _file_response.ok:
            print(f"Writing... {_mp3_info.file_path}")
            with open(_mp3_info.file_path, 'wb') as _f:
                _f.write(_file_response.content)
        else:
            print(f"error saving file response: {_file_response.status_code}")
    else:
        print(f"{_mp3_info.file_name} is already downloaded")


def write_transcripts(_result, _file_name, _episode_path):
    print("\nWriting transcriptions...")
    writer = WriteTXT(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.txt")

    writer = WriteJSON(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.json")

    writer = WriteTSV(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.tsv")

    writer = WriteSRT(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.srt")

    Path(_episode_path / "transcribed").touch()


def transcribe_if_required(_mp3_info, _episode_path):
    if not os.path.exists(episode_path / "transcribed"):
        start = time.time()
        result = model.transcribe(audio=str(_mp3_info.file_path), language="en", verbose=True)
        write_transcripts(result, _mp3_info.file_name, _episode_path)
        end = time.time()
        elapsed = float(end - start)
        elapsed_minutes = str(round(elapsed / 60, 2))
        print(f"\nProcessed {_mp3_info.file_name} With model: {model_name} in: {elapsed_minutes} Minutes")
    else:
        print(f"{_mp3_info.file_name} is already transcribed. Skipping.")


feed = get_feed('http://feeds.libsyn.com/60664')
if feed:
    pod_path = create_pod_path(feed.feed.title)

    for entry in feed.entries:
        episode_path = create_episode_path(pod_path, entry.id)
        mp3_info = get_mp3_info(entry.links, episode_path)

        download_file_if_required(mp3_info)
        transcribe_if_required(mp3_info, episode_path)
