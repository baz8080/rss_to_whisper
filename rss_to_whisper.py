import argparse
import os
import re
import time
from collections import namedtuple
from pathlib import Path
from string import Template
from urllib.parse import urlparse

import feedparser
import requests
import whisper
from whisper.utils import WriteTXT, WriteJSON, WriteTSV, WriteSRT

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
    _pod_path = Path.home() / "rss_to_whisper" / escape_for_jekyll(title)

    if not _pod_path.exists():
        _pod_path.mkdir(parents=True)

    return _pod_path


def escape_for_jekyll(_filename):
    _escaped = "".join([x if x.isalnum() else "-" for x in _filename])
    _escaped = re.sub(r'-{2,}', '-', _escaped)
    _escaped = _escaped[:-1] if _escaped.endswith('-') else _escaped

    return _escaped


def create_episode_path(_pod_path, _episode_identifier):
    _episode_path = _pod_path / escape_for_jekyll(_episode_identifier)

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
            _file_name = os.path.basename(_parsed_url.path)
            _file_path = _episode_path / _file_name

            return MP3(link=_href, file_name=_file_name, file_path=_file_path, length=int(_link.length))

    return None


def get_file_part(_url):
    _parsed_url = urlparse(_url)
    _path = _parsed_url.path
    _filename = os.path.basename(_path)

    return _filename


def download_file_if_required(_mp3_info):
    if not _mp3_info.file_path.exists() or os.path.getsize(_mp3_info.file_path) != _mp3_info.length:
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
    if not os.path.exists(_episode_path / "transcribed"):
        start = time.time()
        result = model.transcribe(audio=str(_mp3_info.file_path), language="en", verbose=True)
        write_transcripts(result, _mp3_info.file_name, _episode_path)
        end = time.time()
        elapsed = float(end - start)
        elapsed_minutes = str(round(elapsed / 60, 2))
        print(f"\nProcessed {_mp3_info.file_name} With model: {model_name} in: {elapsed_minutes} Minutes")
    else:
        print(f"{_mp3_info.file_name} is already transcribed.")


def write_jekyll_post(_template, _episode_path, _file_name, _title, _published_date, _podcast_title):
    with open(_episode_path / f"{_file_name}.txt", 'r') as transcript:
        body = transcript.read().replace(".\n", ".\n\n")

    formatted_published_date = time.strftime("%Y-%m-%d", _published_date)
    processed_title = escape_for_jekyll(_title)
    processed_category = escape_for_jekyll(_podcast_title)

    date_path = formatted_published_date.replace('-', '/')
    processed_url = f"{processed_category}/{date_path}/{processed_title}.html"

    template_data = {
        'title': processed_title,
        'category': processed_category,
        'url': processed_url,
        'body': body
    }

    processed_template = _template.substitute(template_data)

    with open(_episode_path / f"{formatted_published_date}-{processed_title}.md", "w") as jekyll_post:
        jekyll_post.write(processed_template)


def main(feed_uri):
    # 'http://feeds.libsyn.com/60664' - ask a spaceman

    feed = get_feed(feed_uri)
    if feed:
        pod_path = create_pod_path(feed.feed.title)

        with open('jekyll_format.fmt', 'r') as template:
            template = Template(template.read())

        for entry in feed.entries:
            episode_path = create_episode_path(pod_path, entry.title)
            mp3_info = get_mp3_info(entry.links, episode_path)
            download_file_if_required(mp3_info)
            transcribe_if_required(mp3_info, episode_path)
            write_jekyll_post(template, episode_path, mp3_info.file_name, entry.title, entry.published_parsed,
                              feed.feed.title)


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='rss_to_whisper.py',
        description='Utils for downloading podcasts from libsyn and transcribing them',
        epilog='Have fun')

    parser.add_argument("-f", "--feed", required=True,
                        help="Provide a libsyn feed, e.g. http://feeds.libsyn.com/60664 ")
    args = parser.parse_args()

    main(args.feed)
