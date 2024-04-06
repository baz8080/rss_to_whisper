import argparse
import json
import logging
import os
import re
import sys
import time
from collections import namedtuple
from pathlib import Path
from urllib.parse import urlparse

import feedparser
import requests
import torch
import whisper
from whisper.utils import WriteTXT, WriteJSON, WriteTSV, WriteSRT

import utils
from utils import is_writable, get_partial_guid, escape_filename

logger = logging.getLogger(__name__)


def initialise_whisper(model_name: str):
    logger.info(f"Cuda available: {torch.cuda.is_available()}")
    logger.debug(f"Using {model_name} model")
    model = whisper.load_model(model_name)
    return model


def initialise_logging(verbose: bool):
    handler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)

    if verbose:
        logger.level = logging.DEBUG
        handler.setLevel(logging.DEBUG)
    else:
        logger.level = logging.INFO
        handler.setLevel(logging.INFO)


def get_feed(url: str):
    try:
        _feed_response = requests.get(url)
        if _feed_response.ok:
            _feed = feedparser.parse(_feed_response.text)
            return _feed
        else:
            logger.error(f"Feed failed to load {_feed_response.status_code}")
    except requests.exceptions.RequestException as e:
        logger.error("Failed to get feed:", e)

    return None


def create_pod_path(data_dir: str, title: str):
    if not title:
        logger.error("Missing podcast title.")
        return None

    _pod_path = Path(data_dir) / escape_filename(title)

    try:
        if not _pod_path.exists():
            _pod_path.mkdir(parents=True)
        return _pod_path
    except OSError as e:
        logger.error("Failed to make podcast directory: ", e)

    return None


def create_episode_path(_pod_path, _episode_identifier: str):
    _episode_path = _pod_path / escape_filename(_episode_identifier)

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


def get_file_part(_url: str):
    _parsed_url = urlparse(_url)
    _path = _parsed_url.path
    _filename = os.path.basename(_path)

    return _filename


def download_file_if_required(_mp3_info):
    path_exists = _mp3_info.file_path.exists()
    # length_mismatched = os.path.getsize(_mp3_info.file_path) != _mp3_info.length
    if not path_exists:
        logger.debug(f"Downloading... {_mp3_info.file_name}")
        _file_response = requests.get(_mp3_info.link)
        if _file_response.ok:
            logger.debug(f"Writing... {_mp3_info.file_path}")
            with open(_mp3_info.file_path, 'wb') as _f:
                _f.write(_file_response.content)
        else:
            logger.error(f"error saving file response: {_file_response.status_code}")
    else:
        logger.debug(f"{_mp3_info.file_name} is already downloaded")


def write_transcripts(_result, _file_name, _episode_path):
    logger.debug("Writing transcriptions...")
    writer = WriteTXT(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.txt")

    writer = WriteJSON(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.json")

    writer = WriteTSV(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.tsv")

    writer = WriteSRT(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.srt")

    Path(_episode_path / "transcribed").touch()


def transcribe_if_required(_model, _mp3_info, _episode_path):
    if not os.path.exists(_episode_path / "transcribed"):
        logger.debug(f"Starting transcription in {_episode_path}")
        start = time.time()
        result = _model.transcribe(audio=str(_mp3_info.file_path), language="en")
        write_transcripts(result, _mp3_info.file_name, _episode_path)
        end = time.time()
        elapsed = float(end - start)
        elapsed_minutes = str(round(elapsed / 60, 2))
        logger.debug(f"Processed {_mp3_info.file_name} in: {elapsed_minutes} Minutes")
    else:
        logger.debug(f"{_mp3_info.file_name} is already transcribed.")


def get_transcript_text(_episode_path, _file_name):
    with open(_episode_path / f"{_file_name}.txt", 'r') as transcript:
        body = transcript.read()

    body = re.sub(r'(?<=[^.?])\n', ' ', body)
    body = body.replace("\n", "\n\n")
    return body


def main(data_dir: str, feed_uri: str, verbose: bool, model_name: str):
    initialise_logging(verbose)

    if data_dir is None or not is_writable(data_dir):
        logger.error("The data_dir is missing, or not writable. Cannot continue")
        exit(1)

    if feed_uri is None:
        logger.info("Processing default feeds")
        feed_uris = default_feeds()
    else:
        feed_uris = [feed_uri]

    whisper_model = initialise_whisper(model_name)

    for feed_uri in feed_uris:
        feed_response = get_feed(feed_uri)
        logger.info(f"Processing {feed_uri}")
        episode_dicts = []

        if feed_response and feed_response.feed:
            pod_path = create_pod_path(data_dir, feed_response.feed.title)

            if not pod_path:
                logger.error("Cannot find podcast title")
                return

            for entry in feed_response.entries:
                try:
                    formatted_published_date = time.strftime("%Y-%m-%d", entry.published_parsed)
                    entry_title_and_date = f"{formatted_published_date}-{entry.title}"

                    episode_path = create_episode_path(pod_path, entry_title_and_date)
                    mp3_info = get_mp3_info(entry.links, episode_path)

                    if mp3_info is None:
                        logger.debug(f"{entry.title} has no mp3 link")
                    else:
                        download_file_if_required(mp3_info)
                        transcribe_if_required(whisper_model, mp3_info, episode_path)
                        transcript = get_transcript_text(episode_path, mp3_info.file_name)
                        episode_dicts.append(get_episode_dict(feed_response.feed, entry, transcript))

                except Exception as e:
                    logger.error(f"Couldn't process episode entry: {entry.title}")
                    logger.error(e)

        if len(episode_dicts) > 0:
            with open("pods.ndjson", "w") as pods_json:
                for episode_dict in episode_dicts:
                    pods_json.write('{ "index": {}}')
                    pods_json.write("\n")
                    pods_json.write(json.dumps(episode_dict))
                    pods_json.write("\n")


def get_episode_dict(podcast_metadata, episode_data, transcript: str):

    episode_audio_link = [d["href"] for d in episode_data.links if d["rel"] == "enclosure"]
    if episode_audio_link and len(episode_audio_link) > 0:
        episode_audio_link = episode_audio_link[0]
    else:
        logger.error(f"Skipping episode because it has no MP3")
        return None

    try:
        podcast_title = podcast_metadata.title
        podcast_link = getattr(podcast_metadata, "link", None)
        podcast_language = getattr(podcast_metadata, "language", None)
        podcast_copyright = getattr(podcast_metadata, "rights", None)
        podcast_author = getattr(podcast_metadata, "author", None)

        # todo normalise the tags, dupes, case etc
        podcast_tags = getattr(podcast_metadata, "tags", None)
        if podcast_tags:
            podcast_tags = [d["term"] for d in podcast_tags]

        podcast_image = getattr(podcast_metadata, "image", None)
        if podcast_image:
            podcast_image = podcast_image.href

        podcast_type = getattr(podcast_metadata, "itunes_type", None)

    except AttributeError as e:
        logger.error(f"Error getting podcast metadata in {podcast_title}")
        logger.error(e)
        return None  # todo maybe too harsh to return here

    try:
        episode_title = episode_data.title
        episode_published_on = episode_data.published_parsed
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

        episode_duration = getattr(episode_data, "itunes_duration", None)
        if episode_duration and ":" in episode_duration:
            episode_duration = utils.time_to_seconds(episode_duration)

        # todo maybe include podcast_transcripts
        # todo combine tags and keywords for episode?
        episode_tags = getattr(episode_data, "tags", None)
        if episode_tags:
            episode_tags = [d["term"] for d in episode_tags]

        episode_dict = {
            "podcast_title": podcast_title,
            "podcast_link": podcast_link,
            "podcast_language": podcast_language,
            "podcast_copyright": podcast_copyright,
            "podcast_author": podcast_author,
            "podcast_tags": podcast_tags,
            "podcast_image": podcast_image,
            "podcast_type": podcast_type,

            "episode_title": episode_title,
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
            "episode_keywords": episode_keywords,
            "episode_duration": episode_duration,
            "episode_tags": episode_tags,
            "episode_transcript": transcript
        }

        return episode_dict

    except AttributeError as e:
        print(f"Error in {episode_title}")
        print(e)


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


if __name__ == "__main__":

    parser = argparse.ArgumentParser(
        prog='rss_to_whisper.py',
        description='Utils for downloading podcasts from rss feeds and transcribing them',
        epilog='Have fun')

    parser.add_argument("-d", "--data-dir", required=True,
                        help="Provide a path to a writable directory where pods will be downloaded to.")
    parser.add_argument("-f", "--feed", required=False,
                        help="Provide an rss feed, e.g. http://feeds.libsyn.com/60664 ")
    parser.add_argument("-v", "--verbose", action='store_true')
    parser.add_argument("-m", "--model-name", required=False, default="medium")

    args = parser.parse_args()
    main(data_dir=args.data_dir, feed_uri=args.feed, verbose=args.verbose, model_name=args.model_name)
