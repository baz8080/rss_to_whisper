import argparse
from dotenv import load_dotenv
import logging
import os
import re
import sys
import time
from collections import namedtuple
from pathlib import Path

import feedparser
import requests
import torch
import whisper
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from whisper.utils import WriteTXT, WriteTSV

from utils import is_writable, time_to_seconds, get_hash, get_file_part, default_feeds, create_path, chunk

logger = logging.getLogger(__name__)
load_dotenv()


def main(data_dir: str, feed_uri: str, verbose: bool, model_name: str):
    initialise_logging(verbose)
    process_feeds(data_dir, feed_uri, model_name)


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


def initialise_whisper(model_name: str):
    logger.info(f"Cuda available: {torch.cuda.is_available()}")
    logger.debug(f"Using {model_name} model")
    model = whisper.load_model(model_name)
    return model


def process_feeds(data_dir: str, feed_uri: str, model_name: str):
    if data_dir is None or not is_writable(data_dir):
        logger.error("The data_dir is missing, or not writable. Cannot continue")
        exit(1)

    if feed_uri is None:
        logger.info("Processing default feeds")
        feed_uris = default_feeds()
    else:
        feed_uris = [feed_uri]

    whisper_model = initialise_whisper(model_name)
    episode_dicts = []

    for feed_uri in feed_uris:
        feed_response = get_feed(feed_uri)
        logger.info(f"Processing {feed_uri}")

        if feed_response and feed_response.feed:
            pod_path = create_path(data_dir, feed_response.feed.title)

            if not pod_path:
                logger.error("Cannot find podcast title")
                return

            for entry in feed_response.entries:
                try:
                    entry_title_and_date = get_episode_title_with_date(entry)
                    episode_directory_path = create_path(pod_path, entry_title_and_date)

                    if not episode_directory_path:
                        logger.error("Failed to make directory for the episode")
                        continue

                    mp3_info = get_mp3_info(entry.links, episode_directory_path)

                    if mp3_info is None:
                        logger.warning(f"{entry.title} has no mp3 link. Skipping")
                        continue

                    download_file_if_required(mp3_info)
                    transcribe_if_required(whisper_model, mp3_info, episode_directory_path)
                    transcript_text = get_transcript_text(episode_directory_path / f"{mp3_info.file_name}.txt")
                    episode_dicts.append(get_episode_dict(feed_response.feed, entry, transcript_text))

                except Exception as e:
                    logger.error(f"Couldn't process episode entry: {entry.title}")
                    logger.error(e)

    elastic_api_key = os.getenv("ELASTIC_API_KEY")
    elastic_client = Elasticsearch(hosts="https://localhost:9200/", api_key=elastic_api_key,verify_certs=False)
    elastic_client.indices.delete(index="podcasts")
    bulk(client=elastic_client, actions=generate_data_for_indexing(episode_dicts))


def generate_data_for_indexing(_episode_dicts):
    for chunked_list in chunk(_episode_dicts, 1000):
        logger.info(f"Processing chunk of size {len(chunked_list)}")
        for episode_dict in chunked_list:
            yield episode_dict


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


def get_episode_title_with_date(_episode) -> str:
    formatted_published_date = time.strftime("%Y-%m-%d", _episode.published_parsed)
    entry_title_and_date = f"{formatted_published_date}-{_episode.title}"
    return entry_title_and_date


def get_mp3_info(_pod_links, _episode_path):
    for _link in _pod_links:
        if _link.type == "audio/mpeg":
            MP3 = namedtuple("MP3", ["link", "file_name", "file_path", "length"])

            _href = _link.href
            _file_name = get_file_part(_href)
            _file_path = _episode_path / _file_name

            return MP3(link=_href, file_name=_file_name, file_path=_file_path, length=int(_link.length))

    return None


def download_file_if_required(_mp3_info):
    path_exists = _mp3_info.file_path.exists()
    # todo - some podcasts are lying about the byte size, so this check is not perfect
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


def write_transcripts(_result, _file_name, _episode_path):
    logger.debug("Writing transcriptions...")
    writer = WriteTXT(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.txt")

    writer = WriteTSV(_episode_path)
    writer(_result, _episode_path / f"{_file_name}.tsv")

    Path(_episode_path / "transcribed").touch()


def get_transcript_text(_file_path):
    with open(_file_path, 'r') as transcript:
        body = transcript.read()

    body = re.sub(r'(?<=[^.?])\n', ' ', body)
    body = body.replace("\n", "\n\n")
    return body


def get_episode_dict(podcast_metadata, episode_data, transcript: str):
    episode_dict = None

    _id = get_hash(transcript)

    episode_audio_link = [d["href"] for d in episode_data.links if d["rel"] == "enclosure"]
    if episode_audio_link and len(episode_audio_link) > 0:
        episode_audio_link = episode_audio_link[0]
    else:
        logger.error(f"Skipping episode because it has no MP3")
        return episode_dict

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
            episode_duration = time_to_seconds(episode_duration)

        # todo maybe include podcast_transcripts
        # todo combine tags and keywords for episode?
        episode_tags = getattr(episode_data, "tags", None)
        if episode_tags:
            episode_tags = [d["term"] for d in episode_tags]

        episode_dict = {
            "_id": _id,
            "_index": "podcasts",
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
        logger.error(f"Error getting podcast metadata")
        logger.error(e)
        return None


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
