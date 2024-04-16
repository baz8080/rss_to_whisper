import argparse
import logging
import os
import re
import time
from collections import namedtuple
from pathlib import Path

import feedparser
import requests
import torch
import whisper
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from whisper.utils import WriteTXT, WriteTSV

import utils
from utils import is_writable, get_file_part, default_feeds, create_path, chunk, initialise_logging, get_episode_dict

logger = logging.getLogger(__name__)
load_dotenv()


def main(data_dir: str, single_feed_uri: str, single_feed_collection: str,
         verbose: bool, model_name: str, process_local_only: bool):
    initialise_logging(logger, verbose)
    initialise_logging(utils.logger, verbose)

    process_feeds(data_dir=data_dir, single_feed_uri=single_feed_uri, single_feed_collection=single_feed_collection,
                  model_name=model_name, process_local_only=process_local_only)


def initialise_whisper(model_name: str):
    logger.info(f"Cuda available: {torch.cuda.is_available()}")
    logger.debug(f"Using {model_name} model")
    model = whisper.load_model(model_name)
    return model


def process_feeds(data_dir: str, single_feed_uri: str, single_feed_collection: str,
                  model_name: str, process_local_only: bool = False):
    if data_dir is None or not is_writable(data_dir):
        logger.error("The data_dir is missing, or not writable. Cannot continue")
        exit(1)

    if single_feed_uri and single_feed_collection:
        logger.info(f"Processing single feed {single_feed_uri}")
        feed_collections = {
            single_feed_collection.lower(): [single_feed_uri]
        }
    else:
        logger.info("Processing default feeds")
        feed_collections = default_feeds()

    episode_dicts = []
    whisper_model = None

    for collection in feed_collections.keys():
        for feed_uri in feed_collections[collection]:
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

                        mp3_and_transcript_exist = (mp3_info.file_path.exists() and
                                                    (episode_directory_path / "transcribed").exists())

                        if mp3_and_transcript_exist:
                            transcript_text = get_transcript_text(episode_directory_path / f"{mp3_info.file_name}.txt")
                            episode_dicts.append(
                                get_episode_dict(feed_response.feed, entry, transcript_text, collection))
                        elif process_local_only:
                            continue
                        else:
                            download_file_if_required(mp3_info)

                            if whisper_model is None:
                                whisper_model = initialise_whisper(model_name)

                            transcribe_if_required(whisper_model, mp3_info, episode_directory_path)
                            transcript_text = get_transcript_text(episode_directory_path / f"{mp3_info.file_name}.txt")
                            episode_dicts.append(
                                get_episode_dict(feed_response.feed, entry, transcript_text, collection))

                    except Exception as e:
                        logger.error(f"Couldn't process episode entry: {entry.title}")
                        logger.error(e)

    elastic_api_key = os.getenv("ELASTIC_API_KEY")
    elastic_client = Elasticsearch(hosts="https://nasty.local:9200/", api_key=elastic_api_key, verify_certs=False)
    elastic_client.indices.delete(index="podcasts")
    elastic_client.indices.create(
        index="podcasts",
        body={
            "settings": {
                "index.store.preload": ["nvd", "dvd"]
            }
        }
    )
    bulk(client=elastic_client, actions=generate_data_for_indexing(episode_dicts))


def generate_data_for_indexing(_episode_dicts):
    for chunked_list in chunk(_episode_dicts, 300):
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
        if _link.type == "audio/mpeg" or _link.type == "audio/mp3":
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

    body, was_altered = replace_repeated_phrases(body)

    body = re.sub(r'(?<=[^.?])\n', ' ', body)
    body = body.replace("\n", "\n\n")
    return body


def replace_repeated_phrases(text, threshold=13):
    pattern = r'\b(.+?)\s+(?:\1\s+){' + str(threshold - 1) + r',}\b'

    def repl(match):
        whitespace = '\n' if '\n' in match.group(0) else ' '

        ttr = match.group(1)
        ttr_lower = ttr.lower()

        starts_with_emphasis = ttr_lower.startswith("no") or ttr_lower.startswith("nope") or ttr_lower.startswith(
            "many") or ttr_lower.startswith("now") or ttr_lower.startswith("great") or ttr_lower.startswith("big")

        if " " in ttr:
            return ttr + whitespace
        elif "000," in ttr or starts_with_emphasis:
            return match.group(0)
        else:
            return ttr + whitespace

    replaced_text = re.sub(pattern, repl, text, flags=re.IGNORECASE)
    text_was_changed = replaced_text != text

    return replaced_text, text_was_changed


if __name__ == "__main__":
    parser = argparse.ArgumentParser(
        prog='rss_to_whisper.py',
        description='Utils for downloading podcasts from rss feeds and transcribing them',
        epilog='Have fun')

    parser.add_argument("-d", "--data-dir", required=True,
                        help="Provide a path to a writable directory where pods will be downloaded to.")
    parser.add_argument("-f", "--feed", required=False,
                        help="Provide a single rss feed, e.g. http://feeds.libsyn.com/60664. Must also provide a "
                             "collection with -c --collection")
    parser.add_argument("-v", "--verbose", action='store_true')
    parser.add_argument("-m", "--model-name", required=False, default="medium")
    parser.add_argument("-l", "--local-only", required=False, action="store_true")
    parser.add_argument("-c", "--collection", required=False)

    args = parser.parse_args()
    main(data_dir=args.data_dir, single_feed_uri=args.feed, single_feed_collection=args.collection,
         verbose=args.verbose, model_name=args.model_name, process_local_only=args.local_only)
