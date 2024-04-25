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
import yaml
from dotenv import load_dotenv
from elasticsearch import Elasticsearch
from elasticsearch.helpers import bulk
from whisper.utils import WriteTXT, WriteTSV

import utils
from utils import is_writable, get_file_part, create_path, chunk, initialise_logging, get_episode_dict

logger = logging.getLogger(__name__)
load_dotenv()


def main(config_file: str):
    with open(config_file, "r") as pods_config_file:
        pods_config = yaml.safe_load(pods_config_file)

    if not pods_config:
        print("Cannot read configuration file")
        exit(1)

    verbose_logging = pods_config["verbose"] if "verbose" in pods_config else False
    initialise_logging(logger, verbose_logging)
    initialise_logging(utils.logger, verbose_logging)

    process_feeds(pods_config)


def initialise_whisper(model_name: str):
    logger.info(f"Cuda available: {torch.cuda.is_available()}")
    logger.debug(f"Using {model_name} model")
    model = whisper.load_model(model_name)
    return model


def process_feeds(config):
    whisper_model_name = config["whisper_model"] if "whisper_model" in config else "tiny"
    process_local_files_only = config["process_local_files_only"] if "process_local_files_only" in config else False

    if "elastic_server" not in config or "data_directory" not in config or "podcasts" not in config:
        logger.error("Required configuration missing.")
        exit(1)

    elastic_server = config["elastic_server"]
    data_dir = config["data_directory"]

    if data_dir is None or not is_writable(data_dir):
        logger.error("The data_dir is missing, or not writable. Cannot continue")
        exit(1)

    whisper_model = None
    podcasts = config["podcasts"]

    elastic_client = initialise_elastic_client(elastic_server, os.getenv("ELASTIC_API_KEY"))

    for podcast in podcasts:
        podcast_url = podcast["url"] if "url" in podcast else None

        if not podcast_url:
            logger.error("Skipping podcast with missing URL")
            continue

        episode_dicts = []
        feed_response = get_feed(podcast_url)

        if feed_response and feed_response.feed:
            logger.info(f"Downloaded {podcast['url']}")

            collections = podcast["collections"] if "collections" in podcast else []
            excludes = podcast["excludes"] if "excludes" in podcast else []

            pod_path = create_path(data_dir, feed_response.feed.title)

            if not pod_path:
                logger.error("Cannot find podcast path to write to")
                return

            for entry in feed_response.entries:

                if any(exclude.lower() in entry.title.lower() for exclude in excludes):
                    logger.debug("Skipping podcast entry because of excludes match")
                    continue

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
                        # transcript_text = get_transcript_text(episode_directory_path / f"{mp3_info.file_name}.txt")
                        transcript_text = (
                            get_transcript_text_with_timing(episode_directory_path / f"{mp3_info.file_name}.tsv"))
                        episode_dicts.append(
                            get_episode_dict(feed_response.feed, entry, transcript_text, collections))

                    elif process_local_files_only:
                        continue
                    else:
                        download_file_if_required(mp3_info)

                        if whisper_model is None:
                            whisper_model = initialise_whisper(whisper_model_name)

                        transcribe_if_required(whisper_model, mp3_info, episode_directory_path)
                        # transcript_text = get_transcript_text(episode_directory_path / f"{mp3_info.file_name}.txt")
                        transcript_text = (
                            get_transcript_text_with_timing(episode_directory_path / f"{mp3_info.file_name}.txt"))
                        episode_dicts.append(
                            get_episode_dict(feed_response.feed, entry, transcript_text, collections))

                except Exception as e:
                    logger.error(f"Couldn't process episode entry: {entry.title}")
                    logger.error(e)

        bulk(client=elastic_client, actions=generate_data_for_indexing(episode_dicts))


def initialise_elastic_client(elastic_host: str, api_key: str):
    elastic_client = Elasticsearch(hosts=elastic_host, api_key=api_key, verify_certs=False)
    elastic_client.indices.delete(index="podcasts")
    elastic_client.indices.create(
        index="podcasts",
        body={
            "settings": {
                "index.store.preload": ["nvd", "dvd"]
            }
        }
    )

    elastic_client.cluster.put_settings(body={
        "persistent": {
            "search.max_async_search_response_size": "101mb"
        }
    })

    return elastic_client


def generate_data_for_indexing(_episode_dicts):
    for chunked_list in chunk(_episode_dicts, 100):
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

    body, _ = replace_repeated_phrases(body)

    body = re.sub(r'(?<=[^.?])\n', ' ', body)
    return body


def get_transcript_text_with_timing(_file_path):

    body = ""

    with open(_file_path, "r") as input_file:

        input_file.readline()  # skip header

        accumulated_text = ""
        accumulated_text_start = None

        for line in input_file:
            current_start, _, text = line.strip().split('\t')
            current_start = int(current_start)

            if not accumulated_text:
                accumulated_text_start = current_start

            if not text.endswith('.'):
                accumulated_text += text + ' '
            else:
                if accumulated_text:
                    body += f"{accumulated_text_start}\t{accumulated_text.strip()} {text}\n"
                    accumulated_text = ""
                    accumulated_text_start = None
                else:
                    body += f"{current_start}\t{text}\n"

    body, was_altered = replace_repeated_phrases(body)
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

    parser.add_argument("-c", "--config", required=False, help="Provide a config yaml file")
    args = parser.parse_args()
    main("pods.yaml")
