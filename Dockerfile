FROM nvidia/cuda:12.4.1-devel-ubuntu22.04

RUN mkdir -p /root/rss_to_whisper_data
RUN mkdir -p /root/code/rss_to_whisper

RUN apt-get update
RUN apt-get install -y python3-all python-is-python3 python3-pip

WORKDIR /root/code/rss_to_whisper
COPY *.py ./
COPY requirements.txt ./
COPY .env ./
COPY pods.yaml ./


RUN pip install -r requirements.txt

# Command to keep the container running
CMD ["tail", "-f", "/dev/null"]