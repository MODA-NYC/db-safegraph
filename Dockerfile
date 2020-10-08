FROM python:3.8-slim-buster

RUN apt update\
    && apt install -y curl git zip unzip gdal-bin gnupg jq\
    && apt autoclean -y

RUN curl -O https://dl.min.io/client/mc/release/linux-amd64/mc\
    && chmod +x mc\
    && mv ./mc /usr/bin

RUN curl "https://awscli.amazonaws.com/awscli-exe-linux-x86_64.zip" -o "awscliv2.zip"\
    && unzip awscliv2.zip\
    && ./aws/install

WORKDIR /

COPY requirements.txt /requirements.txt

RUN pip install --upgrade pip\
    && pip install -r requirements.txt
