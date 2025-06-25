FROM ubuntu:latest
COPY . /app
WORKDIR /app
ENV DEBIAN_FRONTEND=noninteractive
RUN apt update && \
    apt install -y software-properties-common && \
    add-apt-repository -y ppa:deadsnakes/ppa && \
    apt install -y python3.12 python3-pip && \
    pip install --break-system-packages -r requirements.txt