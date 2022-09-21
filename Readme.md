# Edge Service

## Setup Virtual environment

```bash
python3 -m venv venv
source venv/bin/activate
```

## Install Dependencies

```bash
pip3 install -r requirements.txt
```

## Start Locally

```bash
python3 main.py <broker_url> <broker_port> <response_topic>
```

## Build and Run Docker Container

```bash
docker build -t dev.local/edge-service:0.1 .
docker compose up -d
```
