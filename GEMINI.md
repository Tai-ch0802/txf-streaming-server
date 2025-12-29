# GEMINI.md

## Project Overview
This project, `txf-streaming-server`, is a high-frequency trading (HFT) data pipeline service designed to process Taiwan Index Futures (TXF) market data. It connects to the **Shioaji API**, retrieves real-time **Tick** and **BidAsk** data, serializes it using **Google Protobuf**, and pushes it to **Apache Kafka**.

The system is built with concurrency and performance in mind, utilizing `asyncio` for non-blocking I/O operations.

## Architecture
1.  **Source**: Shioaji API (Sinopac Securities)
2.  **Processing**: Python application using `asyncio`.
3.  **Serialization**: Google Protobuf 3 (Scaled Integer for prices).
4.  **Destination**: Apache Kafka (Topics: `txf-tick`, `txf-bidask`).

## Key Files & Directories

### Root Directory
- `README.MD`: Project documentation and quick start guide.
- `Makefile`: Common commands (up, down, clean).
- `ops/`: Infrastructure (Docker).
- `requirements.txt`: Python dependency list.
- `.env`: (Ignored) Contains sensitive API keys and Kafka configuration.
- `src/`: Source code directory.
- `protos/`: Protobuf definition files.
- `pic/`: Images for documentation.

### Source Code (`src/`)
- `src/txf_producer.py`: **Main Application**. The entry point for the producer service. Handles API connection, data subscription, and pushing to Kafka.
- `src/txf_consumer.py`: A consumer script for testing and verifying data flow from Kafka.
- `src/config.py`: Configuration loader.
- `src/txf_data_pb2.py`: Generated Protobuf Python class (Do not edit manually).

### Protobuf (`protos/`)
- `protos/txf_data.proto`: Defines the data schema for `Tick` and `BidAsk` messages.

## Data Schema (Protobuf)
All price fields use **Scaled Integer (x10000)** to preserve precision while identifying as integers for performance.

- **Tick Data**: `txf.Tick` (Code, Timestamp, Close, Volume, etc.)
- **BidAsk Data**: `txf.BidAsk` (5-level Bid/Ask prices and volumes)

## Tech Stack
- **Language**: Python 3.9+
- **API**: Shioaji (Sinopac)
- **Serialization**: Google Protobuf
- **Message Broker**: Apache Kafka
- **Async Framework**: `asyncio`
- **Deployment**: Systemd, Crontab
