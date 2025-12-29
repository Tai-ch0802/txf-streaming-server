# 技術堆棧 (Tech Stack)

## 核心技術 (Core Technologies)
- **Language**: Python 3.9+
- **Async Framework**: `asyncio` (Python 標準函式庫)
- **Market Data API**: Shioaji (Sinopac Securities)
- **Serialization**: Google Protobuf 3
- **Message Broker**: Apache Kafka

## 開發工具 (Development Tools)
- **Version Control**: Git
- **Virtual Environment**: `venv`
- **Process Management**: Systemd (Linux)
- **Scheduling**: Crontab (Linux)

## 相依套件 (Dependencies)
詳見 `requirements.txt`。主要包含：
- `shioaji`: 永豐 API 客戶端
- `kafka-python`: Kafka 客戶端
- `protobuf`: Protobuf 支援庫
- `grpcio-tools`: Protobuf 編譯工具
