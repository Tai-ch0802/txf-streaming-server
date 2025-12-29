# TXF Streaming Server

這是一個高頻交易 (HFT) 資料管線服務，使用 **Asyncio** 連接永豐 Shioaji API，將台指期 (TXF) 的 **Tick** 與 **BidAsk** 數據經由 **Google Protobuf** 序列化後，即時推送到 **Apache Kafka**。

更詳細的專案說明與架構，請參閱 [GEMINI.md](GEMINI.md)。

-----

## 主要功能

- **即時串流**: 訂閱 TXF 即時行情。
- **高效序例化**: 使用 Protobuf 壓縮數據，並採用 Scaled Integer (x10000) 保存價格精度。
- **非同步處理**: 使用 Python `asyncio` 確保高吞吐量。

## 快速開始 (Quick Start)

### 1. 環境準備 (Docker)

本專案提供 `Makefile` 與 Docker Compose 整合，可一鍵啟動 Kafka 環境：

```bash
# 1. 建立虛擬環境與安裝依賴
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt

# 2. 啟動 Kafka 服務 (Docker)
make up
```

若需停止服務：
```bash
make down
```

### 2. 設定檔 (Configuration)

在專案根目錄下建立 `.env` 檔案：

```ini
# .env
# Shioaji API
SHIOAJI_API_KEY=your_api_key
SHIOAJI_SECRET_KEY=your_secret_key

# Kafka
KAFKA_BOOTSTRAP_SERVERS=localhost:9092
TICK_TOPIC=txf-tick
BIDASK_TOPIC=txf-bidask
```

### 3. Protobuf 編譯

若修改 `.proto` 檔需重新編譯：

```bash
python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto
```

### 4. 執行

```bash
python -m src.txf_producer
```

## 部署 (Deployment)

支援 Systemd 與 Crontab 自動排程，詳細設定請參考原始文件或 `GEMINI.md` 中的說明。

-----

## Disclaimer

本專案僅供程式交易研究與教育用途。使用者需自行承擔交易風險。