# TXF Streaming Server

這是一個練習高頻交易 (HFT) 資料管線設計的 Python 服務。它使用 **Asyncio** 連接永豐 Shioaji API，將台指期 (TXF) 的 **Tick** 與 **BidAsk** 數據經由 **Google Protobuf** 序列化後，即時推送到 **Apache Kafka**。

-----

## 核心架構 (Architecture) 

| 元件 | 職責 (Role) | 優勢與考量點 (Why it Matters) |
| :--- | :--- | :--- |
| **Source: Sinopac Shioaji API** (WebSocket) | 資料來源 | **WebSocket** 提供全雙工連線，適合即時、低延遲的行情推送。|
| **Transport: Confluent Kafka** (librdkafka) | 傳輸層/消息佇列 | **高吞吐、高可靠性**。負責將數據從 Producer 穩定傳輸至 Consumer，並允許 Consumer 異步處理。`librdkafka` (C-based) 確保了 Python 介面下的極致速度。|
| **Serialization: Google Protobuf 3** (Scaled Integers) | 資料格式 | **極致壓縮與序列化速度**。Protobuf 比 JSON/XML 體積小、編解碼快，是 HFT 數據管線的標準選擇。`Scaled Integers` 則確保了金融數據的精度，同時利用整數運算優化性能。|
| **Concurrency: Python Asyncio** (Non-blocking I/O) | 併發模型 | **I/O 效率高，低 CPU 消耗**。允許服務在單一執行緒內高效地等待來自網路 (Shioaji) 的數據，而不會阻塞程式。|
| **Asyncio Engine: uvloop** (High-Performance Event Loop) | 併發引擎 | **進階優化**。取代 Python 標準事件迴圈，用 C 語言實現的 `libuv` 核心來大幅提升 I/O 排程與任務切換的效率 (Jitter/Latency 降低)。|
| **Process Management: Systemd** (Auto-restart / Logging) | 服務管理 | **穩定性與自動化**。作為作業系統的守護程序，確保服務啟動、統一收集日誌 (`journalctl`)，並負責在程式錯誤退出時 (Exit Code 1) **自動重啟**，以實現盤中自癒。|
| **Scheduling: Linux Crontab** (Day/Night Session control) | 時間控制 | **定時啟動與關閉**。精確控制服務的生命週期，使其只在台指期開市期間運行，避免浪費資源。|


這個架構將 I/O 密集型任務 (Shioaji) 和 CPU 密集型任務 (Serialization) 隔開，並使用高效的 C 語言工具 (librdkafka, uvloop, Protobuf) 貫穿整個流程，以達到低延遲的目標。


-----

## 目錄結構 (Structure)

```text
txf-streaming-server/     
├── protos/
│   └── txf_data.proto       # 數據定義 (Protobuf Source)
├── src/                     
│   ├── __init__.py          # Package 標記檔
│   ├── txf_producer.py      # [核心] 生產者服務 (Systemd 啟動目標)
│   ├── txf_consumer.py      # [測試] 消費者範例 (Kafka -> Strategy)
│   ├── config.py            
│   └── txf_data_pb2.py      # Protobuf 編譯後的 Python 類別
├── .env                     # 敏感資料設定
├── .gitignore               
└── requirements.txt         
```

-----

## 資料格式 (Data Schema)

本專案使用 **Google Protobuf 3** 定義資料結構。為了在保留小數位數的同時提升序列化效能與壓縮比，所有價格欄位皆採用 **Scaled Integer (x10000)** 儲存。

### 1\. Tick Data (`txf.Tick`)

| Field | Type | Description | Note |
| :--- | :--- | :--- | :--- |
| `code` | string | 商品代碼 | e.g., "TXFR1" |
| `timestamp_ms` | int64 | 時間戳記 | Unix Epoch (ms) |
| `close` | int64 | 成交價 | **Price \* 10000** |
| `volume` | int32 | 單筆量 | |
| `tick_type` | int32 | 內外盤 | 1:外盤 (Buy), 2:內盤 (Sell) |
| `total_volume` | int32 | 當日總量 | 用於檢測封包遺漏 |
| `underlying_price` | int64 | 標的物價格 | Price \* 10000 |

### 2\. BidAsk Data (`txf.BidAsk`)

| Field | Type | Description | Note |
| :--- | :--- | :--- | :--- |
| `bid/ask_total_vol` | int32 | 委託總量 | |
| `bid/ask_price` | repeated int64 | 五檔價格 | List[5], **Price \* 10000** |
| `bid/ask_volume` | repeated int32 | 五檔掛單量 | List[5] |
| `diff_..._vol` | repeated int32 | 掛單變化量 | List[5], 策略訊號用 |

-----

## 快速開始 (Quick Start)

### 1\. 環境準備

建議使用 Python 3.9+ 與虛擬環境：

```bash
git clone https://github.com/gman-quant/txf-streaming-server.git
cd txf-streaming-server
python3 -m venv .venv
. .venv/bin/activate
pip install -r requirements.txt
```

### 2\. Protobuf 編譯

若您修改了 `.proto` 檔，需重新編譯 Python 類別：

```bash
# 確保已安裝 protoc 編譯器
python -m grpc_tools.protoc -I protos --python_out=src protos/txf_data.proto
```

### 3\. 設定檔 (Configuration)

請在專案根目錄下建立 `.env` 檔案，用於儲存 API 憑證與 Kafka 設定：

```python
# .env

# -----------------------------------------------
# 1. Shioaji API 憑證 (請先申請)
# -----------------------------------------------
SHIOAJI_API_KEY=your_api_key_here
SHIOAJI_SECRET_KEY=your_secret_key_here

# -----------------------------------------------
# 2. Kafka 伺服器設定 (請先建立 Kafka Topic)
# -----------------------------------------------
# Kafka Broker 範例: 192.168.1.10:9092 或 多個 broker 用逗號分隔
KAFKA_BOOTSTRAP_SERVERS=kafka_broker_IP:9092
# Topic 名稱範例: txf-tick, txf-bidask
TICK_TOPIC=txf-tick
BIDASK_TOPIC=txf-bidask
```

### 4\. 執行測試

手動執行時，Log 會包含時間戳記；Systemd 執行時則會自動隱藏時間 (Smart Logging)。

```bash
python -m src.txf_producer
```

-----

## 伺服器部署 (Deployment)

本服務設計為 Ubuntu Systemd 背景服務，配合 Crontab 進行自動開關機。

### 1\. Systemd 設定

建立 `/etc/systemd/system/txf-producer.service`：

```ini
[Unit]
Description=TXF Streaming Producer (Shioaji -> Kafka)
Documentation=https://github.com/gman-quant/txf-streaming-server

After=network-online.target kafka.service
Requires=kafka.service
PartOf=kafka.service

[Service]
# 請填入實際的使用者與路徑
User=
Group=
WorkingDirectory=
ExecStart=
Restart=on-failure
RestartSec=5s
Environment=PYTHONUNBUFFERED=1
Environment=LC_ALL=C.UTF-8
Environment=LANG=C.UTF-8
StandardOutput=journal
StandardError=journal

[Install]
WantedBy=multi-user.target
```

### 2\. Crontab 排程 (自動交易時段控制)

設定自動於日盤/夜盤開盤前啟動，收盤後停止：

```bash
sudo crontab -e
```

於文件最下方貼上以下內容：

```nano
# --- Day Session (08:29 Start - 13:46 Stop) ---
29 08 * * 1-5 /bin/systemctl start txf-producer.service
46 13 * * 1-5 /bin/systemctl stop txf-producer.service

# --- Night Session (14:49 Start - 05:01 Stop) ---
49 14 * * 1-5 /bin/systemctl start txf-producer.service
01 05 * * 2-6 /bin/systemctl stop txf-producer.service
```

-----

## 維運指令速查 (Operations Cheat Sheet)

以下指令皆需以 `sudo` 執行（Root 權限）。

### 1\. 監控與狀態 (Monitoring)

| 動作 | 指令 |
| :--- | :--- |
| **即時監控 Log** | `sudo journalctl -u txf-producer.service -f` |
| **查看服務狀態** | `sudo systemctl status txf-producer.service` |

### 2\. 手動控制 (Manual Control)

| 動作 | 指令 |
| :--- | :--- |
| **啟動服務** | `sudo systemctl start txf-producer.service` |
| **停止服務** | `sudo systemctl stop txf-producer.service` |
| **重啟服務** | `sudo systemctl restart txf-producer.service` |

> *註：修改 Python 程式碼後，請執行「重啟服務」。*

### 3\. 排程與設定 (Configuration)

| 動作 | 指令 |
| :--- | :--- |
| **修改排程** | `sudo crontab -e` |
| **檢查排程** | `sudo crontab -l` |
| **修改 Service 檔** | `sudo nano /etc/systemd/system/txf-producer.service` |
| **重載 Service 設定**| `sudo systemctl daemon-reload` |

> *註：修改 `.service` 檔案後，務必執行「重載 Service 設定」。*

### 4\. 開機啟動管理 (Boot Control)

| 動作 | 指令 |
| :--- | :--- |
| **停用開機自啟** | `sudo systemctl disable txf-producer.service` |
| **啟用開機自啟** | `sudo systemctl enable txf-producer.service` |
| **檢查設定狀態** | `systemctl is-enabled txf-producer.service` |

> *註：本專案使用 Crontab 控制交易時段，建議設定為 **停用 (disable)** 以免非交易時間誤啟動。*

-----

## Disclaimer

本專案僅供程式交易研究與教育用途。高頻交易涉及極高風險，使用者需自行承擔 API 連線不穩、報價延遲或系統故障可能導致的損失。