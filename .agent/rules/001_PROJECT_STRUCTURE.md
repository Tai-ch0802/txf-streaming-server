# 專案結構 (Project Structure)

本專案採用標準 Python 專案結構，核心邏輯位於 `src/`，介面定義位於 `protos/`。

## 目錄說明 (Directory Description)
- `src/`: 原始碼目錄 (Source Code)
    - `txf_producer.py`: 主程式入口，負責資料與 Kafka 的串接。
    - `txf_consumer.py`: 測試用消費者程式。
    - `config.py`: 設定檔讀取模組。
- `protos/`: Protobuf 定義檔目錄
    - `txf_data.proto`: 定義 Tick 與 BidAsk 的資料結構。
- `pic/`: 文件圖片資源
- `.gemini/`: AI 助手記憶與筆記 (Context Storage)
- `.agent/rules/`: 專案開發規則 (Development Rules)

## 檔案命名 (Naming Convention)
- Python 模組使用 `snake_case` (e.g., `txf_producer.py`)。
- Protobuf 檔案使用 `snake_case` (e.g., `txf_data.proto`)。
