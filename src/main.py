"""
TXF Streaming Producer (Shioaji -> Kafka Protobuf)
--------------------------------------------------
Description: 
    連接永豐 Shioaji API 接收台指期 (TXF) 的 Tick 與 BidAsk 數據，
    透過 Protobuf 序列化後，極速推送到 Kafka Broker。
    
Architecture:
    - Process Management: Systemd (Auto-restart on exit code 1)
    - Concurrency: asyncio (Low CPU usage)
    - Serialization: Google Protobuf (Scaled Integers)
    - Transport: confluent-kafka (librdkafka C binding)

Author: Garrett & Gemini
Last Updated: 2025-11-28
"""

import sys
import asyncio
from datetime import datetime
from decimal import Decimal
from typing import Optional

# --- Third-party Imports ---
import shioaji as sj
from shioaji import TickFOPv1, BidAskFOPv1, Exchange
from confluent_kafka import Producer

# --- Local Imports ---
import txf_data_pb2  # Protobuf Definition
from config import (
    SHIOAJI_API_KEY, 
    SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC, 
    BIDASK_TOPIC
)

# ==========================================
# Global Constants & Configuration
# ==========================================

# 全域 API 實例 (用於 Session 重建)
API_INSTANCE: Optional[sj.Shioaji] = None 

# 價格縮放倍數 (配合 .proto 定義，保留 4 位小數)
SCALE = 10000

# 致命錯誤碼集合：遇到這些 Solace 代碼時，視為連線徹底失敗，需觸發 Systemd 重啟
# 1: Session Down, 2: Connect Failed, 8: Assured Delivery Down
FATAL_CODES = {1, 2, 8} 

# Kafka Producer Configuration
KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'txf-producer-hft',
    
    # --- 速度核心 (Speed) ---
    'acks': '0',              # 極致速度，不等待確認
    'linger.ms': 0,           # 零延遲，有資料即刻發送 (HFT 關鍵)
    'compression.type': 'lz4', # 解壓縮最快，延遲最低
    
    # --- 穩定性防護 (Safety from old config) ---
    # 增加內部緩衝區，防止網路抖動或 Solace 重連時記憶體溢出
    # 128MB 足夠應付 TXF 快市時的斷線緩衝
    'queue.buffering.max.kbytes': 131072, 
    
    # 允許瞬間快市時的大封包 (雖然 linger=0，但瞬間大量寫入時仍有效)
    'batch.size': 262144,     # 256KB
    
    # 網路層優化 (針對 TCP)
    'socket.send.buffer.bytes': 102400, # 增加 TCP 發送緩衝
    'socket.receive.buffer.bytes': 102400,
}

# 初始化 Producer
try:
    producer = Producer(KAFKA_CONFIG)
except Exception as e:
    print(f"❌ Kafka Producer Initialization Failed: {e}")
    sys.exit(1)

# ==========================================
# Helper Functions
# ==========================================

def delivery_report(err, msg):
    """Kafka 傳送回調 (僅用於錯誤記錄)"""
    if err is not None:
        print(f'Message delivery failed: {err}')

def to_scaled_int(val: Optional[Decimal]) -> int:
    """將 Decimal/Float 轉換為 int64 (x10000) 以符合 Protobuf 定義"""
    if val is None: 
        return 0
    return int(val * SCALE)

# ==========================================
# Core Processing Logic (Protobuf Packing)
# ==========================================


def process_tick(quote: TickFOPv1):
    """
    處理 Tick 數據：極致瘦身版
    """
    try:
        if quote.simtrade == 1: 
            return

        tick = txf_data_pb2.Tick()
        
        # --- 必要欄位 ---
        tick.code = quote.code
        tick.timestamp_ms = int(quote.datetime.timestamp() * 1000)
        tick.tick_type = int(quote.tick_type)
        
        # --- 核心價格與量 ---
        tick.close = to_scaled_int(quote.close)
        tick.volume = int(quote.volume)
        tick.underlying_price = to_scaled_int(quote.underlying_price)
        
        # --- 檢核用 (Packet Loss Detection) ---
        tick.total_volume = int(quote.total_volume)

        # [已移除] simtrade, open, high, low, avg, chg, pct, amount...
        # 這些都在後端計算，傳輸這些是浪費頻寬。

        producer.produce(
            TICK_TOPIC, 
            key=tick.code.encode('utf-8'), 
            value=tick.SerializeToString(), 
            on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"❌ Error processing tick: {e}")

def process_bidask(quote: BidAskFOPv1):
    """
    處理 BidAsk 數據：極致瘦身版
    """
    try:
        if quote.simtrade == 1: 
            return
            
        ba = txf_data_pb2.BidAsk()
        
        # --- 基礎資訊 ---
        ba.code = quote.code
        ba.timestamp_ms = int(quote.datetime.timestamp() * 1000)
        
        # --- 總量 (觀察 OBI 大趨勢) ---
        ba.bid_total_vol = int(quote.bid_total_vol)
        ba.ask_total_vol = int(quote.ask_total_vol)

        # --- 五檔核心數據 (List Comprehension) ---
        ba.bid_price.extend([to_scaled_int(x) for x in quote.bid_price])
        ba.ask_price.extend([to_scaled_int(x) for x in quote.ask_price])
        
        ba.bid_volume.extend(quote.bid_volume)
        ba.ask_volume.extend(quote.ask_volume)
        
        # --- 策略關鍵：掛單變化量 (偵測撤單/虛掛單) ---
        ba.diff_bid_vol.extend(quote.diff_bid_vol)
        ba.diff_ask_vol.extend(quote.diff_ask_vol)
        
        # [已移除] underlying_price (Tick 有了), simtrade, first_derived_*

        producer.produce(
            BIDASK_TOPIC, 
            key=ba.code.encode('utf-8'), 
            value=ba.SerializeToString(), 
            on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"❌ Error processing bidask: {e}")


# ==========================================
# Connection & Event Handling (Smart Exit)
# ==========================================

def handle_session_down(reason: str = "Retries Timeout"):
    """
    [CRITICAL] 處理 Session Down 事件。
    策略：不嘗試原地重連，而是強制退出，讓 Systemd 負責啟動全新的乾淨實例。
    """
    global API_INSTANCE
    print(f"🚨 API Session Down Detected (Final): {reason}")
    print("--- 🛑 Terminating to force clean API recreation via Systemd... ---")
    
    # 嘗試優雅登出
    try: API_INSTANCE.logout()
    except: pass
    
    # 確保資料送出
    producer.flush() 
    
    # 非零退出碼 (1) 會告訴 Systemd 服務發生錯誤，需要重啟
    sys.exit(1)

def quote_event_handler(resp_code: int, event_code: int, info: str, event: str):
    """
    Solace Event Code 處理器。
    用途：過濾掉正在自動重試的訊號，僅在遇到致命錯誤時觸發 Systemd 重啟。
    """
    
    # Case A: 正常運作或自動恢復中 (忽略)
    # 0:OK, 12:Reconnecting, 13:Reconnected, 16:SubOK
    if event_code in {0, 6, 10, 13, 15, 16, 18}: 
        if event_code == 13: print("    -> ✅ Solace 重連成功，服務恢復運行。")
        return
        
    if event_code == 12: 
        print("    -> ⏳ Solace 正在自動重試，保持服務運行...")
        return
        
    # Case B: 致命錯誤 (觸發退出)
    if event_code in FATAL_CODES:
        print(f"    -> ❌ 偵測到致命錯誤 (Code {event_code})。通知 Systemd 進行重啟...")
        handle_session_down(f"Fatal Event Code {event_code}: {info}")
    
    # Case C: 未知錯誤 (僅記錄)
    print(f"[SOLACE EVENT] Unhandled Error Code {event_code}: {info}")


# ==========================================
# Main Execution
# ==========================================

def main():
    global API_INSTANCE

    # --- 1. 初始化與登入 ---
    # 每次啟動都是全新的實例，確保內存與狀態乾淨
    API_INSTANCE = sj.Shioaji(simulation=True)
    
    print("登入 Shioaji API...")
    try:
        API_INSTANCE.login(
            api_key=SHIOAJI_API_KEY, 
            secret_key=SHIOAJI_SECRET_KEY
        )
        print("✅ 登入成功")
    except Exception as e:
        print(f"❌ 登入失敗: {e}")
        sys.exit(1) # 登入失敗直接讓 Systemd 重試

    # --- 2. 註冊事件處理器 ---
    # 必須優先註冊斷線處理邏輯
    API_INSTANCE.on_session_down(handle_session_down) 
    API_INSTANCE.quote.on_event(quote_event_handler) 

    # --- 3. 定義數據回調 (包含 Kafka 推送與 Log) ---
    
    @API_INSTANCE.on_tick_fop_v1()
    def tick_data_handler(exchange: Exchange, tick: TickFOPv1):
        # A. 執行 Kafka 推送 (最優先)
        process_tick(tick)
        
        # # B. 執行延遲監控與 Log (維持您要求的格式)
        # local_time = datetime.now()
        # event_time = tick.datetime
        # latency_ms = (local_time - event_time).total_seconds() * 1000
        
        # print("-" * 60)
        # print(f"[{tick.code} | {tick.total_volume} Lot] (API RECVD)")
        # print(f"  成交發生時間: {event_time}")
        # print(f"  本機接收時間: {local_time}")
        # print(f"  -> API 接收延遲: {latency_ms:.3f} ms")
        # print(f"Price: {tick.close}, Total Volume: {tick.total_volume}, tick_type: {tick.tick_type}")
        # print("-" * 60)

    @API_INSTANCE.on_bidask_fop_v1()
    def bidask_data_handler(exchange: Exchange, bidask: BidAskFOPv1):
        # A. 執行 Kafka 推送
        process_bidask(bidask)
        
        # # B. 簡潔 Log (維持風格)
        # print(f"BidAsk PUSHED | {str(bidask.datetime)}: Bid: {bidask.bid_price[0]}, Ask: {bidask.ask_price[0]}")

    # --- 4. 訂閱行情 ---
    print("📢 訂閱台指期行情...")
    # 這裡直接訂閱 TXFR1 (近月)，Shioaji 會自動處理換月
    target_contract = API_INSTANCE.Contracts.Futures.TXF.TXFR1
    
    API_INSTANCE.quote.subscribe(target_contract, quote_type=sj.constant.QuoteType.Tick)
    API_INSTANCE.quote.subscribe(target_contract, quote_type=sj.constant.QuoteType.BidAsk)
    
    print(f"✅ 已訂閱: {target_contract.code} ({target_contract.name})")
    print("📡 服務啟動，進入事件循環 (CPU 佔用極低)...")

    # --- 5. 進入事件循環 ---
    try:
        # 使用 asyncio run_forever 取代 while True loop，大幅降低 CPU 使用率
        loop = asyncio.get_event_loop()
        loop.run_forever() 
    except KeyboardInterrupt:
        print("👋 收到停止訊號 (User Interrupt)...")
    except Exception as e:
        print(f"❌ 主程序發生未預期錯誤: {e}")
        sys.exit(1) # 遇到未知錯誤也重啟
    finally:
        # Systemd stop 或 Ctrl+C 都會觸發這裡
        print("⏳ 正在執行優雅退出程序...")
        if API_INSTANCE:
            try:
                print("登出 API...")
                API_INSTANCE.logout()
            except: pass
        
        print("🧹 清空 Kafka 緩衝區...")
        producer.flush() 
        print("✅ 程式結束")

if __name__ == "__main__":
    main()