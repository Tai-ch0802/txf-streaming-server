"""
TXF Streaming Consumer (Kafka -> Console Inspector)
---------------------------------------------------
Description: 
    從 Kafka 接收 TXF Tick 與 BidAsk 數據 (Protobuf 格式)，
    進行即時解碼並顯示完整欄位內容。
    主要用於：
    1. 驗證資料正確性 (Data Integrity)
    2. 監控端到端延遲 (End-to-End Latency)
    3. 除錯 (Debugging)
    
Architecture:
    - Deserialization: Google Protobuf
    - Transport: confluent-kafka (Consumer)
    - Output: Stdout (Full verbose mode)

Author: Garrett & Gemini
Last Updated: 2025-11-28
"""

import sys
import time
from datetime import datetime

# --- Third-party Imports ---
from confluent_kafka import Consumer, KafkaError

# --- Local Imports ---
import txf_data_pb2  # Protobuf Definition
from config import (
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC, 
    BIDASK_TOPIC
)

# ==========================================
# Global Constants
# ==========================================

# 價格還原倍數 (需與 Producer 保持一致)
SCALE = 10000.0

# Consumer Group ID (使用獨立 ID 以避免干擾正式服務)
GROUP_ID = 'txf-console-inspector'

# ==========================================
# Helper Functions
# ==========================================

def to_decimal(scaled_int: int) -> float:
    """將 int64 (x10000) 還原回 float。"""
    return scaled_int / SCALE

def format_list(data_list, is_price=False) -> str:
    """
    將列表格式化為易讀字串。
    - is_price=True: 自動除以 SCALE 還原價格
    """
    if not data_list:
        return "[]"
    
    if is_price:
        # 將所有價格還原
        items = [str(to_decimal(x)) for x in data_list]
        return f"[{', '.join(items)}]"
    
    # 一般數值直接顯示
    return str(list(data_list))

# ==========================================
# Main Execution
# ==========================================

def main():
    
    # --- 1. 初始化 Consumer ---
    print("🔧 初始化 Kafka Consumer...")
    
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': GROUP_ID,
        'auto.offset.reset': 'latest',     # 只監控最新數據，不回補
        'enable.auto.commit': True
    }

    try:
        consumer = Consumer(conf)
        consumer.subscribe([TICK_TOPIC, BIDASK_TOPIC])
        
        print(f"🚀 全欄位監控模式啟動 | Broker: {KAFKA_BOOTSTRAP_SERVERS}")
        print(f"📡 監聽 Topics: {TICK_TOPIC}, {BIDASK_TOPIC}")
        print( "⏳ 等待資料中... (按 Ctrl+C 停止)")
        print( "-" * 60)
        
    except Exception as e:
        print(f"❌ Consumer 初始化失敗: {e}")
        sys.exit(1)

    # --- 2. 進入監控迴圈 ---
    try:
        while True:
            # poll(0.5): 測試模式不需要極致低延遲，0.5秒可降低 CPU 空轉
            msg = consumer.poll(0.5)

            if msg is None:
                continue
            
            # --- 錯誤處理 ---
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(f"❌ Kafka Error: {msg.error()}")
                    continue
            
            # --- 3. 計算延遲 ---
            # 記錄 Consumer 收到資料的當下時間 (奈秒 -> 毫秒)
            processing_time_ms = time.time_ns() // 1_000_000
            topic = msg.topic()
            
            # --- 4. 解析與顯示 (Tick) ---
            if topic == TICK_TOPIC:
                tick = txf_data_pb2.Tick()
                tick.ParseFromString(msg.value())
                
                # 計算 E2E 延遲
                latency_ms = processing_time_ms - tick.timestamp_ms
                event_time = datetime.fromtimestamp(tick.timestamp_ms / 1000.0).strftime('%H:%M:%S.%f')[:-3]
                
                print(f"\n⚡ [TICK] {tick.code} @ {event_time} (延遲 {latency_ms}ms)")
                print(f"   ├─ 成交價: {to_decimal(tick.close)}")
                print(f"   ├─ 單量: {tick.volume} | 總量: {tick.total_volume}")
                # 加上文字說明讓 tick_type 更易讀
                type_str = {1: "外盤", 2: "內盤"}.get(tick.tick_type, "未知")
                print(f"   ├─ 內外盤: {tick.tick_type} ({type_str})")
                print(f"   └─ 標的價: {to_decimal(tick.underlying_price)}")

            # --- 5. 解析與顯示 (BidAsk) ---
            elif topic == BIDASK_TOPIC:
                ba = txf_data_pb2.BidAsk()
                ba.ParseFromString(msg.value())
                
                latency_ms = processing_time_ms - ba.timestamp_ms
                event_time = datetime.fromtimestamp(ba.timestamp_ms / 1000.0).strftime('%H:%M:%S.%f')[:-3]
                
                print(f"\n📊 [BID/ASK] {ba.code} @ {event_time} (延遲 {latency_ms}ms)")
                print(f"   ├─ 總委買: {ba.bid_total_vol} | 總委賣: {ba.ask_total_vol}")
                
                # 使用 Helper 格式化列表，保持版面整潔
                print(f"   ├─ [買] 價格: {format_list(ba.bid_price, True)}")
                print(f"   ├─ [買] 數量: {format_list(ba.bid_volume)}")
                print(f"   ├─ [買] 增減: {format_list(ba.diff_bid_vol)}")
                print( "   │")
                print(f"   ├─ [賣] 價格: {format_list(ba.ask_price, True)}")
                print(f"   ├─ [賣] 數量: {format_list(ba.ask_volume)}")
                print(f"   └─ [賣] 增減: {format_list(ba.diff_ask_vol)}")

    except KeyboardInterrupt:
        print("\n🛑 收到停止訊號，監控結束。")
        
    finally:
        print("⏳ 正在關閉 Consumer...")
        consumer.close()
        print("✅ Consumer 已關閉。")

if __name__ == '__main__':
    main()