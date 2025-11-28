import sys
from confluent_kafka import Consumer, KafkaError
import txf_data_pb2 # 匯入 Protobuf 定義
from config import KAFKA_BOOTSTRAP_SERVERS, TICK_TOPIC, BIDASK_TOPIC
import time
from datetime import datetime # 需要用來轉換 Unix Timestamp

# 價格還原倍數
SCALE = 10000.0

def to_decimal(scaled_int):
    """將 int64 還原回 float"""
    return scaled_int / SCALE

def main():
    # 1. 消費者設定
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'txf-monitor-group',
        'auto.offset.reset': 'latest',
        'enable.auto.commit': True
    }

    consumer = Consumer(conf)

    # 2. 訂閱兩個 Topic
    consumer.subscribe([TICK_TOPIC, BIDASK_TOPIC])
    print(f"正在監聽 Kafka: {KAFKA_BOOTSTRAP_SERVERS}")
    print(f"訂閱 Topics: {TICK_TOPIC}, {BIDASK_TOPIC}")
    print("等待資料中... (按 Ctrl+C 停止)")

    try:
        while True:
            msg = consumer.poll(0.1)

            if msg is None or msg.error():
                continue
            
            # 獲取處理時間（當前時間，毫秒）
            # 這是 Consumer 收到這筆資料的當下時間
            processing_time_ms = int(time.time() * 1000)

            topic = msg.topic()
            
            if topic == TICK_TOPIC:
                # --- 解析 Tick ---
                tick = txf_data_pb2.Tick()
                tick.ParseFromString(msg.value())
                
                # 從 Protobuf 獲取事件時間（毫秒）
                event_time_ms = tick.timestamp_ms
                
                # 計算 端到端延遲 (E2E Latency)
                e2e_latency_ms = processing_time_ms - event_time_ms
                
                # 還原時間字串以供閱讀
                event_dt = datetime.fromtimestamp(event_time_ms / 1000)

                print(f"[TICK] {tick.code} | "
                      f"時間: {event_dt.strftime('%H:%M:%S.%f')[:-3]} | "
                      f"成交價: {to_decimal(tick.close)} | "
                      f"延遲: {e2e_latency_ms} ms") # <--- 最終延遲輸出

            elif topic == BIDASK_TOPIC:
                # --- 解析 BidAsk ---
                ba = txf_data_pb2.BidAsk()
                ba.ParseFromString(msg.value())
                
                e2e_latency_ms = processing_time_ms - ba.timestamp_ms
                
                bid1 = to_decimal(ba.bid_price[0]) if ba.bid_price else 0
                ask1 = to_decimal(ba.ask_price[0]) if ba.ask_price else 0
                
                print(f"[BA  ] {ba.code} | "
                      f"買一: {bid1} ({ba.bid_volume[0] if ba.bid_volume else 0}) | "
                      f"賣一: {ask1} ({ba.ask_volume[0] if ba.ask_volume else 0}) | "
                      f"延遲: {e2e_latency_ms} ms") # <--- 最終延遲輸出

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()