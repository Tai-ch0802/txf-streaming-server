from confluent_kafka import Consumer, KafkaError
import txf_data_pb2 # 匯入 Protobuf 定義
from config import KAFKA_BOOTSTRAP_SERVERS, TICK_TOPIC, BIDASK_TOPIC

# 價格還原倍數
SCALE = 10000.0

def to_decimal(scaled_int):
    """將 int64 還原回 float"""
    return scaled_int / SCALE

def main():
    # 1. 消費者設定
    conf = {
        'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
        'group.id': 'txf-monitor-group', # 消費者群組 ID
        'auto.offset.reset': 'latest',   # 只聽最新的資料 (Real-time)
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
            msg = consumer.poll(0.1) # 0.1秒 timeout

            if msg is None:
                continue
            if msg.error():
                if msg.error().code() == KafkaError._PARTITION_EOF:
                    continue
                else:
                    print(msg.error())
                    break

            # 3. 收到資料，開始解碼
            topic = msg.topic()
            
            if topic == TICK_TOPIC:
                # --- 解析 Tick ---
                tick = txf_data_pb2.Tick()
                tick.ParseFromString(msg.value()) # <--- 關鍵：二進制轉物件
                
                print(f"[TICK] {tick.code} | "
                      f"時間: {tick.timestamp_ms} | "
                      f"成交價: {to_decimal(tick.close)} | "
                      f"單量: {tick.volume} | "
                      f"總量: {tick.total_volume}")

            elif topic == BIDASK_TOPIC:
                # --- 解析 BidAsk ---
                ba = txf_data_pb2.BidAsk()
                ba.ParseFromString(msg.value()) # <--- 關鍵：二進制轉物件
                
                # 簡單顯示第一檔委託
                bid1 = to_decimal(ba.bid_price[0]) if ba.bid_price else 0
                ask1 = to_decimal(ba.ask_price[0]) if ba.ask_price else 0
                
                print(f"[BA  ] {ba.code} | "
                      f"買一: {bid1} ({ba.bid_volume[0] if ba.bid_volume else 0}) | "
                      f"賣一: {ask1} ({ba.ask_volume[0] if ba.ask_volume else 0})")

    except KeyboardInterrupt:
        pass
    finally:
        consumer.close()

if __name__ == '__main__':
    main()