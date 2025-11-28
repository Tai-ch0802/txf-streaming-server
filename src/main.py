from datetime import datetime

import shioaji as sj
from shioaji import TickFOPv1, BidAskFOPv1, Exchange
from confluent_kafka import Producer
import txf_data_pb2  # 匯入剛剛編譯好的 Protobuf 定義

from config import (
    SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, TICK_TOPIC, BIDASK_TOPIC
)


# Kafka 設定
KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'txf-producer-local',
    'acks': '0',              # 0: 不等待 Server 確認 (最快)，若要安全改成 '1'
    'linger.ms': 0,           # 立即發送，不等待批次
    'compression.type': 'lz4' # 高效壓縮
}

# 價格縮放倍數 (配合 .proto 定義)
SCALE = 10000

# --- 初始化 Kafka Producer ---
producer = Producer(KAFKA_CONFIG)

def delivery_report(err, msg):
    """(非必要) 用來除錯，確認資料有送出去"""
    if err is not None:
        print(f'Message delivery failed: {err}')
    # else:
    #     print(f'Message delivered to {msg.topic()}')

# --- 高效轉換函數 ---
def to_scaled_int(val):
    """將 Decimal/Float 轉為 int64 (x10000)"""
    if val is None: return 0
    return int(val * SCALE)

def process_tick(quote: TickFOPv1):
    """處理 Tick 資料並傳送 (直接使用 Shioaji 物件)"""
    try:
        # [過濾試撮合] 如果是 simtrade，直接跳過 (極致效率要求)
        if quote.simtrade == 1:
            return

        tick = txf_data_pb2.Tick()
        
        # 1. 基礎欄位填入 (直接使用點號存取)
        tick.code = quote.code
        # 時間處理：物件屬性
        tick.timestamp_ms = int(quote.datetime.timestamp() * 1000)
            
        # 2. 價格與量 (直接使用屬性，並轉為 Scaled Int)
        tick.open = to_scaled_int(quote.open)
        tick.underlying_price = to_scaled_int(quote.underlying_price)
        tick.bid_side_total_vol = int(quote.bid_side_total_vol)
        tick.ask_side_total_vol = int(quote.ask_side_total_vol)
        tick.avg_price = to_scaled_int(quote.avg_price)
        tick.close = to_scaled_int(quote.close)
        tick.high = to_scaled_int(quote.high)
        tick.low = to_scaled_int(quote.low)
        
        tick.amount = int(quote.amount)
        tick.total_amount = int(quote.total_amount)
        tick.volume = int(quote.volume)
        tick.total_volume = int(quote.total_volume)
        
        tick.tick_type = int(quote.tick_type)
        tick.chg_type = int(quote.chg_type)
        tick.price_chg = to_scaled_int(quote.price_chg)
        tick.pct_chg = to_scaled_int(quote.pct_chg)
        tick.simtrade = bool(quote.simtrade)

        # 3. 序列化並發送
        producer.produce(
            TICK_TOPIC,
            key=tick.code.encode('utf-8'),
            value=tick.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"Error processing tick: {e}")

def process_bidask(quote: BidAskFOPv1):
    """處理 BidAsk 資料並傳送 (直接使用 Shioaji 物件)"""
    try:
        # [過濾試撮合]
        if quote.simtrade == 1:
            return
            
        ba = txf_data_pb2.BidAsk()
        
        ba.code = quote.code
        ba.timestamp_ms = int(quote.datetime.timestamp() * 1000)
            
        ba.bid_total_vol = int(quote.bid_total_vol)
        ba.ask_total_vol = int(quote.ask_total_vol)
        ba.underlying_price = to_scaled_int(quote.underlying_price)
        ba.simtrade = bool(quote.simtrade)

        # 處理 List (直接存取屬性，並在 extend 時完成 Decimal 轉 int64)
        # 這裡不需要檢查 if 'bid_price' in quote:，因為物件屬性一定存在，只是列表可能為空
        ba.bid_price.extend([to_scaled_int(x) for x in quote.bid_price])
        ba.bid_volume.extend(quote.bid_volume)
        ba.diff_bid_vol.extend(quote.diff_bid_vol)
            
        ba.ask_price.extend([to_scaled_int(x) for x in quote.ask_price])
        ba.ask_volume.extend(quote.ask_volume)
        ba.diff_ask_vol.extend(quote.diff_ask_vol)
        
        # 處理衍生一檔價格
        ba.first_derived_bid_price = to_scaled_int(quote.first_derived_bid_price)
        ba.first_derived_ask_price = to_scaled_int(quote.first_derived_ask_price)
        ba.first_derived_bid_vol = int(quote.first_derived_bid_vol)
        ba.first_derived_ask_vol = int(quote.first_derived_ask_vol)

        producer.produce(
            BIDASK_TOPIC,
            key=ba.code.encode('utf-8'),
            value=ba.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"Error processing bidask: {e}")


# --- 主程式 ---
def main():
    api = sj.Shioaji(simulation=True)
    
    print("登入 Shioaji API...")
    api.login(
        api_key=SHIOAJI_API_KEY, 
        secret_key=SHIOAJI_SECRET_KEY
    )
    print("登入成功")

    # 設定回調函數
    @api.on_tick_fop_v1()
    def tick_data_handler(exchange:Exchange, tick:TickFOPv1): # <class 'tick.TickFOPv1'>
        process_tick(tick)

        local_time = datetime.now()
        event_time = tick.datetime
        # 計算延遲 (timedelta)
        latency_timedelta = local_time - event_time
        
        # 將 timedelta 轉換為毫秒 (ms)
        latency_ms = latency_timedelta.total_seconds() * 1000
        print("-" * 60)
        print(f"[{tick.code} | {tick.total_volume} Lot]")
        print(f"  事件發生時間: {event_time}")
        print(f"  本機接收時間: {local_time}")
        print(f"  -> API 接收延遲: {latency_ms:.3f} ms") # 保留三位小數
        print(f"Price: {tick.close}, Total Volume: {tick.total_volume}, tick_type: {tick.tick_type}")
        print("-" * 60)

    @api.on_bidask_fop_v1()
    def bidask_data_handler(exchange:Exchange, bidask:BidAskFOPv1): # <class 'bidask.BidAskFOPv1'>
        process_bidask(bidask)
        print(f"{str(bidask.datetime)}: First Bid: {bidask.bid_price[0]}, First Ask: {bidask.ask_price[0]}, Bid Total Vol: {bidask.bid_total_vol}, Ask Total Vol: {bidask.ask_total_vol}")


    # 訂閱台指期 (這裡以 TXF 近月為例，你可以依需求修改)
    # 這裡示範訂閱最近的台指期
    print("訂閱台指期行情...")
    
    # 訂閱 TXF 熱門月 (近月)
    api.quote.subscribe(
        api.Contracts.Futures.TXF.TXFR1,
        quote_type=sj.constant.QuoteType.Tick
    )
    api.quote.subscribe(
        api.Contracts.Futures.TXF.TXFR1,
        quote_type=sj.constant.QuoteType.BidAsk
    )
    
    print(f"已訂閱: {api.Contracts.Futures.TXF.TXFR1.code}")
    print(f"資料推送至 Kafka: {KAFKA_CONFIG['bootstrap.servers']}")
    print(f"Tick   資料推送至 Topic: {TICK_TOPIC}")
    print(f"BidAsk 資料推送至 Topic: {BIDASK_TOPIC}")
    print("按 Ctrl+C 結束...")

    # 保持程式運行
    try:
        import time
        while True:
            time.sleep(1)
    except KeyboardInterrupt:
        print("停止中...")
        api.logout()
        producer.flush() # 確保所有訊息都送出
        print("程式結束")

if __name__ == "__main__":
    main()