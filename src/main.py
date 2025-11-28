import sys
import os
import json
import shioaji as sj
from shioaji import TickSTkQuote, BidAskSTkQuote
from datetime import datetime
from decimal import Decimal
from confluent_kafka import Producer
import txf_data_pb2  # 匯入剛剛編譯好的 Protobuf 定義

from config import (
    SHIOAJI_API_KEY, 
    SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC,
    BIDASK_TOPIC
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

def process_tick(quote: dict):
    """處理 Tick 資料並傳送"""
    try:
        tick = txf_data_pb2.Tick()
        
        # 1. 基礎欄位填入
        tick.code = quote.get('code', '')
        # 時間處理：Shioaji datetime 物件轉 Unix Timestamp (毫秒)
        dt = quote.get('datetime')
        if dt:
            tick.timestamp_ms = int(dt.timestamp() * 1000)
            
        # 2. 價格與量 (轉為 Scaled Int)
        tick.open = to_scaled_int(quote.get('open'))
        tick.underlying_price = to_scaled_int(quote.get('underlying_price'))
        tick.bid_side_total_vol = int(quote.get('bid_side_total_vol', 0))
        tick.ask_side_total_vol = int(quote.get('ask_side_total_vol', 0))
        tick.avg_price = to_scaled_int(quote.get('avg_price'))
        tick.close = to_scaled_int(quote.get('close'))
        tick.high = to_scaled_int(quote.get('high'))
        tick.low = to_scaled_int(quote.get('low'))
        
        tick.amount = int(quote.get('amount', 0))
        tick.total_amount = int(quote.get('total_amount', 0))
        tick.volume = int(quote.get('volume', 0))
        tick.total_volume = int(quote.get('total_volume', 0))
        
        tick.tick_type = int(quote.get('tick_type', 0))
        tick.chg_type = int(quote.get('chg_type', 0))
        tick.price_chg = to_scaled_int(quote.get('price_chg'))
        tick.pct_chg = to_scaled_int(quote.get('pct_chg'))
        tick.simtrade = bool(quote.get('simtrade', 0))

        # 3. 序列化並發送 (Key=Code 以保證順序)
        producer.produce(
            TICK_TOPIC,
            key=tick.code.encode('utf-8'),
            value=tick.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0) # 觸發 callback，但不阻塞

    except Exception as e:
        print(f"Error processing tick: {e}")

def process_bidask(quote: dict):
    """處理 BidAsk 資料並傳送"""
    try:
        ba = txf_data_pb2.BidAsk()
        
        ba.code = quote.get('code', '')
        dt = quote.get('datetime')
        if dt:
            ba.timestamp_ms = int(dt.timestamp() * 1000)
            
        ba.bid_total_vol = int(quote.get('bid_total_vol', 0))
        ba.ask_total_vol = int(quote.get('ask_total_vol', 0))
        ba.underlying_price = to_scaled_int(quote.get('underlying_price'))
        ba.simtrade = bool(quote.get('simtrade', 0))

        # 處理 List (五檔報價)
        # 注意：Shioaji 傳回的 List 內元素需逐一轉換
        if 'bid_price' in quote:
            ba.bid_price.extend([to_scaled_int(x) for x in quote['bid_price']])
        if 'bid_volume' in quote:
            ba.bid_volume.extend(quote['bid_volume'])
        if 'diff_bid_vol' in quote:
            ba.diff_bid_vol.extend(quote['diff_bid_vol'])
            
        if 'ask_price' in quote:
            ba.ask_price.extend([to_scaled_int(x) for x in quote['ask_price']])
        if 'ask_volume' in quote:
            ba.ask_volume.extend(quote['ask_volume'])
        if 'diff_ask_vol' in quote:
            ba.diff_ask_vol.extend(quote['diff_ask_vol'])

        producer.produce(
            BIDASK_TOPIC,
            key=ba.code.encode('utf-8'),
            value=ba.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0)

    except Exception as e:
        print(f"Error processing bidask: {e}")

# --- Shioaji Callback ---
def quote_callback(topic: str, quote: dict):
    """
    Shioaji 的回調函數。
    Topic 格式範例: 'MKT/id/FOP/TXF/202512' (這是 Shioaji 的 topic，不是 Kafka 的)
    """
    # 簡單判斷是 Tick 還是 BidAsk
    # Shioaji 的 quote dict 中，Tick 通常有 'tick_type'，BidAsk 有 'bid_price'
    
    # 這裡的 quote 已經是 dict 格式
    if 'tick_type' in quote:
        # print(f"Tick received: {quote['code']} at {quote['datetime']}")
        process_tick(quote)
    elif 'bid_price' in quote:
        # print(f"BidAsk received: {quote['code']} at {quote['datetime']}")
        process_bidask(quote)

# --- 主程式 ---
def main():
    api = sj.Shioaji()
    
    print("登入 Shioaji API...")
    accounts = api.login(
        api_key=SHIOAJI_API_KEY, 
        secret_key=SHIOAJI_SECRET_KEY
    )
    print("登入成功")

    # 設定回調函數
    api.set_context(quote_callback)

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