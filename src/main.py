import sys
import asyncio
from datetime import datetime

import shioaji as sj
from shioaji import TickFOPv1, BidAskFOPv1, Exchange
from confluent_kafka import Producer
import txf_data_pb2  # 匯入 Protobuf 定義

# --- Configuration ---
from config import (
    SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, TICK_TOPIC, BIDASK_TOPIC
)

# --- 全域變數與常量 ---
API_INSTANCE = None 
SCALE = 10000

# 設定致命錯誤碼：當遇到這些代碼時，我們知道 Solace 已經放棄或連線徹底失敗，必須重啟。
# 1: Session Down Error, 2: Connect Failed Error, 8: Assured Delivery Down
FATAL_CODES = {1, 2, 8} 

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'txf-producer-local',
    'acks': '0',
    'linger.ms': 0,
    'compression.type': 'lz4'
}
producer = Producer(KAFKA_CONFIG)


# --- 實用函數 (保持不變) ---

def delivery_report(err, msg):
    if err is not None:
        print(f'Message delivery failed: {err}')

def to_scaled_int(val):
    if val is None: return 0
    return int(val * SCALE)

# --- 核心處理函數 (保持不變，使用物件屬性存取) ---

def process_tick(quote: TickFOPv1):
    try:
        if quote.simtrade == 1: return
        tick = txf_data_pb2.Tick()
        # 欄位填入邏輯 (使用 quote.attribute 方式)
        tick.code = quote.code
        tick.timestamp_ms = int(quote.datetime.timestamp() * 1000)
        tick.open = to_scaled_int(quote.open); tick.underlying_price = to_scaled_int(quote.underlying_price)
        tick.bid_side_total_vol = int(quote.bid_side_total_vol); tick.ask_side_total_vol = int(quote.ask_side_total_vol)
        tick.avg_price = to_scaled_int(quote.avg_price); tick.close = to_scaled_int(quote.close)
        tick.high = to_scaled_int(quote.high); tick.low = to_scaled_int(quote.low)
        tick.amount = int(quote.amount); tick.total_amount = int(quote.total_amount)
        tick.volume = int(quote.volume); tick.total_volume = int(quote.total_volume)
        tick.tick_type = int(quote.tick_type); tick.chg_type = int(quote.chg_type)
        tick.price_chg = to_scaled_int(quote.price_chg); tick.pct_chg = to_scaled_int(quote.pct_chg)
        tick.simtrade = bool(quote.simtrade)

        producer.produce(TICK_TOPIC, key=tick.code.encode('utf-8'), value=tick.SerializeToString(), on_delivery=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f"Error processing tick: {e}")

def process_bidask(quote: BidAskFOPv1):
    try:
        if quote.simtrade == 1: return
        ba = txf_data_pb2.BidAsk()
        # 欄位填入邏輯 (使用 quote.attribute 方式)
        ba.code = quote.code; ba.timestamp_ms = int(quote.datetime.timestamp() * 1000)
        ba.bid_total_vol = int(quote.bid_total_vol); ba.ask_total_vol = int(quote.ask_total_vol)
        ba.underlying_price = to_scaled_int(quote.underlying_price); ba.simtrade = bool(quote.simtrade)

        ba.bid_price.extend([to_scaled_int(x) for x in quote.bid_price])
        ba.bid_volume.extend(quote.bid_volume); ba.diff_bid_vol.extend(quote.diff_bid_vol)
        ba.ask_price.extend([to_scaled_int(x) for x in quote.ask_price])
        ba.ask_volume.extend(quote.ask_volume); ba.diff_ask_vol.extend(quote.diff_ask_vol)
        
        ba.first_derived_bid_price = to_scaled_int(quote.first_derived_bid_price)
        ba.first_derived_ask_price = to_scaled_int(quote.first_derived_ask_price)
        ba.first_derived_bid_vol = int(quote.first_derived_bid_vol)
        ba.first_derived_ask_vol = int(quote.first_derived_ask_vol)

        producer.produce(BIDASK_TOPIC, key=ba.code.encode('utf-8'), value=ba.SerializeToString(), on_delivery=delivery_report)
        producer.poll(0)
    except Exception as e:
        print(f"Error processing bidask: {e}")


# --- 智能退出處理器 ---

def handle_session_down(reason: str = "Retries Timeout"):
    """(Code 1 最終觸發) 處理 Session Down 事件，強制退出。"""
    print(f"🚨 API Session Down Detected (Final): {reason}")
    print("--- 🛑 Terminating to force clean API recreation via Systemd... ---")
    
    # 清理舊狀態
    if API_INSTANCE:
        try: API_INSTANCE.logout()
        except: pass
    
    producer.flush() 
    # 強制退出，Systemd 會偵測到非零退出碼 (1) 並重啟服務
    sys.exit(1)

def quote_event_handler(resp_code: int, event_code: int, info: str, event: str):
    """根據 Solace Event Code 決定是否需要執行強制退出。"""
    
    # 忽略通知類和成功類代碼
    if event_code in {0, 6, 10, 13, 15, 16, 18}: # 成功、重連成功、OK
        if event_code == 13: print("    -> Solace 重連成功，服務恢復運行。")
        return
        
    # 忽略重試中代碼，讓 Solace 繼續嘗試自癒
    if event_code == 12: # RECONNECTING_NOTICE
        print("    -> Solace 正在自動重試，保持服務運行...")
        return
        
    # 遇到致命錯誤代碼，立刻觸發退出
    if event_code in FATAL_CODES:
        print(f"    -> ❌ 偵測到致命錯誤 (Code {event_code})。通知 Systemd 進行重啟...")
        handle_session_down(f"Fatal Event Code {event_code}: {info}")
    
    # 記錄其他不常見的錯誤
    print(f"[SOLACE EVENT] Unhandled Error Code {event_code}: {info}")


# --- 主程式 ---
def main():
    global API_INSTANCE

    # 1. 建立 API 實例與登入
    API_INSTANCE = sj.Shioaji(simulation=True)
    
    print("登入 Shioaji API...")
    API_INSTANCE.login(
        api_key=SHIOAJI_API_KEY, 
        secret_key=SHIOAJI_SECRET_KEY
    )
    print("登入成功")

    # 2. 註冊 Session Down 處理器 (最終退出點)
    API_INSTANCE.on_session_down(handle_session_down) 
    # 註冊 Solace 事件碼處理器 (智能重試決策點)
    API_INSTANCE.quote.on_event(quote_event_handler) 


    # 3. 註冊數據回調函數
    @API_INSTANCE.on_tick_fop_v1()
    def tick_data_handler(exchange:Exchange, tick:TickFOPv1):
        process_tick(tick)
        # Latency check printing (保持不變)
        local_time = datetime.now()
        event_time = tick.datetime
        latency_ms = (local_time - event_time).total_seconds() * 1000
        print("-" * 60)
        print(f"[{tick.code} | {tick.total_volume} Lot]")
        print(f"  事件發生時間: {event_time}")
        print(f"  本機接收時間: {local_time}")
        print(f"-> API 接收延遲: {latency_ms:.3f} ms")
        print(f"Price: {tick.close}, Total Volume: {tick.total_volume}, tick_type: {tick.tick_type}")
        print("-" * 60)

    @API_INSTANCE.on_bidask_fop_v1()
    def bidask_data_handler(exchange:Exchange, bidask:BidAskFOPv1):
        process_bidask(bidask)
        # BidAsk 簡潔打印 (保持不變)
        print(f"BidAsk PUSHED | {str(bidask.datetime)}: Bid: {bidask.bid_price[0]}, Ask: {bidask.ask_price[0]}")


    # 4. 訂閱邏輯
    print("訂閱台指期行情...")
    API_INSTANCE.quote.subscribe(
        API_INSTANCE.Contracts.Futures.TXF.TXFR1,
        quote_type=sj.constant.QuoteType.Tick
    )
    API_INSTANCE.quote.subscribe(
        API_INSTANCE.Contracts.Futures.TXF.TXFR1,
        quote_type=sj.constant.QuoteType.BidAsk
    )
    
    print(f"已訂閱: {API_INSTANCE.Contracts.Futures.TXF.TXFR1.code}")
    print("服務啟動，進入事件循環 (CPU 佔用極低)...")

    # 5. 保持程式運行 (asyncio loop)
    try:
        loop = asyncio.get_event_loop()
        loop.run_forever() 
    except KeyboardInterrupt:
        print("收到停止訊號...")
    except Exception as e:
        print(f"主程序發生錯誤: {e}")
    finally:
        # 確保在程式退出時登出並清空 Kafka 緩衝區
        print("登出並清空 Kafka 緩衝區...")
        API_INSTANCE.logout()
        producer.flush() 
        print("程式結束")

if __name__ == "__main__":
    main()