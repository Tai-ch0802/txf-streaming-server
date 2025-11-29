'''
==========================================
TXF Streaming Producer (Shioaji -> Kafka Protobuf)
------------------------------------------
Description: 連接永豐 Shioaji API 接收台指期 Tick / BidAsk，透過 Protobuf 推送 Kafka
Architecture:
  - Process Management: Systemd
  - Concurrency: asyncio
  - Serialization: Google Protobuf
  - Transport: confluent-kafka
  - Logging: stdout captured by systemd
Author: Garrett & Gemini
Last Updated: 2025-11-28
==========================================
'''

import sys, asyncio, logging
from datetime import datetime
from decimal import Decimal
from typing import Optional
import shioaji as sj
from shioaji import TickFOPv1, BidAskFOPv1
from confluent_kafka import Producer
import txf_data_pb2
from config import (
    SHIOAJI_API_KEY, SHIOAJI_SECRET_KEY, 
    KAFKA_BOOTSTRAP_SERVERS, 
    TICK_TOPIC, BIDASK_TOPIC
)

# ==========================================
# Logging Configuration
# ==========================================
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s [%(levelname)s] %(message)s',
    datefmt='%Y-%m-%d %H:%M:%S',
    handlers=[logging.StreamHandler(sys.stdout)]
)
logger = logging.getLogger("TXF_Producer")

# ==========================================
# Global Constants & Configuration
# ==========================================
API_INSTANCE: Optional[sj.Shioaji] = None  # 全域 API 實例
SCALE = 10000  # 價格縮放倍數 (符合 proto)
FATAL_CODES = {1,2,8}  # 致命錯誤碼

KAFKA_CONFIG = {
    'bootstrap.servers': KAFKA_BOOTSTRAP_SERVERS,
    'client.id': 'txf-producer-hft',
    'acks': '0', 'linger.ms': 0, 'compression.type': 'lz4',
    'queue.buffering.max.kbytes': 131072, 'batch.size': 262144,
    'socket.send.buffer.bytes': 102400, 'socket.receive.buffer.bytes': 102400,
}

try:
    producer = Producer(KAFKA_CONFIG)
except Exception as e:
    logger.critical(f"❌ Kafka Producer Initialization Failed: {e}")
    sys.exit(1)

# ==========================================
# Helper Functions
# ==========================================
def delivery_report(err, msg):
    """Kafka 傳送回調 (僅用於錯誤記錄)"""
    if err: logger.error(f'❌ Message delivery failed: {err}')

def to_scaled_int(val: Optional[Decimal]) -> int:
    """將 Decimal/Float 轉換為 int64 (x10000)"""
    if val is None: return 0
    return int(val * SCALE)

# ==========================================
# Core Processing Logic (Protobuf Packing)
# ==========================================
def process_tick(quote: TickFOPv1):
    """處理 Tick 數據"""
    try:
        if quote.simtrade == 1: return
        tick = txf_data_pb2.Tick()
        tick.code = quote.code
        tick.timestamp_ms = int(quote.datetime.timestamp()*1000)
        tick.tick_type = int(quote.tick_type)
        tick.close = to_scaled_int(quote.close)
        tick.volume = int(quote.volume)
        tick.underlying_price = to_scaled_int(quote.underlying_price)
        tick.total_volume = int(quote.total_volume)

        producer.produce(
            TICK_TOPIC,
            key=tick.code.encode('utf-8'),
            value=tick.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        logger.error(f"❌ Error processing tick: {e}")

def process_bidask(quote: BidAskFOPv1):
    """處理 BidAsk 數據"""
    try:
        if quote.simtrade == 1: return
        ba = txf_data_pb2.BidAsk()
        ba.code = quote.code
        ba.timestamp_ms = int(quote.datetime.timestamp()*1000)
        ba.bid_total_vol = int(quote.bid_total_vol)
        ba.ask_total_vol = int(quote.ask_total_vol)
        ba.bid_price.extend([to_scaled_int(x) for x in quote.bid_price])
        ba.ask_price.extend([to_scaled_int(x) for x in quote.ask_price])
        ba.bid_volume.extend(quote.bid_volume)
        ba.ask_volume.extend(quote.ask_volume)
        ba.diff_bid_vol.extend(quote.diff_bid_vol)
        ba.diff_ask_vol.extend(quote.diff_ask_vol)

        producer.produce(
            BIDASK_TOPIC,
            key=ba.code.encode('utf-8'),
            value=ba.SerializeToString(),
            on_delivery=delivery_report
        )
        producer.poll(0)
    except Exception as e:
        logger.error(f"❌ Error processing bidask: {e}")

# ==========================================
# Session & Event Handling
# ==========================================
def handle_session_down(reason="Retries Timeout"):
    """處理 Session Down 事件，Systemd 會自動重啟"""
    global API_INSTANCE
    logger.critical(f"🚨 API Session Down Detected: {reason}")
    logger.critical("🛑 Terminating to force clean API recreation via Systemd...")
    try: API_INSTANCE.logout()
    except: pass
    producer.flush()
    sys.exit(1)

def quote_event_handler(resp_code, event_code, info, event):
    """Solace Event Code 處理"""
    if event_code in {0,6,10,13,15,16,18}:
        if event_code==13: logger.info("✅ Solace 重連成功")
        return
    if event_code==12:
        logger.warning("⏳ Solace 正在自動重試...")
        return
    if event_code in FATAL_CODES:
        logger.error(f"❌ 致命錯誤 (Code {event_code}): {info}")
        handle_session_down(f"Fatal Event Code {event_code}: {info}")
    logger.warning(f"[SOLACE EVENT] Unhandled Code {event_code}: {info}")

# ==========================================
# Debug Logging
# ==========================================
def log_tick_debug(tick: TickFOPv1):
    """Debug Tick Log"""
    if not logger.isEnabledFor(logging.DEBUG): return
    latency_ms = (datetime.now()-tick.datetime).total_seconds()*1000
    logger.debug(
        "──────────────────────────\n"
        f"Tick {tick.code} ({tick.total_volume} Lot)\n"
        f"成交時間: {tick.datetime}\n"
        f"接收時間: {datetime.now()}\n"
        f"延   遲: {latency_ms:.3f} ms\n"
        f"價   格: {tick.close}, tick_type: {tick.tick_type}\n"
        "──────────────────────────"
    )

def log_bidask_debug(bidask: BidAskFOPv1):
    """Debug BidAsk Log"""
    if not logger.isEnabledFor(logging.DEBUG): return
    logger.debug(
        "──────────────────────────\n"
        f"📊 BidAsk {bidask.datetime}\n"
        f"Bid: {bidask.bid_price[0]}, Ask: {bidask.ask_price[0]}\n"
        "──────────────────────────"
    )

# ==========================================
# Main Async Execution
# ==========================================
async def main_async():
    """
    TXF Streaming Producer 主流程 (Async 版本)
    - 登入 Shioaji API
    - 註冊事件回調 (Session Down / Solace / Tick / BidAsk)
    - 訂閱台指期行情
    - 永遠等待事件回調
    - Ctrl+C 或異常觸發優雅退出
    """
    global API_INSTANCE
    API_INSTANCE = sj.Shioaji(simulation=True)

    # --- API 登入 ---
    logger.info("🔑 登入 Shioaji API...")
    try:
        API_INSTANCE.login(api_key=SHIOAJI_API_KEY, secret_key=SHIOAJI_SECRET_KEY)
        logger.info("✅ 登入成功")
    except Exception as e:
        logger.error(f"❌ 登入失敗: {e}")
        sys.exit(1)

    # --- 註冊回調 ---
    API_INSTANCE.on_session_down(handle_session_down)
    API_INSTANCE.quote.on_event(quote_event_handler)

    @API_INSTANCE.on_tick_fop_v1()
    def tick_data_handler(_, tick):
        process_tick(tick)
        #log_tick_debug(tick)

    @API_INSTANCE.on_bidask_fop_v1()
    def bidask_data_handler(_, bidask):
        process_bidask(bidask)
        #log_bidask_debug(bidask)

    # --- 訂閱行情 ---
    logger.info("⏳ 訂閱台指期行情...")
    target_contract = API_INSTANCE.Contracts.Futures.TXF.TXFR1
    API_INSTANCE.quote.subscribe(target_contract, quote_type=sj.constant.QuoteType.Tick)
    API_INSTANCE.quote.subscribe(target_contract, quote_type=sj.constant.QuoteType.BidAsk)
    logger.info(f"✅ 已訂閱: {target_contract.code} ({target_contract.name})")
    logger.info("🟢 服務已啟動，等待 Tick / BidAsk / 系統事件觸發...")

    # --- 永遠等待事件回調 ---
    stop_event = asyncio.Event()  # 用於 async-friendly Ctrl+C 停止
    try:
        await stop_event.wait()
    except asyncio.CancelledError:
        logger.info("🛑 收到停止訊號 (Ctrl + C)")
    finally:
        # --- 優雅退出流程 ---
        logger.info("⏳ 優雅退出程序...")
        if API_INSTANCE:
            try:
                logger.info("⏳ 登出 API...")
                API_INSTANCE.logout()
            except Exception:
                pass
        logger.info("⏳ 清空 Kafka 緩衝區...")
        producer.flush()
        logger.info("✅ 程式結束")

if __name__ == "__main__":
    asyncio.run(main_async())
