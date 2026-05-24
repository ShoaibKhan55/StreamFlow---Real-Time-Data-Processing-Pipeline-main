"""
StreamFlow - Crypto Data Producer
Streams real-time cryptocurrency data from Binance to Kafka
"""

import websocket
import json
from kafka import KafkaProducer
from datetime import datetime
import logging
import time

# Configure logging
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class CryptoProducer:
    def __init__(self, kafka_server='localhost:9092'):
        """Initialize Kafka producer"""
        self.kafka_server = kafka_server
        self.topic = 'crypto_data'
        self.message_count = 0
        self.producer = None
        
        self._init_kafka_producer()
    
    def _init_kafka_producer(self):
        """Initialize Kafka producer with retry logic"""
        max_retries = 5
        retry_count = 0
        
        while retry_count < max_retries:
            try:
                self.producer = KafkaProducer(
                    bootstrap_servers=[self.kafka_server],
                    value_serializer=lambda v: json.dumps(v).encode('utf-8'),
                    acks='all',
                    retries=3,
                    max_in_flight_requests_per_connection=1
                )
                logger.info("✅ Kafka producer initialized successfully")
                return
            except Exception as e:
                retry_count += 1
                logger.warning(f"⚠️  Kafka connection attempt {retry_count}/{max_retries} failed: {e}")
                if retry_count < max_retries:
                    time.sleep(5)
                else:
                    logger.error("❌ Failed to connect to Kafka after multiple attempts")
                    raise
                
    def on_message(self, ws, message):
        """Handle incoming WebSocket messages"""
        try:
            msg = json.loads(message)
            # Combined stream format → actual payload is inside "data"
            if "data" not in msg:
                return # ignore non-data messages
            
            trade = msg["data"]
            
            # Parse Binance trade data
            crypto_data = {
                'symbol': trade['s'],
                'price': float(trade['p']),
                'quantity': float(trade['q']),
                'timestamp': datetime.fromtimestamp(trade['T'] / 1000).isoformat(),
                'trade_id': trade['t'],
                'buyer_is_maker': trade['m']
            }
            
            # Send to Kafka
            future = self.producer.send(self.topic, value=crypto_data)
            future.get(timeout=10)  # Wait for confirmation
            
            self.message_count += 1
            
            # Log every 20 messages to avoid spam
            if self.message_count % 20 == 0:
                logger.info(
                    f"📊 [{self.message_count}] {crypto_data['symbol']}: "
                    f"${crypto_data['price']:,.2f}"
                )
            
        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
    
    def on_error(self, ws, error):
        """Handle WebSocket errors"""
        logger.error(f"❌ WebSocket error: {error}")
    
    def on_close(self, ws, close_status_code, close_msg):
        """Handle WebSocket close"""
        logger.info(f"🔌 WebSocket closed: {close_msg}")
        logger.info(f"✅ Total messages sent: {self.message_count}")
    
    def on_open(self, ws):
        """Handle WebSocket open"""
        logger.info("=" * 70)
        logger.info("🚀 STREAMFLOW CRYPTO PRODUCER STARTED")
        logger.info("=" * 70)
        logger.info("🔗 WebSocket connection established")
        logger.info(f"📡 Kafka topic: {self.topic}")
        logger.info("⚡ Streaming real-time crypto data...")
        logger.info("=" * 70)
    
    def start(self, symbols=None):
        """
        Start streaming cryptocurrency data
        
        Args:
            symbols: List of trading pairs (default: BTC, ETH, BNB, SOL, ADA)
        """
        if symbols is None:
            symbols = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'adausdt']
        
        # Create WebSocket URL for multiple symbols
        streams = '/'.join([f"{symbol}@trade" for symbol in symbols])
        ws_url = f"wss://stream.binance.com:9443/stream?streams={streams}"
        
        logger.info(f"📌 Streaming: {', '.join([s.upper() for s in symbols])}")
        
        # Create WebSocket connection
        ws = websocket.WebSocketApp(
            ws_url,
            on_open=self.on_open,
            on_message=self.on_message,
            on_error=self.on_error,
            on_close=self.on_close
        )
        
        try:
            # Run forever (blocking)
            ws.run_forever()
        except KeyboardInterrupt:
            logger.info("\n🛑 Producer stopped by user")
        finally:
            if self.producer:
                self.producer.close()
                logger.info("✅ Kafka producer closed")


def main():
    """Main entry point"""
    print("\n" + "=" * 70)
    print("🔥 STREAMFLOW - REAL-TIME CRYPTO DATA PRODUCER")
    print("=" * 70)
    print("📌 Data Source: Binance WebSocket (FREE)")
    print("📌 Message Broker: Apache Kafka")
    print("📌 Press Ctrl+C to stop")
    print("=" * 70 + "\n")
    
    # Configuration
    KAFKA_SERVER = 'localhost:9092'
    CRYPTO_SYMBOLS = ['btcusdt', 'ethusdt', 'bnbusdt', 'solusdt', 'adausdt']
    
    try:
        producer = CryptoProducer(kafka_server=KAFKA_SERVER)
        producer.start(symbols=CRYPTO_SYMBOLS)
    except Exception as e:
        logger.error(f"❌ Producer failed: {e}")
        print("\n⚠️  TROUBLESHOOTING:")
        print("1. Make sure Docker containers are running: docker-compose ps")
        print("2. Check Kafka logs: docker-compose logs kafka")
        print("3. Verify network: docker network ls")


if __name__ == "__main__":
    main()