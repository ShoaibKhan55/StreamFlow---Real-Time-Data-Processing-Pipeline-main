"""
StreamFlow - Stream Processor
==============================
Consumes real-time cryptocurrency trade data from the Kafka topic
'crypto_data', computes rolling metrics (moving averages, 1-minute
price change), and persists every record to PostgreSQL.

Cleanup of records older than 7 days is handled by the dedicated
Apache Airflow DAG at  dags/cleanup_dag.py  (runs daily at 00:00 UTC).
Do NOT call cleanup_old_data() from here in production — the DAG owns
that responsibility.
"""

from kafka import KafkaConsumer
import json
from sqlalchemy import create_engine, Column, Integer, String, Float, DateTime, Boolean, text
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker
from datetime import datetime
import logging
from collections import defaultdict
import time

# ---------------------------------------------------------------------------
# Logging
# ---------------------------------------------------------------------------
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)

Base = declarative_base()


# ---------------------------------------------------------------------------
# ORM Model
# ---------------------------------------------------------------------------
class CryptoData(Base):
    """Database model for cryptocurrency trade data."""
    __tablename__ = 'crypto_data'

    id              = Column(Integer,   primary_key=True, autoincrement=True)
    symbol          = Column(String(20), nullable=False,  index=True)
    price           = Column(Float,      nullable=False)
    quantity        = Column(Float)
    timestamp       = Column(DateTime,   nullable=False,  index=True)
    trade_id        = Column(String(50))
    buyer_is_maker  = Column(Boolean)
    moving_avg_5    = Column(Float)
    moving_avg_20   = Column(Float)
    price_change_1m = Column(Float)


# ---------------------------------------------------------------------------
# Stream Processor
# ---------------------------------------------------------------------------
class StreamProcessor:
    def __init__(self, kafka_server: str = 'localhost:9092', db_url: str = None):
        self.kafka_server  = kafka_server
        self.topic         = 'crypto_data'
        self.message_count = 0

        if db_url is None:
            db_url = 'postgresql://admin:admin123@localhost:5432/streamflow_db'

        self._init_database(db_url)
        self._init_kafka_consumer()
        self.price_buffers: dict = defaultdict(list)

    # ------------------------------------------------------------------
    # Initialisation helpers
    # ------------------------------------------------------------------

    def _init_database(self, db_url: str) -> None:
        try:
            self.engine = create_engine(db_url, echo=False)
            Base.metadata.create_all(self.engine)
            Session = sessionmaker(bind=self.engine)
            self.session = Session()
            logger.info("✅ Database initialised successfully")
        except Exception as e:
            logger.error(f"❌ Database initialisation failed: {e}")
            raise

    def _init_kafka_consumer(self) -> None:
        max_retries = 5
        for attempt in range(1, max_retries + 1):
            try:
                self.consumer = KafkaConsumer(
                    self.topic,
                    bootstrap_servers=[self.kafka_server],
                    value_deserializer=lambda m: json.loads(m.decode('utf-8')),
                    auto_offset_reset='latest',
                    enable_auto_commit=True,
                    group_id='streamflow-processor'
                )
                logger.info("✅ Kafka consumer initialised successfully")
                return
            except Exception as e:
                logger.warning(
                    f"⚠️  Kafka connection attempt {attempt}/{max_retries} failed: {e}"
                )
                if attempt < max_retries:
                    time.sleep(5)
                else:
                    logger.error("❌ Failed to connect to Kafka after all retries")
                    raise

    # ------------------------------------------------------------------
    # Metric helpers
    # ------------------------------------------------------------------

    def calculate_moving_average(self, symbol: str, price: float, period: int = 5) -> float:
        key = f"{symbol}_{period}"
        buf = self.price_buffers[key]
        buf.append(price)
        if len(buf) > period:
            buf.pop(0)
        return sum(buf) / len(buf) if buf else price

    def calculate_price_change(self, symbol: str, current_price: float) -> float:
        history = self.price_buffers[f"{symbol}_history"]
        if not history:
            return 0.0
        old_price = history[0]['price']
        if old_price > 0:
            return round(((current_price - old_price) / old_price) * 100, 2)
        return 0.0

    # ------------------------------------------------------------------
    # Cleanup (BUG FIXED — both queries share the same open connection)
    # NOTE: In production this is driven by the Airflow DAG, not called here.
    # ------------------------------------------------------------------

    def cleanup_old_data(self) -> int:
        """
        Delete records older than 7 days and log resulting DB size.

        BUG FIX: original code used `conn` outside its `with` block,
        causing InvalidRequestError because the connection was already
        closed. Both the DELETE and pg_database_size() query now execute
        inside the same `with engine.begin() as conn:` context.
        """
        try:
            with self.engine.begin() as conn:
                # Step 1 — delete stale rows
                delete_result = conn.execute(text("""
                    DELETE FROM crypto_data
                    WHERE timestamp < NOW() - INTERVAL '7 days'
                """))
                deleted_count = delete_result.rowcount

                # Step 2 — read DB size (same connection, still open)
                size_result = conn.execute(text("""
                    SELECT pg_size_pretty(pg_database_size('streamflow_db'))
                """))
                db_size = size_result.fetchone()[0]

            if deleted_count > 0:
                logger.info(f"🗑️  Cleaned up {deleted_count:,} old records (>7 days)")
                logger.info(f"💾 Database size after cleanup: {db_size}")
            else:
                logger.info("✅ No old data to clean up (all data < 7 days old)")
                logger.info(f"💾 Current database size: {db_size}")

            return deleted_count

        except Exception as e:
            logger.error(f"❌ Cleanup error: {e}")
            return 0

    # ------------------------------------------------------------------
    # Core processing loop
    # ------------------------------------------------------------------

    def process_message(self, message: dict) -> None:
        try:
            symbol = message['symbol']
            price  = message['price']

            ma_5         = self.calculate_moving_average(symbol, price, period=5)
            ma_20        = self.calculate_moving_average(symbol, price, period=20)
            price_change = self.calculate_price_change(symbol, price)

            history = self.price_buffers[f"{symbol}_history"]
            history.append({'price': price, 'time': datetime.now()})
            if len(history) > 60:
                history.pop(0)

            record = CryptoData(
                symbol          = symbol,
                price           = price,
                quantity        = message.get('quantity'),
                timestamp       = datetime.fromisoformat(message['timestamp']),
                trade_id        = str(message.get('trade_id', '')),
                buyer_is_maker  = message.get('buyer_is_maker'),
                moving_avg_5    = round(ma_5, 2),
                moving_avg_20   = round(ma_20, 2),
                price_change_1m = price_change,
            )

            self.session.add(record)
            self.session.commit()
            self.message_count += 1

            if self.message_count % 20 == 0:
                logger.info(
                    f"💾 [{self.message_count}] {symbol}: "
                    f"Price=${price:,.2f} | "
                    f"MA5=${ma_5:,.2f} | "
                    f"Change={price_change:+.2f}%"
                )

        except Exception as e:
            logger.error(f"❌ Error processing message: {e}")
            self.session.rollback()

    def start_processing(self) -> None:
        logger.info("=" * 70)
        logger.info("🚀 STREAMFLOW STREAM PROCESSOR STARTED")
        logger.info("=" * 70)
        logger.info(f"📡 Kafka topic : {self.topic}")
        logger.info("💾 Database    : PostgreSQL")
        logger.info("🧹 Cleanup     : Managed by Airflow DAG (daily 00:00 UTC)")
        logger.info("⚡ Processing real-time data...")
        logger.info("=" * 70)

        try:
            for message in self.consumer:
                self.process_message(message.value)
        except KeyboardInterrupt:
            logger.info("\n🛑 Processor stopped by user")
        except Exception as e:
            logger.error(f"❌ Processor error: {e}")
        finally:
            self.consumer.close()
            self.session.close()
            logger.info(
                f"✅ Processor closed. Total messages processed: {self.message_count}"
            )


# ---------------------------------------------------------------------------
# Entry point
# ---------------------------------------------------------------------------
def main():
    print("\n" + "=" * 70)
    print("🔥 STREAMFLOW - STREAM PROCESSOR")
    print("=" * 70)
    print("📌 Input  : Kafka Topic (crypto_data)")
    print("📌 Output : PostgreSQL Database")
    print("📌 Cleanup: Apache Airflow DAG (daily at 00:00 UTC)")
    print("📌 Press Ctrl+C to stop")
    print("=" * 70 + "\n")

    KAFKA_SERVER = 'localhost:9092'
    DB_URL       = 'postgresql://admin:admin123@localhost:5432/streamflow_db'

    try:
        processor = StreamProcessor(kafka_server=KAFKA_SERVER, db_url=DB_URL)
        processor.start_processing()
    except Exception as e:
        logger.error(f"❌ Processor failed: {e}")
        print("\n⚠️  TROUBLESHOOTING:")
        print("1. Make sure Docker containers are running: docker-compose ps")
        print("2. Check if producer is sending data")
        print("3. Verify database connection: docker-compose logs postgres")


if __name__ == "__main__":
    main()
