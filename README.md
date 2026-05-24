# 📊 StreamFlow — Real-Time Data Processing Pipeline

A production-style real-time cryptocurrency data pipeline that ingests live trade data from **Binance**, streams it through **Apache Kafka**, processes and stores it in **PostgreSQL**, visualises it in a **Streamlit** dashboard, and automatically cleans up stale data every night via an **Apache Airflow** DAG.

---

## 🏗️ Architecture

```
┌─────────────────────────────────────────────────────────────────┐
│                        StreamFlow Pipeline                      │
│                                                                 │
│  Binance WebSocket                                              │
│  (BTC, ETH, BNB,  ──►  Kafka  ──►  Stream     ──►  PostgreSQL  │
│   SOL, ADA)              │        Processor        (crypto_data)│
│                          │                              │       │
│                          │                         Streamlit    │
│                          │                         Dashboard    │
│                          │                                      │
│                          └──  Airflow DAG (daily cleanup) ──►  │
└─────────────────────────────────────────────────────────────────┘
```

### Component Summary

| Component | Technology | Role |
|-----------|-----------|------|
| **Producer** | Python + `websocket-client` | Streams live trades from Binance → Kafka |
| **Message Broker** | Apache Kafka + Zookeeper | Decouples producer from consumer |
| **Processor** | Python + `kafka-python` + SQLAlchemy | Consumes Kafka, computes metrics, writes to DB |
| **Database** | PostgreSQL 16 | Persists all processed trade records |
| **Dashboard** | Streamlit + Plotly | Real-time visualisation with auto-refresh |
| **Scheduler** | Apache Airflow 2.9 | Daily cleanup DAG (deletes records > 7 days) |
| **DB Admin** | pgAdmin 4 | Optional web UI for inspecting PostgreSQL |

---

## 📁 Project Structure

```
StreamFlow/
├── README.md
├── requirements.txt
└── streamflow/
    ├── docker-compose.yml          # All infrastructure services
    ├── producers/
    │   └── crypto_producer.py      # Binance → Kafka producer
    ├── consumers/
    │   └── stream_processor.py     # Kafka → PostgreSQL consumer
    ├── dash/
    │   └── dash.py                 # Streamlit real-time dashboard
    ├── dags/
    │   └── cleanup_dag.py          # Airflow DAG: daily 7-day cleanup
    └── airflow/
        ├── init-airflow-db.sql     # Creates airflow_db on first boot
        ├── logs/                   # Airflow task logs (auto-created)
        └── plugins/                # Custom Airflow plugins (empty)
```

---

## 🚀 Quick Start

### Prerequisites

- [Docker Desktop](https://www.docker.com/products/docker-desktop/) (or Docker + Docker Compose)
- Python 3.10+ (for running producer/processor locally)

### 1 — Start all infrastructure services

```bash
cd streamflow/
docker-compose up -d
```

This starts: **Zookeeper**, **Kafka**, **PostgreSQL**, **pgAdmin**, **Airflow scheduler**, and **Airflow webserver**.

Wait ~30 seconds for all services to become healthy:

```bash
docker-compose ps
```

All services should show `Up` or `healthy`.

### 2 — Install Python dependencies

```bash
pip install -r requirements.txt
```

### 3 — Start the Kafka producer

```bash
python streamflow/producers/crypto_producer.py
```

You should see live trade messages for BTC, ETH, BNB, SOL, ADA streaming in.

### 4 — Start the stream processor (new terminal)

```bash
python streamflow/consumers/stream_processor.py
```

The processor reads from Kafka, computes rolling metrics, and writes to PostgreSQL.

### 5 — Launch the dashboard (new terminal)

```bash
streamlit run streamflow/dash/dash.py
```

Open **http://localhost:8501** in your browser.

---

## 🌐 Service URLs

| Service | URL | Credentials |
|---------|-----|-------------|
| Streamlit Dashboard | http://localhost:8501 | — |
| Airflow UI | http://localhost:8080 | `admin` / `admin123` |
| pgAdmin | http://localhost:5050 | `admin@streamflow.com` / `admin123` |
| Kafka | localhost:9092 | — |
| PostgreSQL | localhost:5432 | `admin` / `admin123` |

---

## 🧹 Data Cleanup — Airflow DAG

Stale data (older than 7 days) is removed automatically by the
`streamflow_daily_cleanup` DAG, which runs every night at **00:00 UTC**.

### DAG task order

```
start
  └─► check_db_connection   — fast-fail if PostgreSQL is unreachable
        └─► count_old_records    — counts rows > 7 days (pushes to XCom)
              └─► delete_old_records  — bulk DELETE with 7-day cutoff
                    └─► log_database_stats — logs DB size & live row count
                          └─► vacuum_table     — VACUUM ANALYZE to reclaim disk
                                └─► end
```

### Viewing DAG runs

1. Open **http://localhost:8080**
2. Log in with `admin` / `admin123`
3. Find **`streamflow_daily_cleanup`** in the DAG list
4. Click it to see run history, task logs, and XCom values

### Triggering manually (for testing)

```bash
docker exec airflow-scheduler airflow dags trigger streamflow_daily_cleanup
```

Or click the **▶ Trigger DAG** button in the Airflow UI.

---

## 📊 Dashboard Features

| Feature | Details |
|---------|---------|
| **Symbol selector** | Switch between BTC, ETH, BNB, SOL, ADA |
| **Auto-refresh** | Configurable 1–30 second interval |
| **Price + MA chart** | Price, MA(5), MA(20) with volume subplot |
| **Metric cards** | Current price, MA(5), MA(20), trade count, last update time |
| **Recent trades table** | Last 20 records with formatted values |

---

## 📡 Data Schema

### Kafka message (`crypto_data` topic)

```json
{
  "symbol": "BTCUSDT",
  "price": 65432.10,
  "quantity": 0.00123,
  "timestamp": "2025-02-05T10:38:00.123",
  "trade_id": 3428794521,
  "buyer_is_maker": false
}
```

### PostgreSQL table (`crypto_data`)

| Column | Type | Description |
|--------|------|-------------|
| `id` | INTEGER | Auto-increment primary key |
| `symbol` | VARCHAR(20) | Trading pair, e.g. `BTCUSDT` |
| `price` | FLOAT | Trade price in USDT |
| `quantity` | FLOAT | Trade volume |
| `timestamp` | DATETIME | Trade execution time (indexed) |
| `trade_id` | VARCHAR(50) | Binance trade ID |
| `buyer_is_maker` | BOOLEAN | Order side |
| `moving_avg_5` | FLOAT | 5-period simple moving average |
| `moving_avg_20` | FLOAT | 20-period simple moving average |
| `price_change_1m` | FLOAT | % change over last ~60 trades |

---

## ⚙️ Configuration

All configuration is currently inline. Key values to change:

| Setting | File | Variable |
|---------|------|---------|
| Kafka address | `crypto_producer.py`, `stream_processor.py` | `KAFKA_SERVER` |
| DB connection | `stream_processor.py`, `dash/dash.py` | `DB_URL` |
| Tracked symbols | `crypto_producer.py` | `CRYPTO_SYMBOLS` |
| Cleanup interval | `dags/cleanup_dag.py` | `schedule_interval` |
| Retention period | `dags/cleanup_dag.py` | `INTERVAL '7 days'` |

> 💡 **Recommended**: Move all credentials to a `.env` file and load with `python-dotenv` or Docker Compose's `env_file` directive.

---

## 🛠️ Troubleshooting

### Docker containers not starting
```bash
docker-compose logs kafka
docker-compose logs postgres
docker-compose logs airflow-scheduler
```

### Producer can't connect to Kafka
```bash
# Check Kafka is running
docker-compose ps kafka
# Restart if needed
docker-compose restart kafka
```

### Dashboard shows "No data available"
- Make sure both the **producer** and **processor** are running
- Check PostgreSQL has data: `docker exec -it postgres psql -U admin -d streamflow_db -c "SELECT COUNT(*) FROM crypto_data;"`

### Airflow DAG not appearing
- DAGs can take up to 30 seconds to parse on first load
- Check for syntax errors: `docker exec airflow-scheduler airflow dags list`

### Reset everything (clean slate)
```bash
docker-compose down -v   # removes all containers AND volumes
docker-compose up -d
```

---

## 🔮 Potential Enhancements

- [ ] Environment-based config (`.env` / `python-dotenv`)
- [ ] Multi-symbol comparison view in dashboard
- [ ] Price spike alerting (email / Slack webhook)
- [ ] RSI, Bollinger Bands, MACD indicators
- [ ] Unit and integration tests
- [ ] Dockerise the producer and processor themselves
- [ ] Airflow email alerts on DAG failure

---

## 📦 Dependencies

| Package | Purpose |
|---------|---------|
| `kafka-python` | Kafka producer & consumer |
| `websocket-client` | Binance WebSocket connection |
| `sqlalchemy` | ORM & database abstraction |
| `psycopg2-binary` | PostgreSQL driver |
| `pandas` | DataFrame operations in dashboard |
| `streamlit` | Dashboard framework |
| `plotly` | Interactive charts |
| `streamlit-autorefresh` | Auto-refresh component |
| `requests` | HTTP utilities |
| `apache-airflow` | Workflow orchestration |
| `apache-airflow-providers-postgres` | Airflow PostgreSQL hooks |

---

## 📄 License

MIT — free to use, modify, and distribute.
