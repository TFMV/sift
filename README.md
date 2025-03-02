# 🚀 Sift - High-Throughput Event Log Processor

**Blazing fast log ingestion. Zero-copy efficiency. Real-time analytics.**  
Sift is the log processing engine built for **speed**, **scalability**, and **simplicity**.  

🔥 **Flatbuffers for ultra-lightweight serialization**  
⚡ **Redpanda for high-throughput event streaming**  
📊 **DuckDB & Parquet for analytics-ready storage**  
🖥️ **Fiber-powered API for low-latency queries**  

## 🎯 Why Sift?

Modern log pipelines are **slow** and **bloated**—parsing JSON, serializing protobufs, struggling with backpressure.  
Sift **eliminates** these bottlenecks with an **event-driven, memory-efficient architecture** designed for real-time workloads.

### ✅ Design Goals

- **🚀 Max Performance** – Process millions of logs per second using Go's concurrency model.
- **📡 Zero Overhead** – Flatbuffers enable instant deserialization (no GC pauses, no allocations).
- **🔍 Query at the Speed of Light** – In-memory DuckDB for hot storage, Parquet for long-term retention.
- **🔗 Simple & Scalable** – Redpanda-based ingestion, easy horizontal scaling.

## 🛠️ Core Architecture

### 1️⃣ Log Ingestion Pipeline

🔹 **Producers → Redpanda**  

- Microservices, applications, and external sources send structured logs.  
- Logs are Flatbuffer-encoded, ensuring ultra-low CPU & memory overhead.  

🔹 **Sift Consumer → Redpanda**  

- Parallel message processing with goroutines & worker pools.  
- Metrics collection via Prometheus.  

### 2️⃣ High-Performance Storage

- **🟢 Hot Storage**: DuckDB for fast, in-memory queries (supports aggregations, filtering, indexing).  
- **🔵 Cold Storage**: Parquet on S3/MinIO for long-term, cost-effective retention.  
- **🛠 Storage Manager**: Automatically flushes old logs from DuckDB → Parquet.  

### 3️⃣ Query API (Fiber-powered)

Sift exposes a blazing-fast query API for log retrieval and analytics.

🔹 **Query Examples**

```bash
curl "http://localhost:8080/logs?service=auth&range=last_10m"
```

```json
{
  "service": "auth",
  "errors": 27,
  "avg_latency_ms": 5.3
}
```

🔹 **Endpoints**

- GET /logs?filter=service:web&range=last_hour
- POST /logs/query → Supports SQL-like syntax
- GET /stats?metric=error_rate

🔹 **Security & Authentication**

- 🔐 Multi-tenant auth with PropelAuth or JWT.

### 4️⃣ Observability & Logging

- 📄 Structured Logging with Zap.
- 📊 Real-time Monitoring via Prometheus + Grafana.
- 📡 OpenTelemetry for distributed tracing.
- 📍 Exposes /metrics for easy observability.

## 🏁 Getting Started

### Prerequisites

- Go 1.24+
- Docker (for local Redpanda & storage)
- DuckDB (optional for in-memory queries)

### Installation

```bash
git clone https://github.com/TFMV/sift.git
cd sift
go mod tidy
```

### Running Locally

```bash
docker-compose up -d  # Start Redpanda, MinIO, and DuckDB
go run cmd/sift/main.go
```

## 📜 License

MIT. See [LICENSE](LICENSE) file for details.
