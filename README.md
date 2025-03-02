# Sift - High-Throughput Event Log Processor

Sift is a high-performance event log processor designed to ingest, store, and query massive volumes of structured log events in real-time. Built with Go, Flatbuffers, and Redpanda, it optimizes log storage and retrieval with minimal overhead, providing a scalable and efficient solution for analytics, monitoring, and observability workloads.

## Design Goals

- **Efficient Log Ingestion**: Minimize serialization overhead using Flatbuffers and leverage Redpanda as a fast, Kafka-compatible event broker.
- **High-Throughput Processing**: Use Go's concurrency model to parallelize event processing, allowing for multi-core efficiency.
- **Scalable Storage**: Store logs in DuckDB (for queryable in-memory analytics) and Parquet (for efficient long-term storage).
- **Fast Query API**: Provide an optimized Fiber-based API for querying log data, exposing both raw and aggregated insights.
- **Observability**: Integrate Zap logging for high-performance structured logging and OpenTelemetry for tracing.

## Core Components & Architecture

### 1. Log Ingestion Pipeline

- Event producers send structured logs as Flatbuffer-encoded messages to Redpanda
- Sift consumers process logs in parallel using Go's concurrency features
- Metrics collection via Prometheus

### 2. Log Storage

- Short-term storage: DuckDB for fast, in-memory columnar storage
- Long-term storage: Parquet files on object storage (S3, GCS, MinIO)
- Storage manager handles data retention policies and ETL

### 3. Query API

- HTTP/gRPC endpoints for log querying
- SQL-like syntax for complex queries
- Authorization via PropelAuth or JWT

### 4. Observability & Logging

- Structured logging with Zap
- Monitoring with Prometheus and Grafana
- Distributed tracing with OpenTelemetry

## Getting Started

### Prerequisites

- Go 1.24+
- Docker (for local development)
- Redpanda
- DuckDB

### Installation

```bash
git clone https://github.com/TFMV/sift.git
cd sift
go mod tidy
```

### Running Locally

```bash
docker-compose up -d  # Start dependencies
go run cmd/sift/main.go
```

## License

MIT. See [LICENSE](LICENSE) file for details.
