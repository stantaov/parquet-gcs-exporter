# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An OpenTelemetry Collector exporter plugin (`parquetgcsstorage`) that converts log records with map-type bodies into Apache Parquet files and uploads them to Google Cloud Storage. Designed for security log pipelines feeding BigQuery external tables via Hive-style time partitioning.

## Build & Test

```bash
# Requires Go 1.25+ (use GOTOOLCHAIN=auto if local Go is older)
GOTOOLCHAIN=auto go build ./...
GOTOOLCHAIN=auto go test ./...
GOTOOLCHAIN=auto go test -v -run TestWriteParquet_RoundTrip ./...  # single test
```

This is a library (OTel plugin), not a standalone binary. To produce a collector binary, use [OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder) (`ocb`) with this module in `manifest.yaml`.

## Architecture

The exporter follows the standard OTel Collector component pattern: **factory** creates the exporter, **config** defines parameters, **exporter** implements the log consumption pipeline.

**Data flow through `consumeLogs`:**
1. `extractBodyMaps` — walks `ResourceLogs > ScopeLogs > LogRecords`, extracts map-type bodies preserving original OTel value types (`typedValue` struct)
2. `inferSchema` — union of all keys across records, determines Parquet column type per key (Int64/Double/Bool/String); mixed types for the same key fall back to String
3. `writeParquet` — builds a dynamic Parquet schema with nullable columns, encodes with Snappy compression into an in-memory buffer
4. `objectPath` — constructs `{prefix}/{partition}/{uuid}.parquet` using Go `time.Format` for Hive-style paths
5. `upload` — writes buffer to GCS via cached client

**GCS client lifecycle:** `getClient()` caches the `storage.Client` behind a mutex, refreshing every 4 minutes to stay ahead of WIF token rotation (~5 min). Stale clients are closed in a background goroutine after a 30-second grace period for in-flight uploads. `start()` sets `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1` once (not per-upload) to avoid race conditions from concurrent queue consumers.

**Key types:**
- `typedValue` — preserves pcommon.Value type alongside extracted values
- `columnType` — enum (colString/colInt64/colDouble/colBool) for Parquet schema decisions
- `Config` — maps to OTel collector YAML via `mapstructure` tags

**Component metadata** lives in `internal/metadata/generated_status.go`: type is `parquetgcsstorage`, stability is Alpha, logs only.

**BindPlane integration** in `bindplane/destination-type.yaml` provides UI-driven configuration with templated OTel collector config rendering.

## Key Dependencies

- `go.opentelemetry.io/collector/*` v1.52.0 — component framework, pdata, exporterhelper (retry/queue)
- `cloud.google.com/go/storage` — GCS uploads
- `github.com/parquet-go/parquet-go` — Parquet encoding (Snappy, dynamic schema, nullable columns)
- `go.uber.org/zap` — structured logging
