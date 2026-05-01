# CLAUDE.md

This file provides guidance to Claude Code (claude.ai/code) when working with code in this repository.

## What This Is

An OpenTelemetry Collector exporter plugin (`parquetgcsstorage`) that projects log records onto a fixed BigQuery-aligned schema, encodes them as Apache Parquet files, and uploads them to Google Cloud Storage. Designed for security log pipelines feeding BigQuery external tables via Hive-style time partitioning.

## Output Schema (fixed, BQ-aligned)

The exporter writes 13 columns matching the destination BQ table 1:1. Every column comes from a top-level key in the OTel log record's body map (BindPlane is expected to have parsed the syslog frame upstream).

| Column | Body key | Parquet type | BQ type | Mode |
|---|---|---|---|---|
| rcvd_ts | `rcvd_ts` (int64 nanos) | INT64 / TIMESTAMP(MICROS, UTC) | TIMESTAMP | REQUIRED |
| event_ts | `event_ts` (int64 nanos) | INT64 / TIMESTAMP(MICROS, UTC) | TIMESTAMP | REQUIRED |
| collector | `collector` | STRING | STRING | NULLABLE |
| namespace | `namespace` | STRING | STRING | NULLABLE |
| log_type | `log_type` | STRING | STRING | NULLABLE |
| source_ip | `source_ip` | STRING | STRING | NULLABLE |
| hostname | `hostname` | STRING | STRING | NULLABLE |
| appname | `appname` | STRING | STRING | NULLABLE |
| facility | `facility` | INT64 | INTEGER | NULLABLE |
| severity | `severity` | INT64 | INTEGER | NULLABLE |
| event_message | `event_message` | STRING | STRING | NULLABLE |
| event_details_json | `event_details_json` | BYTE_ARRAY / JSON | JSON | NULLABLE |
| parse_status | `parse_status` | STRING | STRING | NULLABLE |

Records are skipped (counted in `parquetgcsstorage.records.skipped`) when:
- the LogRecord body is not a map, or
- `rcvd_ts` or `event_ts` is missing or not an integer.

Body fields outside this schema (e.g. extra parser metadata) are silently dropped. `event_details_json` accepts either a pre-serialized JSON string (BindPlane's normal output) or a pcommon.Map/Slice (which is then `json.Marshal`'d).

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
1. `extractRecords` — walks `ResourceLogs > ScopeLogs > LogRecords` and projects each map-bodied record onto the fixed schema via `projectRecord`. Skips records with non-map bodies or missing required timestamps.
2. `writeParquet` — encodes the projected records using the package-level `fixedSchema` with Snappy compression into an in-memory buffer
3. `objectPath` — constructs `{prefix}/{partition}/{uuid}.parquet` using Go `time.Format` for Hive-style paths
4. `upload` — writes buffer to GCS via cached client

**GCS client lifecycle:** `getClient()` caches the `storage.Client` behind a mutex, refreshing every 4 minutes to stay ahead of WIF token rotation (~5 min). Stale clients are closed in a background goroutine after a 30-second grace period for in-flight uploads. `start()` sets `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1` once (not per-upload) to avoid race conditions from concurrent queue consumers.

**Key types:**
- `record` — single log record projected onto the fixed schema; pointer fields distinguish absent (nil) from zero-valued
- `fixedSchema` — package-level `parquet.Schema` defining the 13 columns; column ordinals are alphabetical (matching `parquet.Group.Fields()`) and exposed as `idx*` constants
- `Config` — maps to OTel collector YAML via `mapstructure` tags

**Component metadata** lives in `internal/metadata/generated_status.go`: type is `parquetgcsstorage`, stability is Alpha, logs only.

**BindPlane integration** in `bindplane/destination-type.yaml` provides UI-driven configuration with templated OTel collector config rendering.

## Key Dependencies

- `go.opentelemetry.io/collector/*` v1.52.0 — component framework, pdata, exporterhelper (retry/queue)
- `cloud.google.com/go/storage` — GCS uploads
- `github.com/parquet-go/parquet-go` — Parquet encoding (Snappy, fixed schema, TIMESTAMP_MICROS and JSON logical types)
- `go.uber.org/zap` — structured logging
