# Parquet GCS Exporter

An OpenTelemetry Collector exporter that writes log data as Apache Parquet files to Google Cloud Storage (GCS).

Each field in the log body becomes a Parquet column. Files are time-partitioned using Hive-style paths (e.g., `year=2006/month=01/day=02/hour=15`) for efficient querying with BigQuery external tables.

## Features

- **Dynamic schema** — Parquet columns are inferred from log body fields at write time; no fixed schema required
- **Type-aware columns** — Preserves original value types (Int64, Double, Bool, String); mixed types for the same key fall back to String
- **Time-based partitioning** — Configurable granularity (day, hour, minute) using Hive-style directory layout
- **Snappy compression** — Parquet files use Snappy codec for balanced compression and read performance
- **Retry with backoff** — Exponential back-off on failed GCS uploads
- **Sending queue** — In-memory or persistent (file-backed) queue decouples ingestion from writes
- **WIF support** — Works with Workload Identity Federation, Service Account keys, and Application Default Credentials
- **BindPlane integration** — Includes a BindPlane destination type for UI-driven configuration

## Architecture

```
OTel Collector Pipeline
  └─ Logs
       └─ parquetgcsstorage exporter
            ├─ Extract body maps from log records
            ├─ Build dynamic Parquet schema (union of all keys)
            ├─ Infer column types (Int64, Double, Bool, String)
            ├─ Encode as Parquet (Snappy, nullable typed columns)
            └─ Upload to GCS: gs://{bucket}/{prefix}/{partition}/{uuid}.parquet
```

## Development

### Requirements

- Go 1.25+ (if your local Go is older, set `GOTOOLCHAIN=auto` to let it download the right version)
- GCP credentials with `storage.objects.create` permission on the target bucket (for runtime)

### Build

```bash
go build ./...
```

### Testing

Run all tests:

```bash
go test ./...
```

Run tests with verbose output:

```bash
go test -v ./...
```

Run a specific test:

```bash
go test -v -run TestWriteParquet_RoundTrip ./...
```

The test suite covers:
- **extractBodyMaps** — typed value extraction, non-map body skipping, multi-resource/scope traversal
- **inferSchema** — consistent types, mixed-type fallback to string, sorted key ordering
- **writeParquet** — round-trip encode/decode, nullable columns, all column types, mixed-type fallback
- **objectPath** — prefix handling, empty prefix, trailing slash, hourly partitioning
- **Config.Validate** — required field validation

## Installation

Add the exporter to your custom OTel Collector build using the [OpenTelemetry Collector Builder](https://github.com/open-telemetry/opentelemetry-collector/tree/main/cmd/builder) (`ocb`):

```yaml
# manifest.yaml
exporters:
  - gomod: github.com/stantaov/parquet-gcs-exporter v0.2.5
```

Then build:

```bash
ocb --config manifest.yaml
```

## Configuration

```yaml
exporters:
  parquetgcsstorage:
    # Required: GCS bucket name
    bucket: "my-logs-bucket"

    # Optional: folder prefix (default: "logs")
    prefix: "wsa/raw"

    # Optional: GCP project ID (inferred from credentials if empty)
    project_id: ""

    # Optional: path to SA key or WIF config JSON (uses ADC if empty)
    credentials_file: "/etc/security/wif/config.json"

    # Optional: Go time.Format string for partition path
    # Default: "year=2006/month=01/day=02"
    # Hourly: "year=2006/month=01/day=02/hour=15"
    partition_format: "year=2006/month=01/day=02/hour=15"

    # Retry configuration
    retry_on_failure:
      enabled: true
      initial_interval: 5s
      max_interval: 30s
      max_elapsed_time: 0s  # 0 = retry forever

    # Sending queue
    sending_queue:
      num_consumers: 10
      queue_size: 1000
      # Optional: use a filestorage extension for crash-safe persistent queue
      # storage: file_storage/parquet_queue
```

### Authentication

The exporter supports three authentication methods (in order of precedence):

| Method | Config | Use Case |
|--------|--------|----------|
| Credentials file | `credentials_file: "/path/to/key.json"` | Service Account key or WIF config |
| Application Default Credentials | *(leave credentials_file empty)* | GKE Workload Identity, `gcloud auth` |
| WIF executable source | `credentials_file: "/path/to/wif-config.json"` | On-prem collectors with WIF |

For WIF executable-based credentials, the exporter automatically sets `GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES=1`.

### Partition Formats

| Granularity | Format String | Example Path |
|-------------|---------------|--------------|
| Day | `year=2006/month=01/day=02` | `logs/year=2025/month=03/day=15/abc123.parquet` |
| Hour | `year=2006/month=01/day=02/hour=15` | `logs/year=2025/month=03/day=15/hour=14/abc123.parquet` |
| Minute | `year=2006/month=01/day=02/hour=15/minute=04` | `logs/year=2025/month=03/day=15/hour=14/minute=30/abc123.parquet` |

## GCS Object Layout

```
gs://{bucket}/
  └── {prefix}/
      └── year=2025/
          └── month=03/
              └── day=15/
                  └── hour=14/
                      ├── a1b2c3d4-e5f6-7890-abcd-ef1234567890.parquet
                      └── f9e8d7c6-b5a4-3210-fedc-ba0987654321.parquet
```

This Hive-style layout enables BigQuery partition pruning on external tables, significantly reducing query costs and latency.

## BigQuery External Table

Create an external table over the Parquet files for SQL querying:

```sql
CREATE EXTERNAL TABLE `project.dataset.logs`
WITH PARTITION COLUMNS (
  year INT64,
  month INT64,
  day INT64,
  hour INT64
)
OPTIONS (
  format = 'PARQUET',
  uris = ['gs://my-logs-bucket/logs/*.parquet'],
  hive_partition_uri_prefix = 'gs://my-logs-bucket/logs/'
);
```

## BindPlane

A BindPlane destination type is included in `bindplane/destination-type.yaml`. Import it to enable UI-driven configuration of the exporter within BindPlane.

## Component Metadata

| Property | Value |
|----------|-------|
| Component type | `parquetgcsstorage` |
| Stability | Alpha |
| Supported signals | Logs |
| Module | `github.com/stantaov/parquet-gcs-exporter` |

## License

See [LICENSE](LICENSE) for details.
