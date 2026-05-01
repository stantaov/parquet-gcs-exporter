# Parquet GCS Exporter

An OpenTelemetry Collector exporter that writes log data as Apache Parquet files to Google Cloud Storage (GCS).

The exporter uses a **fixed BigQuery-aligned schema**: each log record's body map (produced by upstream BindPlane processors) is projected onto 13 columns (`rcvd_ts`, `event_ts`, `collector`, `namespace`, `log_type`, `source_ip`, `hostname`, `appname`, `facility`, `severity`, `event_message`, `event_details_json`, `parse_status`). Files are time-partitioned using Hive-style paths (e.g., `year=2006/month=01/day=02/hour=15`) for efficient querying with BigQuery external tables.

## Features

- **Fixed BQ-aligned schema** — Records are projected onto a 13-column schema that matches the destination BigQuery table 1:1; extra body fields are dropped, records missing the required `rcvd_ts`/`event_ts` are skipped
- **Native BQ types** — Timestamps encoded as INT64 `TIMESTAMP(MICROS, UTC)`; `event_details_json` encoded with the Parquet `JSON` logical type, both loadable into BigQuery TIMESTAMP/JSON columns without conversion
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
            ├─ Project body maps onto fixed 13-column schema
            ├─ Skip records with non-map bodies or missing rcvd_ts/event_ts
            ├─ Encode as Parquet (Snappy, TIMESTAMP_MICROS, JSON logical type)
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
- **extractRecords** — full-record happy path, non-map body skipping, missing-required-timestamp skipping, optional-field absence, extra-field dropping, JSON-as-map fallback
- **writeParquet** — round-trip encode/decode and schema column verification, empty input
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

## BigQuery Schema

The destination BQ table that matches the Parquet output:

```sql
CREATE TABLE `project.dataset.logs` (
  rcvd_ts            TIMESTAMP NOT NULL OPTIONS(description="Timestamp of the relay agent that received the event."),
  event_ts           TIMESTAMP NOT NULL OPTIONS(description="Identified SYSLOG TIMESTAMP if known, otherwise rcvd_ts."),
  collector          STRING             OPTIONS(description="Name of the asset that received the logs (relay host or BindPlane agent)."),
  namespace          STRING             OPTIONS(description="Business group / data owner. Aligned to SecOps namespace."),
  log_type           STRING             OPTIONS(description="Derived message type aligned to SecOps Log Types where possible."),
  source_ip          STRING             OPTIONS(description="IP address of the sending device as interpreted by the relay agent."),
  hostname           STRING             OPTIONS(description="Identified SYSLOG HOSTNAME value."),
  appname            STRING             OPTIONS(description="Identified SYSLOG APP-NAME value."),
  facility           INTEGER            OPTIONS(description="Identified SYSLOG PRI facility value."),
  severity           INTEGER            OPTIONS(description="Identified SYSLOG SEVERITY value."),
  event_message      STRING             OPTIONS(description="Raw log event message as received by the collector."),
  event_details_json JSON               OPTIONS(description="Log-type-specific extracted fields as JSON."),
  parse_status       STRING             OPTIONS(description="Parser outcome: 'parsed' or 'unparsed'.")
);
```

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

## CI/CD

### PR Checks (`ci.yml`)

Triggers on **pull requests** to `main`. Runs two parallel jobs:
- **build-and-test** — `go build ./...` and `go test -v -race`
- **lint** — `golangci-lint`

Both must pass before merging.

### Tag-Based Releases (`release.yml`)

Triggers when you push a `v*` tag:

```bash
git tag v1.0.2
git push origin v1.0.2
```

1. Runs build + tests to validate the tagged code
2. If tests pass, creates a **GitHub Release** with:
   - Auto-generated changelog (commit messages since previous tag)
   - Installation snippet with the exact tag version

## Component Metadata

| Property | Value |
|----------|-------|
| Component type | `parquetgcsstorage` |
| Stability | Alpha |
| Supported signals | Logs |
| Module | `github.com/stantaov/parquet-gcs-exporter` |

## License

See [LICENSE](LICENSE) for details.
