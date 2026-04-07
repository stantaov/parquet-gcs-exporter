// Package parquetgcsexporter implements an OpenTelemetry Collector exporter
// that writes log records as Apache Parquet files to Google Cloud Storage.
//
// Log body maps are converted into columnar Parquet files with dynamic,
// type-aware schemas. Files are uploaded to GCS using Hive-style
// time-partitioned paths for efficient querying with BigQuery external tables.
package parquetgcsexporter
