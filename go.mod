module github.com/stantaov/parquet-gcs-exporter

go 1.25.0

require (
	cloud.google.com/go/storage v1.50.0
	github.com/google/uuid v1.6.0
	github.com/parquet-go/parquet-go v0.24.0
	go.opentelemetry.io/collector/component v1.52.0
	go.opentelemetry.io/collector/consumer v1.52.0
	go.opentelemetry.io/collector/exporter v1.52.0
	go.opentelemetry.io/collector/exporter/exporterhelper v0.146.1
	go.opentelemetry.io/collector/pdata v1.52.0
	go.uber.org/zap v1.27.1
	google.golang.org/api v0.264.0
)
