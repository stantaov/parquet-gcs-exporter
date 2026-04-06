package metadata

import "go.opentelemetry.io/collector/component"

var (
	Type      = component.MustNewType("parquetgcsstorage")
	ScopeName = "github.com/stantaov/parquet-gcs-exporter"
)

const (
	LogsStability = component.StabilityLevelAlpha
)
