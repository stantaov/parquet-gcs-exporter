package parquetgcsexporter

import (
	"context"

	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/config/configoptional"
	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/consumer"
	"go.opentelemetry.io/collector/exporter"
	"go.opentelemetry.io/collector/exporter/exporterhelper"

	"github.com/stantaov/parquet-gcs-exporter/internal/metadata"
)

func NewFactory() exporter.Factory {
	return exporter.NewFactory(
		metadata.Type,
		createDefaultConfig,
		exporter.WithLogs(createLogsExporter, metadata.LogsStability),
	)
}

func createDefaultConfig() component.Config {
	return &Config{
		Prefix:          "logs",
		PartitionFormat: "year=2006/month=01/day=02",
		RetryConfig:     configretry.NewDefaultBackOffConfig(),
		QueueConfig:     exporterhelper.NewDefaultQueueConfig(),
	}
}

func createLogsExporter(ctx context.Context, set exporter.Settings, cfg component.Config) (exporter.Logs, error) {
	c := cfg.(*Config)
	exp := newExporter(c, set.Logger)
	return exporterhelper.NewLogs(
		ctx, set, cfg,
		exp.consumeLogs,
		exporterhelper.WithCapabilities(consumer.Capabilities{MutatesData: false}),
		exporterhelper.WithStart(exp.start),
		exporterhelper.WithShutdown(exp.shutdown),
		exporterhelper.WithRetry(c.RetryConfig),
		exporterhelper.WithQueue(configoptional.Some(c.QueueConfig)),
	)
}
