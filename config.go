package parquetgcsexporter

import (
	"errors"

	"go.opentelemetry.io/collector/config/configretry"
	"go.opentelemetry.io/collector/exporter/exporterhelper"
)

// Config defines the configuration for the Parquet GCS exporter.
type Config struct {
	// ProjectID is the GCP project ID. If empty, inferred from the environment.
	ProjectID string `mapstructure:"project_id"`

	// Bucket is the GCS bucket name (required).
	Bucket string `mapstructure:"bucket"`

	// Prefix is prepended to every object path (e.g. "logs").
	Prefix string `mapstructure:"prefix"`

	// PartitionFormat is a Go time.Format string used to build the partition path
	// appended after Prefix. Defaults to "year=2006/month=01/day=02".
	// Use standard Go reference time components (2006, 01, 02, 15, etc.).
	PartitionFormat string `mapstructure:"partition_format"`

	// CredentialsFile is the path to a Google credential file on the collector host.
	// Accepts both a Service Account JSON key and a Workload Identity Federation
	// (WIF) external-credentials JSON config. If empty, Application Default
	// Credentials (ADC) are used — suitable for GKE Workload Identity.
	CredentialsFile string `mapstructure:"credentials_file"`

	// RetryConfig configures exponential back-off retry on failed exports.
	RetryConfig configretry.BackOffConfig `mapstructure:"retry_on_failure"`

	// QueueConfig configures the in-memory sending queue.
	QueueConfig exporterhelper.QueueBatchConfig `mapstructure:"sending_queue"`
}

func (c *Config) Validate() error {
	if c.Bucket == "" {
		return errors.New("bucket is required")
	}
	return nil
}
