package parquetgcsexporter

import (
	"bytes"
	"context"
	"fmt"
	"sort"
	"strings"
	"time"

	"cloud.google.com/go/storage"
	"github.com/google/uuid"
	"github.com/parquet-go/parquet-go"
	"github.com/parquet-go/parquet-go/compress/snappy"
	"go.opentelemetry.io/collector/component"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.uber.org/zap"
	"google.golang.org/api/option"
)

type parquetGCSExporter struct {
	config *Config
	logger *zap.Logger
}

func newExporter(cfg *Config, logger *zap.Logger) *parquetGCSExporter {
	return &parquetGCSExporter{
		config: cfg,
		logger: logger,
	}
}

func (e *parquetGCSExporter) start(_ context.Context, _ component.Host) error {
	return nil
}

func (e *parquetGCSExporter) shutdown(_ context.Context) error {
	return nil
}

// newGCSClient creates a fresh GCS client on every call so that WIF credentials
// (which rotate every ~5 minutes) are always re-read from disk rather than
// cached in a long-lived client.
func (e *parquetGCSExporter) newGCSClient(ctx context.Context) (*storage.Client, error) {
	var opts []option.ClientOption
	if e.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(e.config.CredentialsFile))
	}
	client, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}
	return client, nil
}

func (e *parquetGCSExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	records := extractBodyMaps(ld)
	if len(records) == 0 {
		return nil
	}

	allKeys := unionKeys(records)

	buf, err := writeParquet(records, allKeys)
	if err != nil {
		return fmt.Errorf("failed to encode parquet: %w", err)
	}

	objectPath := e.objectPath()
	if err := e.upload(ctx, objectPath, buf); err != nil {
		return fmt.Errorf("failed to upload gs://%s/%s: %w", e.config.Bucket, objectPath, err)
	}

	e.logger.Info("uploaded parquet file",
		zap.String("bucket", e.config.Bucket),
		zap.String("object", objectPath),
		zap.Int("records", len(records)),
		zap.Int("columns", len(allKeys)),
		zap.Int("bytes", len(buf)),
	)
	return nil
}

// extractBodyMaps walks all log records and returns each body map as map[string]string.
// Records whose body is not a map are skipped.
func extractBodyMaps(ld plog.Logs) []map[string]string {
	var records []map[string]string
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if lr.Body().Type() != pcommon.ValueTypeMap {
					continue
				}
				row := make(map[string]string)
				lr.Body().Map().Range(func(key string, val pcommon.Value) bool {
					row[key] = val.AsString()
					return true
				})
				records = append(records, row)
			}
		}
	}
	return records
}

// unionKeys collects all unique keys across all records and returns them sorted.
// Sorting ensures deterministic column order across batches with the same schema.
func unionKeys(records []map[string]string) []string {
	seen := make(map[string]struct{})
	for _, r := range records {
		for k := range r {
			seen[k] = struct{}{}
		}
	}
	keys := make([]string, 0, len(seen))
	for k := range seen {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys
}

// writeParquet encodes records as a Parquet file with one optional string column per key.
// Missing fields in a record are written as null.
func writeParquet(records []map[string]string, keys []string) ([]byte, error) {
	// Build dynamic schema: every column is an optional (nullable) string.
	group := parquet.Group{}
	for _, k := range keys {
		group[k] = parquet.Optional(parquet.String())
	}
	schema := parquet.NewSchema("logs", group)

	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, schema,
		parquet.Compression(&snappy.Codec{}),
		parquet.MaxRowsPerRowGroup(10_000),
	)

	for _, record := range records {
		row := make(parquet.Row, len(keys))
		for i, k := range keys {
			if v, ok := record[k]; ok {
				// definition level 1 = value present; repetition level 0 = non-repeated
				row[i] = parquet.ValueOf(v).Level(0, 1, i)
			} else {
				// definition level 0 = null
				row[i] = parquet.NullValue().Level(0, 0, i)
			}
		}
		if _, err := writer.WriteRows([]parquet.Row{row}); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

// objectPath builds the GCS object path:
// {prefix}/{partition}/{uuid}.parquet
func (e *parquetGCSExporter) objectPath() string {
	now := time.Now().UTC()
	partition := now.Format(e.config.PartitionFormat)
	id := uuid.New().String()
	parts := []string{}
	if e.config.Prefix != "" {
		parts = append(parts, strings.TrimRight(e.config.Prefix, "/"))
	}
	parts = append(parts, partition, id+".parquet")
	return strings.Join(parts, "/")
}

func (e *parquetGCSExporter) upload(ctx context.Context, path string, data []byte) error {
	client, err := e.newGCSClient(ctx)
	if err != nil {
		return err
	}
	defer client.Close()

	w := client.Bucket(e.config.Bucket).Object(path).NewWriter(ctx)
	w.ContentType = "application/octet-stream"
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
