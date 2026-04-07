package parquetgcsexporter

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"sort"
	"strings"
	"sync"
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

// clientMaxAge controls how long a cached GCS client is reused before being
// refreshed. WIF tokens rotate every ~5 min; 4 min gives comfortable headroom.
const clientMaxAge = 4 * time.Minute

// columnType represents the Parquet column type inferred from log values.
type columnType int

const (
	colString columnType = iota
	colInt64
	colDouble
	colBool
)

// typedValue preserves the original pcommon.Value type for type-aware Parquet encoding.
type typedValue struct {
	vType pcommon.ValueType
	str   string
	i64   int64
	f64   float64
	b     bool
}

type parquetGCSExporter struct {
	config *Config
	logger *zap.Logger

	mu              sync.Mutex
	client          *storage.Client
	clientCreatedAt time.Time
}

func newExporter(cfg *Config, logger *zap.Logger) *parquetGCSExporter {
	return &parquetGCSExporter{
		config: cfg,
		logger: logger,
	}
}

func (e *parquetGCSExporter) start(_ context.Context, _ component.Host) error {
	// WIF executable-based credential sources require this env var.
	// Set it once at startup rather than on every upload to avoid
	// concurrent os.Setenv calls from queue consumers.
	os.Setenv("GOOGLE_EXTERNAL_ACCOUNT_ALLOW_EXECUTABLES", "1")
	return nil
}

func (e *parquetGCSExporter) shutdown(_ context.Context) error {
	e.mu.Lock()
	defer e.mu.Unlock()
	if e.client != nil {
		err := e.client.Close()
		e.client = nil
		return err
	}
	return nil
}

// getClient returns a cached GCS client, creating or refreshing it when
// the current one is nil or older than clientMaxAge.
func (e *parquetGCSExporter) getClient(ctx context.Context) (*storage.Client, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	if e.client != nil && time.Since(e.clientCreatedAt) < clientMaxAge {
		return e.client, nil
	}

	var opts []option.ClientOption
	if e.config.CredentialsFile != "" {
		opts = append(opts, option.WithCredentialsFile(e.config.CredentialsFile))
	}
	newClient, err := storage.NewClient(ctx, opts...)
	if err != nil {
		return nil, fmt.Errorf("failed to create GCS client: %w", err)
	}

	// Close the stale client in the background after a grace period so
	// in-flight uploads that still reference it can finish.
	if e.client != nil {
		old := e.client
		go func() {
			time.Sleep(30 * time.Second)
			old.Close()
		}()
	}

	e.client = newClient
	e.clientCreatedAt = time.Now()
	return newClient, nil
}

func (e *parquetGCSExporter) consumeLogs(ctx context.Context, ld plog.Logs) error {
	records := extractBodyMaps(ld)
	if len(records) == 0 {
		return nil
	}

	keys, colTypes := inferSchema(records)

	buf, err := writeParquet(records, keys, colTypes)
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
		zap.Int("columns", len(keys)),
		zap.Int("bytes", len(buf)),
	)
	return nil
}

// extractBodyMaps walks all log records and returns each body map with
// preserved value types. Records whose body is not a map are skipped.
func extractBodyMaps(ld plog.Logs) []map[string]typedValue {
	var records []map[string]typedValue
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if lr.Body().Type() != pcommon.ValueTypeMap {
					continue
				}
				row := make(map[string]typedValue)
				lr.Body().Map().Range(func(key string, val pcommon.Value) bool {
					tv := typedValue{vType: val.Type(), str: val.AsString()}
					switch val.Type() {
					case pcommon.ValueTypeInt:
						tv.i64 = val.Int()
					case pcommon.ValueTypeDouble:
						tv.f64 = val.Double()
					case pcommon.ValueTypeBool:
						tv.b = val.Bool()
					}
					row[key] = tv
					return true
				})
				records = append(records, row)
			}
		}
	}
	return records
}

// inferSchema collects all unique keys across records and determines each
// column's Parquet type. If all values for a key share a type, that type is
// used; mixed types fall back to string. Keys are returned sorted for
// deterministic column ordering.
func inferSchema(records []map[string]typedValue) ([]string, map[string]columnType) {
	colTypes := make(map[string]columnType)
	initialized := make(map[string]bool)

	for _, r := range records {
		for k, v := range r {
			ct := valueTypeToColumnType(v.vType)
			if !initialized[k] {
				colTypes[k] = ct
				initialized[k] = true
			} else if colTypes[k] != ct {
				colTypes[k] = colString
			}
		}
	}

	keys := make([]string, 0, len(colTypes))
	for k := range colTypes {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	return keys, colTypes
}

func valueTypeToColumnType(vt pcommon.ValueType) columnType {
	switch vt {
	case pcommon.ValueTypeInt:
		return colInt64
	case pcommon.ValueTypeDouble:
		return colDouble
	case pcommon.ValueTypeBool:
		return colBool
	default:
		return colString
	}
}

// writeParquet encodes records as a Parquet file using the inferred column types.
// Missing fields in a record are written as null.
func writeParquet(records []map[string]typedValue, keys []string, colTypes map[string]columnType) ([]byte, error) {
	group := parquet.Group{}
	for _, k := range keys {
		group[k] = parquet.Optional(parquetNode(colTypes[k]))
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
				row[i] = typedParquetValue(v, colTypes[k]).Level(0, 1, i)
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

func parquetNode(ct columnType) parquet.Node {
	switch ct {
	case colInt64:
		return parquet.Int(64)
	case colDouble:
		return parquet.Leaf(parquet.DoubleType)
	case colBool:
		return parquet.Leaf(parquet.BooleanType)
	default:
		return parquet.String()
	}
}

func typedParquetValue(v typedValue, ct columnType) parquet.Value {
	switch ct {
	case colInt64:
		return parquet.Int64Value(v.i64)
	case colDouble:
		return parquet.DoubleValue(v.f64)
	case colBool:
		return parquet.BooleanValue(v.b)
	default:
		return parquet.ValueOf(v.str)
	}
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
	client, err := e.getClient(ctx)
	if err != nil {
		return err
	}

	w := client.Bucket(e.config.Bucket).Object(path).NewWriter(ctx)
	w.ContentType = "application/vnd.apache.parquet"
	if _, err := w.Write(data); err != nil {
		_ = w.Close()
		return err
	}
	return w.Close()
}
