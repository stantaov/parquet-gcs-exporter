package parquetgcsexporter

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
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
	"go.opentelemetry.io/otel/metric"
	"go.uber.org/zap"
	"google.golang.org/api/option"

	"github.com/stantaov/parquet-gcs-exporter/internal/metadata"
)

// clientMaxAge controls how long a cached GCS client is reused before being
// refreshed. WIF tokens rotate every ~5 min; 4 min gives comfortable headroom.
const clientMaxAge = 4 * time.Minute

// Column ordinals for the fixed Parquet schema. parquet.Group orders fields
// alphabetically, so these indices match the alphabetically sorted names below.
const (
	idxAppname = iota
	idxCollector
	idxEventDetailsJSON
	idxEventMessage
	idxEventTs
	idxFacility
	idxHostname
	idxLogType
	idxNamespace
	idxParseStatus
	idxRcvdTs
	idxSeverity
	idxSourceIP
	numColumns
)

// fixedSchema mirrors the destination BigQuery table 1:1.
// rcvd_ts and event_ts are REQUIRED; all other columns are NULLABLE.
// Timestamps are stored as INT64 microseconds (TIMESTAMP_MICROS, UTC),
// which BigQuery loads natively into TIMESTAMP. event_details_json uses
// the JSON logical type, which BigQuery loads natively into JSON.
var fixedSchema = parquet.NewSchema("logs", parquet.Group{
	"rcvd_ts":            parquet.Timestamp(parquet.Microsecond),
	"event_ts":           parquet.Timestamp(parquet.Microsecond),
	"collector":          parquet.Optional(parquet.String()),
	"namespace":          parquet.Optional(parquet.String()),
	"log_type":           parquet.Optional(parquet.String()),
	"source_ip":          parquet.Optional(parquet.String()),
	"hostname":           parquet.Optional(parquet.String()),
	"appname":            parquet.Optional(parquet.String()),
	"facility":           parquet.Optional(parquet.Int(64)),
	"severity":           parquet.Optional(parquet.Int(64)),
	"event_message":      parquet.Optional(parquet.String()),
	"event_details_json": parquet.Optional(parquet.JSON()),
	"parse_status":       parquet.Optional(parquet.String()),
})

// record is a single log record projected onto the fixed BQ schema.
// Pointer fields distinguish absent (nil) from zero-valued.
type record struct {
	rcvdTsMicros     int64
	eventTsMicros    int64
	collector        *string
	namespace        *string
	logType          *string
	sourceIP         *string
	hostname         *string
	appname          *string
	facility         *int64
	severity         *int64
	eventMessage     *string
	eventDetailsJSON []byte
	parseStatus      *string
}

type parquetGCSExporter struct {
	config *Config
	logger *zap.Logger

	mu              sync.Mutex
	client          *storage.Client
	clientCreatedAt time.Time

	// metrics
	recordsExported metric.Int64Counter
	recordsSkipped  metric.Int64Counter
	uploadBytes     metric.Int64Counter
	uploadDuration  metric.Int64Histogram
}

func newExporter(cfg *Config, logger *zap.Logger, mp metric.MeterProvider) (*parquetGCSExporter, error) {
	meter := mp.Meter(metadata.ScopeName)

	recordsExported, err := meter.Int64Counter("parquetgcsstorage.records.exported",
		metric.WithDescription("Number of log records written to Parquet files"),
		metric.WithUnit("{records}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create records.exported metric: %w", err)
	}

	recordsSkipped, err := meter.Int64Counter("parquetgcsstorage.records.skipped",
		metric.WithDescription("Number of log records skipped due to non-map body or missing required fields"),
		metric.WithUnit("{records}"),
	)
	if err != nil {
		return nil, fmt.Errorf("create records.skipped metric: %w", err)
	}

	uploadBytes, err := meter.Int64Counter("parquetgcsstorage.upload.bytes",
		metric.WithDescription("Total bytes uploaded to GCS"),
		metric.WithUnit("By"),
	)
	if err != nil {
		return nil, fmt.Errorf("create upload.bytes metric: %w", err)
	}

	uploadDuration, err := meter.Int64Histogram("parquetgcsstorage.upload.duration",
		metric.WithDescription("GCS upload duration"),
		metric.WithUnit("ms"),
	)
	if err != nil {
		return nil, fmt.Errorf("create upload.duration metric: %w", err)
	}

	return &parquetGCSExporter{
		config:          cfg,
		logger:          logger,
		recordsExported: recordsExported,
		recordsSkipped:  recordsSkipped,
		uploadBytes:     uploadBytes,
		uploadDuration:  uploadDuration,
	}, nil
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
	records, skipped := extractRecords(ld)

	if skipped > 0 {
		e.logger.Debug("skipped log records",
			zap.Int("skipped", skipped),
			zap.String("reason", "non-map body or missing required rcvd_ts/event_ts"),
		)
		e.recordsSkipped.Add(ctx, int64(skipped))
	}

	if len(records) == 0 {
		return nil
	}

	buf, err := writeParquet(records)
	if err != nil {
		return fmt.Errorf("failed to encode parquet: %w", err)
	}

	objectPath := e.objectPath()

	uploadStart := time.Now()
	if err := e.upload(ctx, objectPath, buf); err != nil {
		return fmt.Errorf("failed to upload gs://%s/%s: %w", e.config.Bucket, objectPath, err)
	}
	uploadMs := time.Since(uploadStart).Milliseconds()

	e.recordsExported.Add(ctx, int64(len(records)))
	e.uploadBytes.Add(ctx, int64(len(buf)))
	e.uploadDuration.Record(ctx, uploadMs)

	e.logger.Info("uploaded parquet file",
		zap.String("bucket", e.config.Bucket),
		zap.String("object", objectPath),
		zap.Int("records", len(records)),
		zap.Int("bytes", len(buf)),
		zap.Int64("upload_ms", uploadMs),
	)
	return nil
}

// extractRecords walks all log records and projects each onto the fixed schema.
// Records with non-map bodies or missing required rcvd_ts/event_ts are skipped.
func extractRecords(ld plog.Logs) ([]record, int) {
	var records []record
	var skipped int
	for i := 0; i < ld.ResourceLogs().Len(); i++ {
		rl := ld.ResourceLogs().At(i)
		for j := 0; j < rl.ScopeLogs().Len(); j++ {
			sl := rl.ScopeLogs().At(j)
			for k := 0; k < sl.LogRecords().Len(); k++ {
				lr := sl.LogRecords().At(k)
				if lr.Body().Type() != pcommon.ValueTypeMap {
					skipped++
					continue
				}
				body := lr.Body().Map()
				rec, ok := projectRecord(body)
				if !ok {
					skipped++
					continue
				}
				records = append(records, rec)
			}
		}
	}
	return records, skipped
}

// projectRecord pulls the BQ schema columns out of a body map. Returns ok=false
// if either required timestamp field is missing or unparseable.
func projectRecord(body pcommon.Map) (record, bool) {
	rcvdTs, ok := bodyTimestampMicros(body, "rcvd_ts")
	if !ok {
		return record{}, false
	}
	eventTs, ok := bodyTimestampMicros(body, "event_ts")
	if !ok {
		return record{}, false
	}
	return record{
		rcvdTsMicros:     rcvdTs,
		eventTsMicros:    eventTs,
		collector:        bodyString(body, "collector"),
		namespace:        bodyString(body, "namespace"),
		logType:          bodyString(body, "log_type"),
		sourceIP:         bodyString(body, "source_ip"),
		hostname:         bodyString(body, "hostname"),
		appname:          bodyString(body, "appname"),
		facility:         bodyInt(body, "facility"),
		severity:         bodyInt(body, "severity"),
		eventMessage:     bodyString(body, "event_message"),
		eventDetailsJSON: bodyJSONBytes(body, "event_details_json"),
		parseStatus:      bodyString(body, "parse_status"),
	}, true
}

// bodyString returns the value at key as a string pointer, or nil if absent.
// Non-string types are coerced via pcommon.Value.AsString().
func bodyString(m pcommon.Map, key string) *string {
	v, ok := m.Get(key)
	if !ok {
		return nil
	}
	s := v.AsString()
	return &s
}

// bodyInt returns the value at key as an int64 pointer, or nil if absent or
// not coercible to an integer.
func bodyInt(m pcommon.Map, key string) *int64 {
	v, ok := m.Get(key)
	if !ok {
		return nil
	}
	switch v.Type() {
	case pcommon.ValueTypeInt:
		i := v.Int()
		return &i
	case pcommon.ValueTypeDouble:
		i := int64(v.Double())
		return &i
	default:
		return nil
	}
}

// bodyTimestampMicros reads a Unix-nanosecond integer at key and returns it
// converted to microseconds. ok=false signals the field is absent or not an
// integer — callers treat this as a missing required field.
func bodyTimestampMicros(m pcommon.Map, key string) (int64, bool) {
	v, ok := m.Get(key)
	if !ok {
		return 0, false
	}
	if v.Type() != pcommon.ValueTypeInt {
		return 0, false
	}
	return v.Int() / 1000, true
}

// bodyJSONBytes returns a JSON-encoded byte slice for the value at key.
// Strings pass through as-is (assumed already valid JSON from BindPlane);
// maps and slices are json.Marshal'd. Returns nil if absent or unencodable.
func bodyJSONBytes(m pcommon.Map, key string) []byte {
	v, ok := m.Get(key)
	if !ok {
		return nil
	}
	switch v.Type() {
	case pcommon.ValueTypeStr:
		return []byte(v.Str())
	case pcommon.ValueTypeBytes:
		return v.Bytes().AsRaw()
	case pcommon.ValueTypeMap, pcommon.ValueTypeSlice:
		b, err := json.Marshal(v.AsRaw())
		if err != nil {
			return nil
		}
		return b
	default:
		return nil
	}
}

// writeParquet encodes records as a Parquet file using the fixed schema.
func writeParquet(records []record) ([]byte, error) {
	var buf bytes.Buffer
	writer := parquet.NewWriter(&buf, fixedSchema,
		parquet.Compression(&snappy.Codec{}),
		parquet.MaxRowsPerRowGroup(10_000),
	)

	for _, r := range records {
		if _, err := writer.WriteRows([]parquet.Row{recordToRow(r)}); err != nil {
			return nil, fmt.Errorf("write row: %w", err)
		}
	}

	if err := writer.Close(); err != nil {
		return nil, fmt.Errorf("close parquet writer: %w", err)
	}
	return buf.Bytes(), nil
}

// recordToRow builds a parquet.Row in the column order produced by
// parquet.Group.Fields() (alphabetical). Required fields use definition
// level 0 (the only valid level for required leaves); optional fields use
// definition level 1 when present and 0 when null.
func recordToRow(r record) parquet.Row {
	row := make(parquet.Row, numColumns)
	row[idxAppname] = optString(r.appname, idxAppname)
	row[idxCollector] = optString(r.collector, idxCollector)
	row[idxEventDetailsJSON] = optBytes(r.eventDetailsJSON, idxEventDetailsJSON)
	row[idxEventMessage] = optString(r.eventMessage, idxEventMessage)
	row[idxEventTs] = parquet.Int64Value(r.eventTsMicros).Level(0, 0, idxEventTs)
	row[idxFacility] = optInt(r.facility, idxFacility)
	row[idxHostname] = optString(r.hostname, idxHostname)
	row[idxLogType] = optString(r.logType, idxLogType)
	row[idxNamespace] = optString(r.namespace, idxNamespace)
	row[idxParseStatus] = optString(r.parseStatus, idxParseStatus)
	row[idxRcvdTs] = parquet.Int64Value(r.rcvdTsMicros).Level(0, 0, idxRcvdTs)
	row[idxSeverity] = optInt(r.severity, idxSeverity)
	row[idxSourceIP] = optString(r.sourceIP, idxSourceIP)
	return row
}

func optString(p *string, idx int) parquet.Value {
	if p == nil {
		return parquet.NullValue().Level(0, 0, idx)
	}
	return parquet.ByteArrayValue([]byte(*p)).Level(0, 1, idx)
}

func optInt(p *int64, idx int) parquet.Value {
	if p == nil {
		return parquet.NullValue().Level(0, 0, idx)
	}
	return parquet.Int64Value(*p).Level(0, 1, idx)
}

func optBytes(b []byte, idx int) parquet.Value {
	if b == nil {
		return parquet.NullValue().Level(0, 0, idx)
	}
	return parquet.ByteArrayValue(b).Level(0, 1, idx)
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
