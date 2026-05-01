package parquetgcsexporter

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/parquet-go/parquet-go"
	"go.opentelemetry.io/collector/pdata/pcommon"
	"go.opentelemetry.io/collector/pdata/plog"
	"go.opentelemetry.io/otel/metric/noop"
	"go.uber.org/zap"
)

// fullBody puts every BQ-schema column into a body map so tests can verify
// the happy path. Timestamps are stored as Unix nanoseconds (BindPlane's
// convention).
func fullBody(m pcommon.Map) {
	m.PutInt("rcvd_ts", 1777494951867724305)             // 2026-04-29T20:35:51.867724305Z
	m.PutInt("event_ts", 1777494952000000000)            // 2026-04-29T20:35:52Z
	m.PutStr("collector", "stepwell-abtoll-1")
	m.PutStr("namespace", "TEN Cisco WSA")
	m.PutStr("log_type", "CISCO_WSA")
	m.PutStr("source_ip", "142.178.49.138")
	m.PutStr("hostname", "abc-secwe02.nssi.telus.com")
	m.PutStr("appname", "abc-secwe02_stepwell_access")
	m.PutInt("facility", 1)
	m.PutInt("severity", 6)
	m.PutStr("event_message", "<14>Apr 29 20:35:52 abc raw syslog line")
	m.PutStr("event_details_json", `{"cache_hit":"TCP_MISS","duration":"10040"}`)
	m.PutStr("parse_status", "parsed")
}

// --- extractRecords ---

func TestExtractRecords_FullRecord(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()
	lr := sl.LogRecords().AppendEmpty()
	fullBody(lr.Body().SetEmptyMap())

	records, skipped := extractRecords(ld)
	if skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]

	// nanos / 1000 = micros
	if r.rcvdTsMicros != 1777494951867724 {
		t.Errorf("rcvdTsMicros: got %d", r.rcvdTsMicros)
	}
	if r.eventTsMicros != 1777494952000000 {
		t.Errorf("eventTsMicros: got %d", r.eventTsMicros)
	}
	if r.collector == nil || *r.collector != "stepwell-abtoll-1" {
		t.Errorf("collector: %v", r.collector)
	}
	if r.facility == nil || *r.facility != 1 {
		t.Errorf("facility: %v", r.facility)
	}
	if r.severity == nil || *r.severity != 6 {
		t.Errorf("severity: %v", r.severity)
	}
	if string(r.eventDetailsJSON) != `{"cache_hit":"TCP_MISS","duration":"10040"}` {
		t.Errorf("eventDetailsJSON: %q", r.eventDetailsJSON)
	}
	if r.parseStatus == nil || *r.parseStatus != "parsed" {
		t.Errorf("parseStatus: %v", r.parseStatus)
	}
}

func TestExtractRecords_SkipsNonMapBody(t *testing.T) {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	sl.LogRecords().AppendEmpty().Body().SetStr("plain text")
	sl.LogRecords().AppendEmpty().Body().SetInt(42)
	fullBody(sl.LogRecords().AppendEmpty().Body().SetEmptyMap())

	records, skipped := extractRecords(ld)
	if skipped != 2 {
		t.Errorf("expected 2 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestExtractRecords_SkipsMissingRequiredTimestamps(t *testing.T) {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()

	// Missing rcvd_ts — must skip.
	m1 := sl.LogRecords().AppendEmpty().Body().SetEmptyMap()
	m1.PutInt("event_ts", 1777494952000000000)

	// Missing event_ts — must skip.
	m2 := sl.LogRecords().AppendEmpty().Body().SetEmptyMap()
	m2.PutInt("rcvd_ts", 1777494951867724305)

	// rcvd_ts present but as a string — must skip (not coercible).
	m3 := sl.LogRecords().AppendEmpty().Body().SetEmptyMap()
	m3.PutStr("rcvd_ts", "1777494951867724305")
	m3.PutInt("event_ts", 1777494952000000000)

	// Valid.
	fullBody(sl.LogRecords().AppendEmpty().Body().SetEmptyMap())

	records, skipped := extractRecords(ld)
	if skipped != 3 {
		t.Errorf("expected 3 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Errorf("expected 1 record, got %d", len(records))
	}
}

func TestExtractRecords_OptionalFieldsAbsent(t *testing.T) {
	ld := plog.NewLogs()
	m := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetEmptyMap()
	m.PutInt("rcvd_ts", 1000)
	m.PutInt("event_ts", 2000)
	// All other fields absent.

	records, _ := extractRecords(ld)
	if len(records) != 1 {
		t.Fatalf("expected 1 record")
	}
	r := records[0]
	if r.collector != nil || r.namespace != nil || r.logType != nil ||
		r.sourceIP != nil || r.hostname != nil || r.appname != nil ||
		r.facility != nil || r.severity != nil || r.eventMessage != nil ||
		r.parseStatus != nil || r.eventDetailsJSON != nil {
		t.Errorf("expected all optional fields to be nil, got %+v", r)
	}
}

func TestExtractRecords_ExtraFieldsIgnored(t *testing.T) {
	ld := plog.NewLogs()
	m := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetEmptyMap()
	fullBody(m)
	m.PutStr("not_in_schema_1", "ignored")
	m.PutInt("not_in_schema_2", 99)

	records, skipped := extractRecords(ld)
	if skipped != 0 || len(records) != 1 {
		t.Fatalf("expected 1 record, 0 skipped; got %d, %d", len(records), skipped)
	}
	// Extra fields should not surface anywhere — just confirm projection succeeded.
	if records[0].collector == nil || *records[0].collector != "stepwell-abtoll-1" {
		t.Errorf("expected projection to succeed despite extra fields")
	}
}

func TestExtractRecords_EventDetailsJSON_AsMap(t *testing.T) {
	// BindPlane normally emits event_details_json as a string. Verify the
	// defensive map path also works in case the upstream pipeline changes.
	ld := plog.NewLogs()
	m := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty().LogRecords().AppendEmpty().Body().SetEmptyMap()
	m.PutInt("rcvd_ts", 1000)
	m.PutInt("event_ts", 2000)
	details := m.PutEmptyMap("event_details_json")
	details.PutStr("cache_hit", "TCP_MISS")
	details.PutInt("duration", 10040)

	records, _ := extractRecords(ld)
	if len(records) != 1 {
		t.Fatalf("expected 1 record")
	}
	var got map[string]any
	if err := json.Unmarshal(records[0].eventDetailsJSON, &got); err != nil {
		t.Fatalf("unmarshal: %v (raw: %s)", err, records[0].eventDetailsJSON)
	}
	if got["cache_hit"] != "TCP_MISS" {
		t.Errorf("cache_hit: %v", got["cache_hit"])
	}
}

// --- writeParquet ---

func TestWriteParquet_RoundTrip(t *testing.T) {
	ld := plog.NewLogs()
	sl := ld.ResourceLogs().AppendEmpty().ScopeLogs().AppendEmpty()
	fullBody(sl.LogRecords().AppendEmpty().Body().SetEmptyMap())

	// Second record has only required fields.
	m2 := sl.LogRecords().AppendEmpty().Body().SetEmptyMap()
	m2.PutInt("rcvd_ts", 5000)
	m2.PutInt("event_ts", 6000)

	records, _ := extractRecords(ld)

	data, err := writeParquet(records)
	if err != nil {
		t.Fatalf("writeParquet: %v", err)
	}

	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if f.NumRows() != 2 {
		t.Errorf("expected 2 rows, got %d", f.NumRows())
	}

	// Verify the schema has all 13 columns with the expected names.
	want := map[string]bool{
		"rcvd_ts": false, "event_ts": false, "collector": false,
		"namespace": false, "log_type": false, "source_ip": false,
		"hostname": false, "appname": false, "facility": false,
		"severity": false, "event_message": false,
		"event_details_json": false, "parse_status": false,
	}
	for _, col := range f.Schema().Columns() {
		if len(col) == 1 {
			if _, ok := want[col[0]]; ok {
				want[col[0]] = true
			}
		}
	}
	for name, found := range want {
		if !found {
			t.Errorf("schema missing column %q", name)
		}
	}
}

func TestWriteParquet_EmptyRecords(t *testing.T) {
	data, err := writeParquet(nil)
	if err != nil {
		t.Fatalf("writeParquet([]): %v", err)
	}
	f, err := parquet.OpenFile(bytes.NewReader(data), int64(len(data)))
	if err != nil {
		t.Fatalf("OpenFile: %v", err)
	}
	if f.NumRows() != 0 {
		t.Errorf("expected 0 rows, got %d", f.NumRows())
	}
}

// --- objectPath ---

func TestObjectPath_WithPrefix(t *testing.T) {
	e := &parquetGCSExporter{
		config: &Config{
			Prefix:          "logs",
			PartitionFormat: "year=2006/month=01/day=02",
		},
	}

	path := e.objectPath()
	if !strings.HasPrefix(path, "logs/year=") {
		t.Errorf("unexpected prefix: %s", path)
	}
	if !strings.HasSuffix(path, ".parquet") {
		t.Errorf("expected .parquet suffix: %s", path)
	}

	// UUID filename: 36 chars + ".parquet" = 44 chars
	parts := strings.Split(path, "/")
	filename := parts[len(parts)-1]
	if len(filename) != 44 {
		t.Errorf("unexpected filename length %d: %s", len(filename), filename)
	}
}

func TestObjectPath_EmptyPrefix(t *testing.T) {
	e := &parquetGCSExporter{
		config: &Config{
			Prefix:          "",
			PartitionFormat: "year=2006/month=01/day=02",
		},
	}

	path := e.objectPath()
	if strings.HasPrefix(path, "/") {
		t.Errorf("path should not start with /: %s", path)
	}
	if !strings.HasPrefix(path, "year=") {
		t.Errorf("expected path to start with partition: %s", path)
	}
}

func TestObjectPath_TrailingSlashInPrefix(t *testing.T) {
	e := &parquetGCSExporter{
		config: &Config{
			Prefix:          "data/raw/",
			PartitionFormat: "year=2006/month=01/day=02",
		},
	}

	path := e.objectPath()
	if strings.Contains(path, "//") {
		t.Errorf("path should not contain double slash: %s", path)
	}
	if !strings.HasPrefix(path, "data/raw/year=") {
		t.Errorf("unexpected path: %s", path)
	}
}

func TestObjectPath_HourlyPartition(t *testing.T) {
	e := &parquetGCSExporter{
		config: &Config{
			Prefix:          "logs",
			PartitionFormat: "year=2006/month=01/day=02/hour=15",
		},
	}

	path := e.objectPath()
	if !strings.Contains(path, "hour=") {
		t.Errorf("expected hourly partition in path: %s", path)
	}
}

// --- Config.Validate ---

func TestConfigValidate_Valid(t *testing.T) {
	c := &Config{Bucket: "my-bucket"}
	if err := c.Validate(); err != nil {
		t.Errorf("valid config should not error: %v", err)
	}
}

func TestConfigValidate_EmptyBucket(t *testing.T) {
	c := &Config{}
	if err := c.Validate(); err == nil {
		t.Error("empty bucket should fail validation")
	}
}

func TestConfigValidate_ValidPartitionFormat(t *testing.T) {
	c := &Config{
		Bucket:          "b",
		PartitionFormat: "year=2006/month=01/day=02/hour=15",
	}
	if err := c.Validate(); err != nil {
		t.Errorf("valid partition format should not error: %v", err)
	}
}

func TestConfigValidate_StaticPartitionFormat(t *testing.T) {
	c := &Config{
		Bucket:          "b",
		PartitionFormat: "static-path",
	}
	if err := c.Validate(); err == nil {
		t.Error("static partition format should fail validation")
	}
}

func TestConfigValidate_CredentialsFileExists(t *testing.T) {
	tmp := filepath.Join(t.TempDir(), "creds.json")
	os.WriteFile(tmp, []byte(`{}`), 0644)

	c := &Config{Bucket: "b", CredentialsFile: tmp}
	if err := c.Validate(); err != nil {
		t.Errorf("existing credentials file should not error: %v", err)
	}
}

func TestConfigValidate_CredentialsFileMissing(t *testing.T) {
	c := &Config{Bucket: "b", CredentialsFile: "/nonexistent/path/creds.json"}
	if err := c.Validate(); err == nil {
		t.Error("missing credentials file should fail validation")
	}
}

// --- newExporter ---

func TestNewExporter(t *testing.T) {
	cfg := &Config{Bucket: "test"}
	logger := zap.NewNop()
	exp, err := newExporter(cfg, logger, noop.NewMeterProvider())
	if err != nil {
		t.Fatalf("newExporter: %v", err)
	}

	if exp.config != cfg {
		t.Error("config mismatch")
	}
	if exp.logger != logger {
		t.Error("logger mismatch")
	}
	if exp.client != nil {
		t.Error("client should be nil initially")
	}
	if exp.recordsExported == nil {
		t.Error("recordsExported metric should be initialized")
	}
	if exp.recordsSkipped == nil {
		t.Error("recordsSkipped metric should be initialized")
	}
	if exp.uploadBytes == nil {
		t.Error("uploadBytes metric should be initialized")
	}
	if exp.uploadDuration == nil {
		t.Error("uploadDuration metric should be initialized")
	}
}
