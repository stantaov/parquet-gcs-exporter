package parquetgcsexporter

import (
	"bytes"
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

// --- extractBodyMaps ---

func TestExtractBodyMaps_TypedValues(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	lr := sl.LogRecords().AppendEmpty()
	m := lr.Body().SetEmptyMap()
	m.PutStr("host", "web-01")
	m.PutInt("status", 200)
	m.PutDouble("latency", 1.5)
	m.PutBool("ok", true)

	records, skipped := extractBodyMaps(ld)
	if skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	r := records[0]

	if v := r["host"]; v.vType != pcommon.ValueTypeStr || v.str != "web-01" {
		t.Errorf("host: got %+v", v)
	}
	if v := r["status"]; v.vType != pcommon.ValueTypeInt || v.i64 != 200 {
		t.Errorf("status: got %+v", v)
	}
	if v := r["latency"]; v.vType != pcommon.ValueTypeDouble || v.f64 != 1.5 {
		t.Errorf("latency: got %+v", v)
	}
	if v := r["ok"]; v.vType != pcommon.ValueTypeBool || v.b != true {
		t.Errorf("ok: got %+v", v)
	}
}

func TestExtractBodyMaps_SkipsNonMap(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	// String body — should be skipped.
	lr1 := sl.LogRecords().AppendEmpty()
	lr1.Body().SetStr("plain text log line")

	// Map body — should be included.
	lr2 := sl.LogRecords().AppendEmpty()
	lr2.Body().SetEmptyMap().PutStr("key", "val")

	records, skipped := extractBodyMaps(ld)
	if skipped != 1 {
		t.Errorf("expected 1 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
	if records[0]["key"].str != "val" {
		t.Errorf("unexpected value: %+v", records[0]["key"])
	}
}

func TestExtractBodyMaps_MultipleSkipped(t *testing.T) {
	ld := plog.NewLogs()
	rl := ld.ResourceLogs().AppendEmpty()
	sl := rl.ScopeLogs().AppendEmpty()

	// Three non-map bodies.
	sl.LogRecords().AppendEmpty().Body().SetStr("line 1")
	sl.LogRecords().AppendEmpty().Body().SetStr("line 2")
	sl.LogRecords().AppendEmpty().Body().SetInt(42)

	// One map body.
	sl.LogRecords().AppendEmpty().Body().SetEmptyMap().PutStr("k", "v")

	records, skipped := extractBodyMaps(ld)
	if skipped != 3 {
		t.Errorf("expected 3 skipped, got %d", skipped)
	}
	if len(records) != 1 {
		t.Fatalf("expected 1 record, got %d", len(records))
	}
}

func TestExtractBodyMaps_MultipleResourceAndScopes(t *testing.T) {
	ld := plog.NewLogs()

	for i := 0; i < 2; i++ {
		rl := ld.ResourceLogs().AppendEmpty()
		sl := rl.ScopeLogs().AppendEmpty()
		lr := sl.LogRecords().AppendEmpty()
		lr.Body().SetEmptyMap().PutStr("src", "resource")
	}

	records, skipped := extractBodyMaps(ld)
	if skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", skipped)
	}
	if len(records) != 2 {
		t.Fatalf("expected 2 records across resources, got %d", len(records))
	}
}

func TestExtractBodyMaps_Empty(t *testing.T) {
	ld := plog.NewLogs()
	records, skipped := extractBodyMaps(ld)
	if skipped != 0 {
		t.Errorf("expected 0 skipped, got %d", skipped)
	}
	if len(records) != 0 {
		t.Fatalf("expected 0 records, got %d", len(records))
	}
}

// --- inferSchema ---

func TestInferSchema_ConsistentTypes(t *testing.T) {
	records := []map[string]typedValue{
		{"count": {vType: pcommon.ValueTypeInt, i64: 1}, "name": {vType: pcommon.ValueTypeStr, str: "x"}},
		{"count": {vType: pcommon.ValueTypeInt, i64: 2}, "name": {vType: pcommon.ValueTypeStr, str: "y"}},
	}

	keys, ct := inferSchema(records)
	if len(keys) != 2 || keys[0] != "count" || keys[1] != "name" {
		t.Fatalf("keys: %v", keys)
	}
	if ct["count"] != colInt64 {
		t.Errorf("count: expected colInt64, got %d", ct["count"])
	}
	if ct["name"] != colString {
		t.Errorf("name: expected colString, got %d", ct["name"])
	}
}

func TestInferSchema_MixedTypesFallbackToString(t *testing.T) {
	records := []map[string]typedValue{
		{"x": {vType: pcommon.ValueTypeInt, i64: 42, str: "42"}},
		{"x": {vType: pcommon.ValueTypeStr, str: "hello"}},
	}

	_, ct := inferSchema(records)
	if ct["x"] != colString {
		t.Errorf("x: expected colString (mixed), got %d", ct["x"])
	}
}

func TestInferSchema_SortedKeys(t *testing.T) {
	records := []map[string]typedValue{
		{
			"zebra":  {vType: pcommon.ValueTypeStr},
			"alpha":  {vType: pcommon.ValueTypeStr},
			"middle": {vType: pcommon.ValueTypeStr},
		},
	}

	keys, _ := inferSchema(records)
	if keys[0] != "alpha" || keys[1] != "middle" || keys[2] != "zebra" {
		t.Errorf("expected sorted keys, got %v", keys)
	}
}

func TestInferSchema_AllColumnTypes(t *testing.T) {
	records := []map[string]typedValue{
		{
			"s": {vType: pcommon.ValueTypeStr, str: "hi"},
			"i": {vType: pcommon.ValueTypeInt, i64: 10},
			"d": {vType: pcommon.ValueTypeDouble, f64: 3.14},
			"b": {vType: pcommon.ValueTypeBool, b: true},
		},
	}

	_, ct := inferSchema(records)
	if ct["s"] != colString {
		t.Errorf("s: expected colString")
	}
	if ct["i"] != colInt64 {
		t.Errorf("i: expected colInt64")
	}
	if ct["d"] != colDouble {
		t.Errorf("d: expected colDouble")
	}
	if ct["b"] != colBool {
		t.Errorf("b: expected colBool")
	}
}

// --- valueTypeToColumnType ---

func TestValueTypeToColumnType(t *testing.T) {
	tests := []struct {
		vt   pcommon.ValueType
		want columnType
	}{
		{pcommon.ValueTypeInt, colInt64},
		{pcommon.ValueTypeDouble, colDouble},
		{pcommon.ValueTypeBool, colBool},
		{pcommon.ValueTypeStr, colString},
		{pcommon.ValueTypeMap, colString},
		{pcommon.ValueTypeSlice, colString},
	}
	for _, tt := range tests {
		if got := valueTypeToColumnType(tt.vt); got != tt.want {
			t.Errorf("valueTypeToColumnType(%v) = %d, want %d", tt.vt, got, tt.want)
		}
	}
}

// --- writeParquet ---

func TestWriteParquet_RoundTrip(t *testing.T) {
	records := []map[string]typedValue{
		{
			"name":   {vType: pcommon.ValueTypeStr, str: "alice"},
			"age":    {vType: pcommon.ValueTypeInt, i64: 30, str: "30"},
			"score":  {vType: pcommon.ValueTypeDouble, f64: 9.5, str: "9.5"},
			"active": {vType: pcommon.ValueTypeBool, b: true, str: "true"},
		},
		{
			"name": {vType: pcommon.ValueTypeStr, str: "bob"},
			"age":  {vType: pcommon.ValueTypeInt, i64: 25, str: "25"},
			// score and active missing → null
		},
	}
	keys, colTypes := inferSchema(records)

	data, err := writeParquet(records, keys, colTypes)
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
}

func TestWriteParquet_AllStrings(t *testing.T) {
	records := []map[string]typedValue{
		{"a": {vType: pcommon.ValueTypeStr, str: "hello"}},
		{"a": {vType: pcommon.ValueTypeStr, str: "world"}},
	}
	keys := []string{"a"}
	colTypes := map[string]columnType{"a": colString}

	data, err := writeParquet(records, keys, colTypes)
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
}

func TestWriteParquet_NullableColumns(t *testing.T) {
	records := []map[string]typedValue{
		{
			"a": {vType: pcommon.ValueTypeStr, str: "x"},
			"b": {vType: pcommon.ValueTypeStr, str: "y"},
		},
		{
			"a": {vType: pcommon.ValueTypeStr, str: "z"},
		},
	}
	keys, colTypes := inferSchema(records)

	data, err := writeParquet(records, keys, colTypes)
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
}

func TestWriteParquet_MixedTypeFallback(t *testing.T) {
	records := []map[string]typedValue{
		{"val": {vType: pcommon.ValueTypeInt, i64: 42, str: "42"}},
		{"val": {vType: pcommon.ValueTypeStr, str: "hello"}},
	}
	keys, colTypes := inferSchema(records)

	if colTypes["val"] != colString {
		t.Fatalf("expected colString for mixed type, got %d", colTypes["val"])
	}

	data, err := writeParquet(records, keys, colTypes)
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
