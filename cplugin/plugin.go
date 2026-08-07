// Package main provides a CGO-based C plugin interface for the qv-lite
// time-series database. Build with:
//
//	go build -buildmode=c-shared -o libqv_lite.so .
//
// Or use the included Makefile.
package main

/*
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

// --- Configuration -----------------------------------------------------------

typedef struct {
	char*   path;
	int64_t max_file_size;               // bytes, default 64 MiB
	int32_t max_file_number;
	uint8_t close_buffer;
	int32_t max_buffer_batch_size;       // default 4096
	int64_t max_segment_size;            // bytes, default 64 MiB
	int64_t max_segment_time_interval;   // seconds, 0 = no limit
	int64_t max_storage_time;            // seconds, default 3600
	int64_t data_expiration_time;        // minutes, 0 = never expire
	int64_t dedup_window_ms;
	int64_t min_interval_ms;
	char*   secondary_compression_name;  // "zstd" (default), "lz4", "snappy", "gzip", "none"
	uint8_t async_flush;
	uint8_t async_cleanup;
	int64_t cleanup_interval_seconds;    // default 60
} qv_config_t;

typedef struct {
	char*   name;
	char*   desc;
	uint8_t column_type;
	uint8_t float_precision;
} qv_table_info_t;

// --- Value type codes --------------------------------------------------------

#define QV_TYPE_EMPTY   0
#define QV_TYPE_INT64   1
#define QV_TYPE_UINT64  2
#define QV_TYPE_FLOAT64 3
#define QV_TYPE_BOOL    4
#define QV_TYPE_STRING  5
#define QV_TYPE_JSON    6
#define QV_TYPE_LIST    7
#define QV_TYPE_MAP     8

// --- Value union for float64 ↔ int64 conversion ------------------------------

typedef union { double d; int64_t i; } qv_float_bits_t;

// --- Unified value type (24 bytes) -------------------------------------------

// qv_value_t compactly stores any supported value:
//
//   scalar  → |num|        (int64/uint64/float64 bits/bool)
//   string  → |str_val|    (string / JSON)
//   list    → |num|=count, |str_val|=(char*)qv_value_t[] items
//   map     → |num|=pairs, |str_val|=(char*)qv_kv_t[] entries
//
// For LIST / MAP the caller-owned arrays are read and copied during the call.
typedef struct {
	int32_t value_type;
	int64_t num;
	char*   str_val;
} qv_value_t;

// --- Key-value pair (used by QV_TYPE_MAP) ------------------------------------

typedef struct {
	char*       key;     // copied during the call
	qv_value_t  value;
} qv_kv_t;

// --- Batch point -------------------------------------------------------------

typedef struct {
	char*       tag;
	int64_t     timestamp;
	qv_value_t  value;
} qv_batch_point_t;

// --- Query result ------------------------------------------------------------

typedef struct {
	int64_t    timestamp;
	qv_value_t value;
} qv_point_t;

typedef struct {
	qv_point_t* points;
	int32_t     count;
	char*       error;
} qv_result_t;
*/
import "C"

import (
	"context"
	"math"
	"sync"
	"sync/atomic"
	"unsafe"

	"github.com/mababaNiubi/qv-lite/tsdb"
	"github.com/mababaNiubi/variant"
)

// ---------------------------------------------------------------------------
// Handle management
// ---------------------------------------------------------------------------

type handle struct {
	mu sync.Mutex
	db *tsdb.DB
}

var (
	handles   sync.Map
	handleSeq atomic.Int64
)

func storeHandle(db *tsdb.DB) uintptr {
	seq := handleSeq.Add(1)
	handles.Store(seq, &handle{db: db})
	return uintptr(seq)
}

func loadHandle(h uintptr) *handle {
	v, ok := handles.Load(int64(h))
	if !ok {
		return nil
	}
	return v.(*handle)
}

func removeHandle(h uintptr) { handles.Delete(int64(h)) }

// ---------------------------------------------------------------------------
// Config
// ---------------------------------------------------------------------------

func fromCConfig(c *C.qv_config_t) tsdb.Config {
	return tsdb.Config{
		Path:                     C.GoString(c.path),
		MaxSegmentSize:           int64(c.max_segment_size),
		MaxSegmentTimeInterval:   int64(c.max_segment_time_interval),
		MaxStorageTime:           int64(c.max_storage_time),
		ExpirationMinuteTime:     int64(c.data_expiration_time),
		DedupWindowMs:            int64(c.dedup_window_ms),
		MinIntervalMs:            int64(c.min_interval_ms),
		SecondaryCompressionName: C.GoString(c.secondary_compression_name),
		AsyncFlush:               c.async_flush != 0,
		AsyncCleanup:             c.async_cleanup != 0,
		CleanupIntervalSeconds:   int64(c.cleanup_interval_seconds),
		WalConfig: tsdb.WalConfig{
			MaxFileSize:       int64(c.max_file_size),
			MaxFileNumber:     int(c.max_file_number),
			CloseBuffer:       c.close_buffer != 0,
			MaxBufferBatchSize: int(c.max_buffer_batch_size),
		},
	}
}

// ---------------------------------------------------------------------------
// Value conversion (C qv_value_t ↔ Go variant.Variant)
// ---------------------------------------------------------------------------

func cToVariant(cv *C.qv_value_t) (variant.Variant, error) {
	switch cv.value_type {
	case C.QV_TYPE_EMPTY:
		return variant.NewEmpty(), nil
	case C.QV_TYPE_INT64:
		return variant.NewInt64(int64(cv.num)), nil
	case C.QV_TYPE_UINT64:
		return variant.NewUInt64(uint64(cv.num)), nil
	case C.QV_TYPE_FLOAT64:
		return variant.NewFloat64(math.Float64frombits(uint64(cv.num))), nil
	case C.QV_TYPE_BOOL:
		return variant.NewBool(cv.num != 0), nil
	case C.QV_TYPE_STRING:
		return variant.NewString(C.GoString(cv.str_val)), nil
	case C.QV_TYPE_JSON:
		return variant.UnmarshalJSON([]byte(C.GoString(cv.str_val)))
	case C.QV_TYPE_LIST:
		return cListToVariant(cv)
	case C.QV_TYPE_MAP:
		return cMapToVariant(cv)
	default:
		return variant.NewEmpty(), nil
	}
}

// cListToVariant reads a C array of n qv_value_t items from cv->str_val
// and returns a Go variant list (recursive — items can be any type).
func cListToVariant(cv *C.qv_value_t) (variant.Variant, error) {
	n := int(cv.num)
	if n <= 0 {
		return variant.NewValueList(nil), nil
	}
	if n > 1<<20 {
		n = 1 << 20
	}
	items := (*[1 << 20]C.qv_value_t)(unsafe.Pointer(cv.str_val))[:n:n]
	list := make([]variant.Variant, n)
	for i := range items {
		var err error
		list[i], err = cToVariant(&items[i])
		if err != nil {
			return variant.NewEmpty(), err
		}
	}
	return variant.NewValueList(list), nil
}

// cMapToVariant reads a C array of n qv_kv_t entries from cv->str_val
// and returns a Go variant map (recursive — values can be any type).
func cMapToVariant(cv *C.qv_value_t) (variant.Variant, error) {
	n := int(cv.num)
	if n <= 0 {
		return variant.NewValueMap(nil), nil
	}
	if n > 1<<20 {
		n = 1 << 20
	}
	kv := (*[1 << 20]C.qv_kv_t)(unsafe.Pointer(cv.str_val))[:n:n]
	m := make(map[string]variant.Variant, n)
	for i := range kv {
		v, err := cToVariant(&kv[i].value)
		if err != nil {
			return variant.NewEmpty(), err
		}
		m[C.GoString(kv[i].key)] = v
	}
	return variant.NewValueMap(m), nil
}

func variantToC(v variant.Variant) C.qv_value_t {
	cv := C.qv_value_t{}
	switch v.Type() {
	case variant.TypeEmpty:
		cv.value_type = C.int32_t(C.QV_TYPE_EMPTY)
	case variant.TypeInt64:
		cv.value_type = C.int32_t(C.QV_TYPE_INT64)
		cv.num = C.int64_t(v.AsInterface().(int64))
	case variant.TypeUInt64:
		cv.value_type = C.int32_t(C.QV_TYPE_UINT64)
		cv.num = C.int64_t(v.AsInterface().(uint64))
	case variant.TypeFloat64:
		cv.value_type = C.int32_t(C.QV_TYPE_FLOAT64)
		cv.num = C.int64_t(math.Float64bits(v.AsInterface().(float64)))
	case variant.TypeBool:
		cv.value_type = C.int32_t(C.QV_TYPE_BOOL)
		if b, _ := v.AsBool(); b {
			cv.num = 1
		}
	default:
		// Map / List / unknown → serialise as string.
		cv.value_type = C.int32_t(C.QV_TYPE_STRING)
		cv.str_val = C.CString(v.AsString())
	}
	return cv
}

// ---------------------------------------------------------------------------
// Lifecycle
// ---------------------------------------------------------------------------

//export qv_open
func qv_open(config *C.qv_config_t) C.uintptr_t {
	db, err := tsdb.Open(fromCConfig(config), context.Background())
	if err != nil {
		return 0
	}
	return C.uintptr_t(storeHandle(db))
}

//export qv_close
func qv_close(h C.uintptr_t) {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		return
	}
	hd.mu.Lock()
	_ = hd.db.Close()
	hd.mu.Unlock()
	removeHandle(uintptr(h))
}

//export qv_create_table
func qv_create_table(h C.uintptr_t, info *C.qv_table_info_t) *C.char {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		return C.CString("handle not found")
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	tableInfo := tsdb.TableInfo{
		ColumnAttribute: tsdb.ColumnAttribute{
			Name:           C.GoString(info.name),
			Desc:           C.GoString(info.desc),
			Type:           tsdb.ColumnType(info.column_type),
			FloatPrecision: uint8(info.float_precision),
		},
	}
	if err := hd.db.CreateTable(tableInfo); err != nil {
		return C.CString(err.Error())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Write
// ---------------------------------------------------------------------------

//export qv_write
func qv_write(h C.uintptr_t, table *C.char, tag *C.char, timestamp C.int64_t, value *C.qv_value_t) *C.char {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		return C.CString("handle not found")
	}
	v, err := cToVariant(value)
	if err != nil {
		return C.CString("invalid value: " + err.Error())
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	_, err = hd.db.Write(C.GoString(table), C.GoString(tag), int64(timestamp), v)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

//export qv_write_batch
func qv_write_batch(h C.uintptr_t, table *C.char, points *C.qv_batch_point_t, count C.int) *C.char {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		return C.CString("handle not found")
	}
	if count <= 0 {
		return nil
	}
	n := int(count)
	if n > 1<<20 {
		n = 1 << 20
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()

	cSlice := (*[1 << 20]C.qv_batch_point_t)(unsafe.Pointer(points))[:n:n]
	tagPoints := make([]tsdb.TagPoint, 0, n)
	for i := range cSlice {
		bp := &cSlice[i]
		v, err := cToVariant(&bp.value)
		if err != nil {
			return C.CString("invalid batch value: " + err.Error())
		}
		tagPoints = append(tagPoints, tsdb.TagPoint{
			Tag:       C.GoString(bp.tag),
			Timestamp: int64(bp.timestamp),
			Value:     v,
		})
	}
	_, err := hd.db.WriteBatch(C.GoString(table), tagPoints)
	if err != nil {
		return C.CString(err.Error())
	}
	return nil
}

// ---------------------------------------------------------------------------
// Query helpers
// ---------------------------------------------------------------------------

func allocResult() *C.qv_result_t {
	sz := C.size_t(unsafe.Sizeof(C.qv_result_t{}))
	p := C.malloc(sz)
	*(*C.qv_result_t)(p) = C.qv_result_t{}
	return (*C.qv_result_t)(p)
}

func allocPoints(n int) *C.qv_point_t {
	sz := C.size_t(n) * C.size_t(unsafe.Sizeof(C.qv_point_t{}))
	return (*C.qv_point_t)(C.malloc(sz))
}

func fillResult(r *C.qv_result_t, points []tsdb.Point) {
	if len(points) == 0 {
		return
	}
	r.count = C.int32_t(len(points))
	r.points = allocPoints(len(points))
	cPoints := (*[1 << 20]C.qv_point_t)(unsafe.Pointer(r.points))[:len(points):len(points)]
	for i, p := range points {
		cPoints[i] = C.qv_point_t{
			timestamp: C.int64_t(p.Tms),
			value:     variantToC(p.V),
		}
	}
}

// ---------------------------------------------------------------------------
// Query
// ---------------------------------------------------------------------------

//export qv_query
func qv_query(h C.uintptr_t, table *C.char, tag *C.char, startTime C.int64_t, endTime C.int64_t, maxNumber C.int64_t, fusion C.uint8_t) *C.qv_result_t {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		r := allocResult()
		r.error = C.CString("handle not found")
		return r
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	points, err := hd.db.Query(C.GoString(table), C.GoString(tag), int64(startTime), int64(endTime), int64(maxNumber), uint8(fusion), nil)
	r := allocResult()
	if err != nil {
		r.error = C.CString(err.Error())
		return r
	}
	fillResult(r, points)
	return r
}

//export qv_query_all
func qv_query_all(h C.uintptr_t, table *C.char, tag *C.char, startTime C.int64_t, endTime C.int64_t) *C.qv_result_t {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		r := allocResult()
		r.error = C.CString("handle not found")
		return r
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	points, err := hd.db.QueryAll(C.GoString(table), C.GoString(tag), int64(startTime), int64(endTime), nil)
	r := allocResult()
	if err != nil {
		r.error = C.CString(err.Error())
		return r
	}
	fillResult(r, points)
	return r
}

//export qv_query_latest
func qv_query_latest(h C.uintptr_t, table *C.char, tag *C.char) *C.qv_result_t {
	hd := loadHandle(uintptr(h))
	if hd == nil {
		r := allocResult()
		r.error = C.CString("handle not found")
		return r
	}
	hd.mu.Lock()
	defer hd.mu.Unlock()
	pt, err := hd.db.QueryLatest(C.GoString(table), C.GoString(tag))
	r := allocResult()
	if err != nil {
		r.error = C.CString(err.Error())
		return r
	}
	if pt == nil {
		return r
	}
	r.count = 1
	r.points = allocPoints(1)
	*(*C.qv_point_t)(unsafe.Pointer(r.points)) = C.qv_point_t{
		timestamp: C.int64_t(pt.Tms),
		value:     variantToC(pt.V),
	}
	return r
}

// ---------------------------------------------------------------------------
// Memory
// ---------------------------------------------------------------------------

//export qv_free_result
func qv_free_result(result *C.qv_result_t) {
	if result == nil {
		return
	}
	if result.points != nil && result.count > 0 {
		n := int(result.count)
		if n > 1<<20 {
			n = 1 << 20
		}
		cPoints := (*[1 << 20]C.qv_point_t)(unsafe.Pointer(result.points))[:n:n]
		for i := range cPoints {
			if cPoints[i].value.str_val != nil {
				C.free(unsafe.Pointer(cPoints[i].value.str_val))
			}
		}
		C.free(unsafe.Pointer(result.points))
	}
	if result.error != nil {
		C.free(unsafe.Pointer(result.error))
	}
	C.free(unsafe.Pointer(result))
}

//export qv_free_string
func qv_free_string(s *C.char) {
	if s != nil {
		C.free(unsafe.Pointer(s))
	}
}

//export qv_version
func qv_version() *C.char {
	return C.CString("qv-lite 1.2.0")
}

func main() {}
