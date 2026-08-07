// qv_lite.h — C API for the qv-lite embedded time-series database.
//
// This header is the public contract. Link against libqv_lite.{so,dll,dylib}
// (produced by `go build -buildmode=c-shared`) or libqv_lite.a (c-archive).
#ifndef QV_LITE_H
#define QV_LITE_H

#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#ifdef __cplusplus
extern "C" {
#endif

// ---- Opaque handle ----------------------------------------------------------

typedef uintptr_t qv_handle;   // 0 = invalid / failed-open

// ---- Configuration ----------------------------------------------------------

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

// ---- Table creation ---------------------------------------------------------

typedef struct {
    char*   name;
    char*   desc;
    uint8_t column_type;       // 0=unknown 1=int 2=float 3=string 4=bool 5=json 6=structure
    uint8_t float_precision;
} qv_table_info_t;

// ---- Value type codes -------------------------------------------------------

#define QV_TYPE_EMPTY   0
#define QV_TYPE_INT64   1
#define QV_TYPE_UINT64  2
#define QV_TYPE_FLOAT64 3
#define QV_TYPE_BOOL    4
#define QV_TYPE_STRING  5
#define QV_TYPE_JSON    6
#define QV_TYPE_LIST    7
#define QV_TYPE_MAP     8

// ---- Value union (internal) -------------------------------------------------

typedef union { double d; int64_t i; } qv_float_bits_t;

// ---- Unified value type (24 bytes) ------------------------------------------

// qv_value_t compactly stores any supported value:
//
//   scalar  → |num|        (int64/uint64/float64 bits/bool)
//   string  → |str_val|    (string / JSON)
//   list    → |num|=count, |str_val|=(char*)qv_value_t[] items
//   map     → |num|=pairs, |str_val|=(char*)qv_kv_t[] entries
//
// Arrays are read during the call (qv_write / qv_write_batch copies the data);
// the caller retains ownership.
typedef struct {
    int32_t value_type;
    int64_t num;
    char*   str_val;
} qv_value_t;

// ---- Key-value pair ---------------------------------------------------------

typedef struct {
    char*       key;     // copied during the call
    qv_value_t  value;
} qv_kv_t;

// ---- Constructors -----------------------------------------------------------
// Return-by-value, no allocation. str_val arrays are read during qv_write.

static inline qv_value_t qv_int64(int64_t x) {
    return (qv_value_t){.value_type = QV_TYPE_INT64, .num = x};
}
static inline qv_value_t qv_uint64(uint64_t x) {
    return (qv_value_t){.value_type = QV_TYPE_UINT64, .num = (int64_t)x};
}
static inline qv_value_t qv_float64(double x) {
    qv_float_bits_t u = {.d = x};
    return (qv_value_t){.value_type = QV_TYPE_FLOAT64, .num = u.i};
}
static inline qv_value_t qv_bool(int x) {
    return (qv_value_t){.value_type = QV_TYPE_BOOL, .num = x ? 1 : 0};
}
static inline qv_value_t qv_string(char* s) {
    return (qv_value_t){.value_type = QV_TYPE_STRING, .str_val = s};
}
static inline qv_value_t qv_json(char* s) {
    return (qv_value_t){.value_type = QV_TYPE_JSON, .str_val = s};
}
static inline qv_value_t qv_list(const qv_value_t* items, int n) {
    return (qv_value_t){.value_type = QV_TYPE_LIST, .num = n, .str_val = (char*)items};
}
static inline qv_value_t qv_map(const qv_kv_t* pairs, int n) {
    return (qv_value_t){.value_type = QV_TYPE_MAP, .num = n, .str_val = (char*)pairs};
}

// ---- Accessors --------------------------------------------------------------

static inline int64_t qv_as_int64(qv_value_t v)   { return v.num; }
static inline uint64_t qv_as_uint64(qv_value_t v) { return (uint64_t)v.num; }
static inline double qv_as_float64(qv_value_t v)  {
    qv_float_bits_t u = {.i = v.num}; return u.d;
}
static inline int qv_as_bool(qv_value_t v)        { return v.num != 0; }
static inline char* qv_as_string(qv_value_t v)    { return v.str_val; }

// ---- Batch point ------------------------------------------------------------

typedef struct {
    char*       tag;
    int64_t     timestamp;        // Unix nanosecond
    qv_value_t  value;
} qv_batch_point_t;

// ---- Query result -----------------------------------------------------------

typedef struct {
    int64_t    timestamp;
    qv_value_t value;
} qv_point_t;

typedef struct {
    qv_point_t* points;
    int32_t     count;
    char*       error;            // NULL on success
} qv_result_t;

// ---- API --------------------------------------------------------------------

// Open a database. Returns 0 on failure.
qv_handle qv_open(qv_config_t config);

// Close a database. NULL-safe.
void qv_close(qv_handle h);

// Create a table. Returns NULL or an error string (free with qv_free_string).
char* qv_create_table(qv_handle h, qv_table_info_t info);

// Write a single point. table/tag may be "" (default). timestamp is Unix
// nanoseconds. value.str_val is copied — caller retains ownership. Returns NULL
// on success or an error string (free with qv_free_string).
char* qv_write(qv_handle h, char* table, char* tag, int64_t timestamp, qv_value_t value);

// Batch write. One lock acquisition for all points. Tag strings and
// value.str_val are copied during the call — caller may free/reuse the array
// immediately. Returns NULL or an error string.
char* qv_write_batch(qv_handle h, char* table, const qv_batch_point_t* points, int count);

// Query data within [startTime, endTime] (nanoseconds, inclusive).
//
//  qv_query        — at most maxNumber points, aggregated (0=avg 1=min 2=max).
//  qv_query_all    — all matching points, no aggregation.
//  qv_query_latest — the most recent point for this tag.
//
// Free the result with qv_free_result. On error, result->error is non-NULL.
qv_result_t* qv_query       (qv_handle h, char* table, char* tag, int64_t startTime, int64_t endTime, int64_t maxNumber, uint8_t fusion);
qv_result_t* qv_query_all   (qv_handle h, char* table, char* tag, int64_t startTime, int64_t endTime);
qv_result_t* qv_query_latest(qv_handle h, char* table, char* tag);

// Free a result set. NULL-safe.
void qv_free_result(qv_result_t* r);

// Free a string returned by the API. NULL-safe.
void qv_free_string(char* s);

// Library version string. Free with qv_free_string.
char* qv_version(void);

#ifdef __cplusplus
}
#endif

#endif // QV_LITE_H
