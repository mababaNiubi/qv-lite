# qv-lite C Plugin

CGO bridge that packages the qv-lite time-series database as a native C shared/static library.

## Directory layout

```
cplugin/
├── plugin.go          # Go CGO source (package main, 14 //export functions)
├── qv_lite.h          # Public C header (with inline constructors & accessors)
├── Makefile           # Linux / macOS / Windows build targets
├── example/
│   ├── example.c      # Basic usage demonstration (all value types)
│   ├── perf_test.c    # Large-scale write & query benchmark
│   └── perf_test2.c   # CGO vs batch performance comparison
├── build/             # Build output directory (created by make)
└── README.md
```

## Prerequisites

- **Go 1.24+**
- **MinGW-w64** (Windows) or **gcc** (Linux/macOS) — provides the C compiler required by cgo
- On Windows: download [winlibs.com](https://winlibs.com) or [mingw-builds](https://github.com/niXman/mingw-builds-binaries/releases), extract to `D:\mingw64`, and add `D:\mingw64\mingw64\bin` to `PATH`.

## Quick start

```bash
# Set GCC path (Windows example)
export PATH="/d/mingw64/mingw64/bin:$PATH"

# Build the CGO shared library
cd plugin
CGO_ENABLED=1 go build -buildmode=c-shared -o build/qv_lite.dll .
cp qv_lite.h build/

# Build and run the example
gcc -O2 -o build/example.exe example/example.c -Ibuild -Lbuild -lqv_lite -lpthread
./build/example.exe

# Build and run the performance benchmark
gcc -O2 -o build/perf_test.exe example/perf_test.c -Ibuild -Lbuild -lqv_lite -lpthread
./build/perf_test.exe
```

On Linux/macOS replace `.dll` with `.so` and drop the `.exe` extension.

## API reference

### qv_value_t — unified value type (24 bytes)

A single compact type used for **writes**, **batch writes**, and **query results**.

```c
typedef struct {
    int32_t value_type;   // QV_TYPE_* — tells which field to read
    int64_t num;          // QV_TYPE_INT64 / UINT64 / FLOAT64 (bit-cast) / BOOL
    char*   str_val;      // QV_TYPE_STRING / JSON / LIST items / MAP entries
} qv_value_t;
```

| value_type | Storage | Constructor | Accessor |
|---|---|---|---|
| `QV_TYPE_EMPTY` (0) | — | — | — |
| `QV_TYPE_INT64` (1) | `num` | `qv_int64(x)` | `qv_as_int64(v)` |
| `QV_TYPE_UINT64` (2) | `num` | `qv_uint64(x)` | `qv_as_uint64(v)` |
| `QV_TYPE_FLOAT64` (3) | `num` (bit-cast) | `qv_float64(x)` | `qv_as_float64(v)` |
| `QV_TYPE_BOOL` (4) | `num` (0/1) | `qv_bool(x)` | `qv_as_bool(v)` |
| `QV_TYPE_STRING` (5) | `str_val` | `qv_string(s)` | `qv_as_string(v)` |
| `QV_TYPE_JSON` (6) | `str_val` | `qv_json(s)` | `qv_as_string(v)` |
| `QV_TYPE_LIST` (7) | `num`=count, `str_val`=items[] | `qv_list(arr, n)` | — |
| `QV_TYPE_MAP` (8) | `num`=count, `str_val`=kv[] | `qv_map(pairs, n)` | — |

Float64 is stored via union bit-cast (`union { double d; int64_t i; }`), not pointer aliasing — well-defined C, zero overhead.

### Functions

| Category | Function | Returns |
|---|---|---|
| Lifecycle | `qv_open(config)` | Handle (0 on failure) |
| | `qv_close(handle)` | — |
| | `qv_create_table(handle, info)` | `char*` error or NULL |
| Write | `qv_write(h, table, tag, ts, value)` | `char*` error or NULL |
| | `qv_write_batch(h, table, points, n)` | `char*` error or NULL |
| Query | `qv_query(h, table, tag, from, to, windowSize, fusion)` | `qv_result_t*` |
| | `qv_query_all(h, table, tag, from, to)` | `qv_result_t*` |
| | `qv_query_latest(h, table, tag)` | `qv_result_t*` |
| Memory | `qv_free_result(r)` | — |
| | `qv_free_string(s)` | — |
| Misc | `qv_version()` | `char*` |

Write/query errors are returned as `char*` strings (or `result->error` for query), to be freed with `qv_free_string`.

Query results (`qv_result_t*`) must be freed with `qv_free_result`. Structured types (list/map) are returned as `QV_TYPE_STRING` with a JSON representation.

### Usage examples

```c
// ── Single writes (all types via one function) ──
qv_write(db, "", "tag", ts, qv_int64(42));
qv_write(db, "", "tag", ts, qv_float64(3.14));
qv_write(db, "", "tag", ts, qv_string("hello"));
qv_write(db, "", "tag", ts, qv_bool(1));

// ── List and map (zero JSON overhead) ──
qv_value_t items[] = {qv_int64(1), qv_float64(2.5), qv_string("three")};
qv_write(db, "", "sensor", ts, qv_list(items, 3));

qv_kv_t pairs[] = {
    {.key = "name",  .value = qv_string("device")},
    {.key = "value", .value = qv_float64(99.9)},
};
qv_write(db, "", "sensor", ts, qv_map(pairs, 2));

// Nesting works recursively
qv_kv_t nested[] = {
    {.key = "id",   .value = qv_int64(1)},
    {.key = "data", .value = qv_list(items, 3)},
};
qv_write(db, "", "sensor", ts, qv_map(nested, 2));

// ── Batch write (mixed types, one lock acquisition) ──
qv_batch_point_t batch[] = {
    {.tag = "a", .timestamp = ts, .value = qv_int64(100)},
    {.tag = "b", .timestamp = ts, .value = qv_json("{\"x\":1}")},
    {.tag = "a", .timestamp = ts + 1, .value = qv_list(items, 3)},
};
qv_write_batch(db, "", batch, 3);

// ── Query ──
qv_result_t* r = qv_query_all(db, "", "tag", t0, t1);
for (int i = 0; i < r->count; i++) {
    qv_value_t v = r->points[i].value;
    switch (v.value_type) {
    case QV_TYPE_INT64:   printf("%lld\n", (long long)qv_as_int64(v));   break;
    case QV_TYPE_FLOAT64: printf("%f\n",   qv_as_float64(v));           break;
    case QV_TYPE_STRING:  printf("%s\n",   qv_as_string(v));            break;
    case QV_TYPE_BOOL:    printf("%d\n",   qv_as_bool(v));              break;
    }
}
qv_free_result(r);
```

## Performance caveat: CGO overhead

Each `//export` function call crosses the C ↔ Go language boundary, which
involves stack switching, scheduler interaction, parameter marshalling, and
string allocation. A single `qv_write` spends a fixed overhead of roughly
**20 μs** inside cgo glue code before reaching `tsdb.Write`.

`qv_write_batch` amortises this cost: one boundary crossing per batch. In
benchmarks **`qv_write_batch` achieves the same throughput as calling
`tsdb.WriteBatch` from pure Go** — the CGO overhead becomes negligible when
spread over hundreds or thousands of points.

**Recommendation for high-throughput applications:**

```c
// C-side batching — use qv_write_batch instead of individual qv_write calls.
#define BUF_SIZE 1000
qv_batch_point_t buf[BUF_SIZE];
int bi = 0;

for (...) {
    buf[bi++] = (qv_batch_point_t){.tag = ..., .timestamp = ..., .value = ...};
    if (bi == BUF_SIZE) {
        qv_write_batch(db, "", buf, bi);
        bi = 0;
    }
}
if (bi > 0) qv_write_batch(db, "", buf, bi);
```

Additionally, the first write to a previously unseen tag triggers a metadata
`fsync` inside the Go layer (`tsdb.Meta.addTag`). Pre-creating columns via a
batch write before heavy ingestion avoids accumulating this cost per tag.

## Configuration defaults

| Field | Default |
|---|---|
| `max_file_size` | 64 MiB |
| `max_buffer_batch_size` | 4096 |
| `max_segment_size` | 64 MiB |
| `max_storage_time` | 3600 s (1 hour) |
| `secondary_compression_name` | `"zstd"` |
| `cleanup_interval_seconds` | 60 |

## Thread safety

Each handle is independently locked via a mutex. Multiple threads can call
write/query concurrently on the same handle; operations are serialised.

## Memory model

All structures returned to C are allocated on the C heap (`malloc`). The Go GC
does not manage them — the caller must release them:

- `qv_result_t*` → `qv_free_result`
- Error/version `char*` → `qv_free_string`
- String values inside `qv_point_t` are freed automatically by `qv_free_result`

## Static library

```bash
CGO_ENABLED=1 go build -buildmode=c-archive -o build/libqv_lite.a .
```

Link with `-lqv_lite -lpthread -lm` on Linux, or `qv_lite.lib` + `-lpthread` on Windows.
