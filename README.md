<p align="center">
  <img src="./logo.png" alt="qv-lite logo" width="200" />
</p>

<h1 align="center">qv-lite</h1>

<p align="center">
  <strong>Lightweight Edge Embeddable KV Time-Series Database Engine.</strong>
</p>

<p align="center">
  <a href="./README_CN.md">中文</a> | <strong>English</strong>
</p>

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Usage](#usage)
  - [Open & Close](#open--close)
  - [Write](#write)
  - [Query](#query)
  - [Table Management](#table-management)
  - [Data Types](#data-types)
  - [Aggregation](#aggregation)
  - [Condition Filtering](#condition-filtering)
- [Configuration](#configuration)
- [Compression Algorithms](#compression-algorithms)
- [Data Encoding](#data-encoding)
- [Dependencies](#dependencies)

## Features

- **Embedded & Lightweight** — Designed for edge gateways, industrial controllers, and IoT devices with limited CPU/memory/disk.
- **Minimal & Self-Contained** — Compact, auditable codebase with minimal third-party dependencies. Straightforward on-disk layout: one metadata file per table, timestamp-named segment files, and WAL logs — no heavy database engine, easy to inspect, back up, and migrate.
- **High Write Throughput** — Single-threaded synchronous design. Achieves **8,000,000+ points/second** single-node write performance.
- **Adaptive Type Encoding** — Auto-detects input data structure at runtime and selects the optimal compression codec per data type for maximum storage efficiency.
- **High Compression Ratio** — Compact data encoding plus secondary block compression for extreme storage savings.
- **Multi-Table Support** — Manage multiple independent tables, each with its own schema definition and column set.
- **Downsampling Queries** — Sliding-window aggregation (avg / min / max) for long time-range queries.
- **Data Expiration** — Configurable time-based automatic data eviction.
- **Dedup & Min Interval** — Configurable deduplication window and minimum write interval to prevent duplicate data.
- **Block-Level Index** — Binary-search block index for fast time-range filtering without scanning irrelevant data.

## Installation

```bash
go get github.com/mababaNiubi/qv-lite/tsdb
```

## Usage

### Open & Close

```go
import (
    "context"
    "github.com/mababaNiubi/qv-lite/tsdb"
)

db, err := tsdb.Open(tsdb.Config{
    Path: "./qvLite-data",
}, context.Background())
if err != nil {
    panic(err)
}
defer db.Close()
```

`Open` creates or opens a database at the given path. The `default` table is auto-created on first use. `Close()` flushes all buffered data and releases resources.

### Write

```go
import (
    "time"
    "github.com/mababaNiubi/variant"
)

// Write to the default table (tableName = "" or "default")
written, err := db.Write("", "sensor_temp", time.Now().UnixNano(), variant.New(25.6))

// Write to a named table
written, err := db.Write("metrics", "cpu_usage", time.Now().UnixNano(), variant.New(42.5))
```

- `tableName` — empty string or `"default"` writes to the auto-created default table.
- `tag` — the time-series identifier (e.g. sensor name, metric key).
- `timestamp` — Unix timestamp in **nanoseconds**.
- `value` — use `variant.New(v)` to wrap any supported Go value.
- Returns `(true, nil)` if written; `(false, nil)` if skipped by dedup or min-interval rules.

### Query

```go
// Range query with downsampling — best for long time ranges
points, err := db.Query("default", "sensor_temp",
    time.Now().Add(-1*time.Hour).UnixNano(), // startTime (ns)
    time.Now().UnixNano(),                   // endTime (ns)
    1000,                                    // maxNumber of returned points
    tsdb.AvgFusion,                          // aggregation mode
    nil,                                     // condition filter (nil = no filter)
)

// Fetch all raw data (no downsampling)
points, err := db.QueryAll("default", "sensor_temp",
    time.Now().Add(-30*time.Minute).UnixNano(),
    time.Now().UnixNano(),
    nil,
)

// Get the latest value for a tag
point, err := db.QueryLatest("default", "sensor_temp")
if point != nil {
    fmt.Printf("latest: time=%d, value=%v\n", point.Tms, point.V)
}
```

| Method | Description |
|--------|-------------|
| `Query(tableName, tag, startTime, endTime, maxNumber, polymerization, cond)` | Range query. For spans > 1 hour, returns up to `maxNumber` downsampled points. For spans ≤ 1 hour, returns all raw points directly. |
| `QueryAll(tableName, tag, startTime, endTime, cond)` | Returns all raw data points in the range without limit. |
| `QueryLatest(tableName, tag)` | Returns the most recent point for the given tag. |

All timestamps are in **nanoseconds** (UnixNano). `maxNumber` defaults to 10000 when set to 0.

### Table Management

```go
// Create a simple single-column table — highest write performance
err := db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "device",
        Desc: "attributes",
        Type: tsdb.ColumnTypeFloat,
        FloatPrecision: 2,
    },
})

// Create a multi-column table
err = db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "metrics",
        Desc: "Dev01.CPU",
        Type: tsdb.ColumnTypeStructure,
        Structure: []tsdb.ColumnAttribute{
            {Name: "value",   Type: tsdb.ColumnTypeFloat, FloatPrecision: 2}, // auto-calculate precision
            {Name: "quality", Type: tsdb.ColumnTypeInt},
            {Name: "status",  Type: tsdb.ColumnTypeString},
        },
    },
})

// Create an adaptive-schema table (auto-discovers fields at runtime, slower than fixed schema)
err = db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "events",
        Desc: "dynamic event log",
        Type: tsdb.ColumnTypeUnknown, // adaptive — discovers field format at runtime
    },
})
```

Table metadata is persisted in `{db_path}/table.json` and automatically reloaded on next `Open`.

### Data Types

| Constant | Type | Description |
|----------|------|-------------|
| `ColumnTypeUnknown` (0) | Adaptive | Auto-detect nested structure at runtime |
| `ColumnTypeInt` (1) | Integer | Signed 64-bit integer |
| `ColumnTypeFloat` (2) | Float | 64-bit float with configurable decimal precision |
| `ColumnTypeString` (3) | String | UTF-8 string |
| `ColumnTypeBool` (4) | Boolean | true / false |
| `ColumnTypeJson` (5) | JSON | Arbitrary nested variant |
| `ColumnTypeStructure` (6) | Fixed Struct | Pre-defined column schema |

### Aggregation

| Constant | Value | Description |
|----------|-------|-------------|
| `AvgFusion` | 0 | Sliding-window average |
| `MinFusion` | 1 | Sliding-window minimum |
| `MaxFusion` | 2 | Sliding-window maximum |

### Condition Filtering

The `cond` parameter in query methods supports filtering data points by value:

```go
// Equality filter — only return points where value == "ok"
cond := tsdb.Condition{
    Operator: tsdb.OpEqual,
    Value:    variant.New("ok"),
}
points, err := db.QueryAll("default", "status", startTs, endTs, cond)

// Logical AND — combine multiple conditions
logicalCond := tsdb.LogicalCondition{
    Op:   tsdb.LogicalAnd,
    Cond: []any{
        tsdb.Condition{Operator: tsdb.OpGreaterThan, Value: variant.New(80)},
        tsdb.Condition{Operator: tsdb.OpLessThan,    Value: variant.New(100)},
    },
}
points, err := db.QueryAll("default", "cpu", startTs, endTs, logicalCond)
```

## Configuration

### Config

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `Path` | `string` | `"./qvLite-data"` | Database data storage path |
| `WalConfig` | `WalConfig` | — | WAL settings (see below) |
| `MaxSegmentSize` | `int64` | `67108864` (64MB) | Maximum segment file size in bytes |
| `MaxSegmentTimeInterval` | `int64` | `0` (unlimited) | Maximum segment time span in seconds |
| `MaxStorageTime` | `int64` | `3600` (1 hour) | Reject writes whose timestamps are too far ahead of current time |
| `ExpirationMinuteTime` | `int64` | `0` (disabled) | Auto-evict data older than this many minutes (checked on each write) |
| `DedupWindowMs` | `int64` | `0` (disabled) | Dedup window in ms — skips writes with the same value for the same tag within this window |
| `MinIntervalMs` | `int64` | `0` (disabled) | Minimum interval between consecutive writes in ms — writes arriving too quickly are skipped |
| `SecondaryCompressionName` | `string` | `"zstd"` | Block compression: `"zstd"`, `"lz4"`, `"snappy"`, `"gzip"`, `"none"` |

### WalConfig

| Field | Type | Default | Description |
|-------|------|---------|-------------|
| `MaxCacheSize` | `int64` | `67108864` (64MB) | Maximum WAL in-memory cache size in bytes |
| `MaxFileNumber` | `int` | — | Maximum number of WAL files |
| `CloseBuffer` | `bool` | `false` | Whether to disable in-memory WAL buffering |
| `MaxBufferBatchSize` | `int` | `10000` | Max entries to buffer before sorting and flushing |

**`CloseBuffer` behavior:**

| Scenario | `CloseBuffer = true` | `CloseBuffer = false` |
|----------|---------------------|----------------------|
| Out-of-order writes | Rejected for the same key | Allowed within `MaxBufferBatchSize` batch window |
| Write/query performance | Lower | Higher |
| Crash safety | Data safe | May lose buffered data |
| Memory usage | Typically within `MaxCacheSize` | ~3–4× `MaxCacheSize` |

> Control database memory usage by tuning `MaxCacheSize`.

## Compression Algorithms

| Data Type | Strategy |
|-----------|----------|
| Timestamp | delta-of-delta + scaling + simple8b / RLE |
| Integer | delta + zigzag + simple8b / RLE |
| Float | XOR-delta with mantissa truncation based on decimal precision |
| Boolean | bit-packed (8 per byte) or all-true/all-false RLE |
| String | uvarint length-prefixed concatenation + Snappy |
| JSON | variant binary serialization + LZ4 |
| Fixed Struct | column-by-column encoding, each with typed sub-encoder |
| Adaptive Struct | auto-discovers columns from Maps, recursive nesting, self-describing |

## Data Encoding

### On-Disk Layout

```
{db_path}/
  table.json              # Table metadata
  default/                # Default table directory
    meta.json             # Tag dictionary
    data/                 # Segment data files
      1234567890.tsb      # Segment (BlockFile format)
      1234567890.tsb.idx  # Segment block index
    wal/                  # WAL directory
      1234567890.wal      # WAL log file
```

### Segment File (.tsb) — BlockFile

Each `.tsb` file is a **BlockFile** containing compressed data blocks plus a block-level index at the end:

```
┌──────────────────────────────────────────────────────┐
│                    BlockFile (.tsb)                    │
├───────────────┬──────────────────────────────────────┤
│ FileHeader    │ Magic, version, tx state, codec, etc. │
├───────────────┼──────────────────────────────────────┤
│ Physical      │ ┌ LenPrefix ───────────────────────┐ │
│ Block 0       │ │ CompressedPayload                │ │
│               │ │  = N logical blocks concatenated  │ │
│               │ │  then compressed together         │ │
├───────────────┤ └──────────────────────────────────┘ │
│ Physical      │ ┌ LenPrefix ───────────────────────┐ │
│ Block 1       │ │ CompressedPayload                │ │
│               │ │  ...                              │ │
├───────────────┤ └──────────────────────────────────┘ │
│     ...       │                                       │
├───────────────┼──────────────────────────────────────┤
│ Block Index   │ Count + IndexEntry × M               │
│               │  · RawOff  → logical offset           │
│               │  · CompOff → physical offset          │
│               │  · CompLen → compressed size          │
│               │  · Crc32   → checksum                 │
└───────────────┴──────────────────────────────────────┘
```

The **block index** sits at the end of the file and maps each physical block's position to its decompressed logical offset, enabling direct seek to any block.

### Segment Index File (.tsb.idx)

Sits alongside each `.tsb` segment for time-range + tag filtering without reading segment data:

```
┌──────────────────────────────────────────────────┐
│              Segment Index (.tsb.idx)              │
├──────────────┬───────────────────────────────────┤
│ Header       │ Magic, BlockCount, MinTime, MaxTime│
├──────────────┼───────────────────────────────────┤
│ Entry [0]    │ TagCode, MinTime, MaxTime,         │
│              │ Offset, DataSize                   │
├──────────────┼───────────────────────────────────┤
│ Entry [1]    │ ...                                │
├──────────────┼───────────────────────────────────┤
│     ...      │  (one entry per logical block)     │
└──────────────┴───────────────────────────────────┘
```

Each entry records which tag column a logical block belongs to, its time range, and where to find it in the `.tsb` file. Queries binary-search the index to skip irrelevant blocks.

### Logical Block

The basic unit written by column encoders. Multiple logical blocks are concatenated and compressed into one physical block:

```
┌──────────────────────────────────────────────────────────┐
│                    One Logical Block                       │
├────────────────┬─────────────────────────────────────────┤
│ SegmentHeader  │          Payload (compressed data)       │
│                │  ┌─────────────────┬──────────────────┐ │
│  · TagCode     │  │ CompressedValue │ CompressedTime   │ │
│  · MinTime     │  │ (type-specific) │ (delta-of-delta) │ │
│  · MaxTime     │  └─────────────────┴──────────────────┘ │
│  · DataSize    │                                          │
│  · CRC32       │                                          │
└────────────────┴──────────────────────────────────────────┘
```

### Payload Formats

**Scalar types** (Int, Float, String, Bool, Json):

```
┌────────────────────┬─────────────────────┬──────────────────┐
│ ValueByteLength    │ CompressedValueData │ CompressedTime   │
│  (length prefix)   │ (type encoder)      │ (TimeEncoder)    │
└────────────────────┴─────────────────────┴──────────────────┘
```

**Structure types** (fixed / adaptive structs):

```
┌────────┬──────────┬──────────┬─────┬──────────────────┬──────────────────┐
│ Marker │ Field0Len│ Field1Len│ ... │ FieldData Concat │ CompressedTime   │
│ (1B)   │  (each 8B)          │     │ (per-field enc.) │ (TimeEncoder)    │
└────────┴──────────┴──────────┴─────┴──────────────────┴──────────────────┘
```

Each field within a struct is independently encoded using its own type-specific compressor, then concatenated. The marker identifies whether the schema is fixed (pre-registered) or adaptive (auto-discovered from Maps).

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/mababaNiubi/variant` | Dynamic type system for value representations |
| `github.com/golang/snappy` | Snappy compression for strings and block files |
| `github.com/jwilder/encoding/simple8b` | Simple-8b packing for integers and timestamps |
| `github.com/dgryski/go-bitstream` | Bit-level I/O for float compression |
| `github.com/pierrec/lz4/v4` | LZ4 compression for JSON and block files |
| `github.com/klauspost/compress/zstd` | Zstd compression for block files |
