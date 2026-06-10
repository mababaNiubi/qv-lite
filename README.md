<p align="center">
  <img src="./logo.png" alt="qv-lite logo" width="200" />
</p>

<h1 align="center">qv-lite</h1>

<p align="center">
  <strong>A high-performance embedded time-series database engine written in Go.</strong>
</p>

<p align="center">
  <a href="./README_CN.md">中文</a> | <strong>English</strong>
</p>

## Table of Contents

- [Features](#features)
- [Installation](#installation)
- [Quick Start](#quick-start)
- [Configuration](#configuration)
- [Compression Algorithms](#compression-algorithms)
- [Data Encoding](#data-encoding)
- [Dependencies](#dependencies)

## Features

- **Embedded Architecture** — No external services required. Integrates as a library into Go applications.
- **Strongly-Typed Column Store** — Supports Int, Float, String, Bool, Json, Structure, and more, each with specialized per-type compression.
- **WAL + Segment Storage** — Writes are buffered in a WAL then periodically flushed to compressed on-disk segments.
- **Multi-Table Support** — Manage multiple independent tables, each with its own tag dictionary and column set.
- **Efficient Compression** — Timestamps use delta-of-delta + simple8b/RLE; integers use zigzag + simple8b; floats use XOR-delta; booleans use bit-packing; strings use Snappy; JSON uses LZ4.
- **Downsampling Queries** — Sliding-window aggregation (avg / min / max) for long time-range queries.
- **Data Expiration** — Configurable time-based automatic data removal.
- **Dedup & Min Interval** — Configurable deduplication window and minimum write interval to prevent duplicate data.
- **Block-Level Index** — Binary-search block index for fast time-range filtering.
- **Crash Recovery** — Transaction state flags for BlockFile recovery; transaction rollback for segments.

## Installation

```bash
go get github.com/mababaNiubi/qv-lite/tsdb
```

## Quick Start

```go
package main

import (
    "context"
    "fmt"
    "time"

    "github.com/mababaNiubi/qv-lite/tsdb"
    "github.com/mababaNiubi/variant"
)

func main() {
    // Open or create a database
    db, err := tsdb.Open(tsdb.Config{
        Path: "./my_tsdb_data",
        Segment: tsdb.SegmentConfig{
            MaxSize:                64 * 1024 * 1024, // 64MB segment size
            MaxSegmentTimeInterval: 3600,              // max time span in seconds
        },
        Wal: tsdb.WalConfig{
            MaxCacheBufferSize: 128 * 1024 * 1024, // 128MB WAL cache
            MaxWalFileNumber:   10,
        },
    }, context.Background())
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // Write a data point
    db.Write("default", "cpu_usage", time.Now().UnixNano(), variant.New(42.5))

    // Query data with downsampling
    points, err := db.Query("default", "cpu_usage",
        time.Now().Add(-1*time.Hour).UnixNano(),
        time.Now().UnixNano(),
        0, tsdb.AvgFusion, nil)
    if err != nil {
        panic(err)
    }
    for _, p := range points {
        fmt.Printf("time: %d, value: %v\n", p.Tms, p.V)
    }
}
```

## Configuration

### Config

| Field | Type | Description |
|-------|------|-------------|
| `Path` | `string` | Database data storage path |
| `Segment` | `SegmentConfig` | Segment file settings |
| `Wal` | `WalConfig` | WAL settings |
| `DataExpirationDays` | `int32` | Data expiration in days (0 = no expiration) |
| `Dedup` | `bool` | Enable in-window deduplication |
| `DedupWindowMs` | `int64` | Dedup window in milliseconds |
| `MinIntervalMs` | `int64` | Minimum write interval in milliseconds |
| `SecondaryCompression` | `bool` | Enable secondary compression for on-disk data |

### SegmentConfig

| Field | Type | Description |
|-------|------|-------------|
| `MaxSize` | `int64` | Maximum segment file size in bytes |
| `MaxSegmentTimeInterval` | `int64` | Maximum segment time span in seconds |

### WalConfig

| Field | Type | Description |
|-------|------|-------------|
| `MaxCacheBufferSize` | `int64` | Maximum WAL in-memory cache size in bytes |
| `MaxWalFileNumber` | `int32` | Maximum number of WAL files |
| `CloseBuffer` | `bool` | Disable in-memory buffering |
| `MaxBufferBatchSize` | `int64` | Maximum batch write size |

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

**Block index** sits at the end of the file and maps each physical block's position (CompOff) to its decompressed logical offset (RawOff), enabling direct seek to any block.

### Segment Index File (.tsb.idx)

Sits alongside each `.tsb` segment and provides time-range + tag filtering without reading the segment data:

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

Each entry records which tag column a logical block belongs to, its time range, and where to find it in the `.tsb` file. Queries use this to binary-search for relevant blocks and skip the rest.

### Logical Block

A logical block is the unit of data written by column encoders. Multiple logical blocks are concatenated and compressed into one physical block:

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

Two layout variants depending on the value type:

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

Each field within a struct is independently encoded using its own type-specific compressor, then concatenated. The marker byte identifies whether the schema is fixed (pre-registered columns) or adaptive (auto-discovered from Maps).

## Dependencies

| Package | Purpose |
|---------|---------|
| `github.com/mababaNiubi/variant` | Dynamic type system for value representations |
| `github.com/golang/snappy` | Snappy compression for strings and block files |
| `github.com/jwilder/encoding/simple8b` | Simple-8b packing for integers and timestamps |
| `github.com/dgryski/go-bitstream` | Bit-level I/O for float compression |
| `github.com/pierrec/lz4/v4` | LZ4 compression for JSON and block files |
| `github.com/klauspost/compress/zstd` | Zstd compression for block files |
