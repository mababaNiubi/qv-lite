<p align="center">
  <img src="./logo.png" alt="qv-lite logo" width="200" />
</p>

<h1 align="center">qv-lite</h1>

<p align="center">
  <strong>高性能嵌入式时序数据库引擎，使用 Go 语言编写。</strong>
</p>

<p align="center">
  <strong>中文</strong> | <a href="./README.md">English</a>
</p>

## 目录

- [特性](#特性)
- [安装](#安装)
- [快速开始](#快速开始)
- [配置项](#配置项)
- [压缩算法](#压缩算法)
- [数据编码](#数据编码)
- [依赖](#依赖)

## 特性

- **嵌入式架构** — 无需外部服务，以库的形式直接集成到 Go 应用中。
- **强类型列存储** — 支持 Int、Float、String、Bool、Json、Structure 等多种类型，每种类型使用专门的压缩算法。
- **WAL + 段存储** — 写操作先缓冲到 WAL，再定期刷入压缩后的磁盘段文件。
- **多表支持** — 管理多个独立表，每个表拥有独立的标签字典和列集合。
- **高效压缩** — 时间戳使用 delta-of-delta + simple8b/RLE；整数使用 zigzag + simple8b；浮点数使用 XOR-delta；布尔值使用位打包；字符串使用 Snappy；JSON 使用 LZ4。
- **降采样查询** — 长时间范围查询支持滑动窗口聚合（avg / min / max）。
- **数据过期** — 可配置的基于时间的数据自动删除。
- **去重与最小间隔** — 可配置的去重窗口和最小写入间隔，防止重复数据。
- **块级索引** — 基于二分搜索的块索引，实现快速时间范围过滤。
- **崩溃恢复** — BlockFile 事务状态标记支持崩溃恢复；段文件支持事务回滚。

## 安装

```bash
go get github.com/mababaNiubi/qv-lite/tsdb
```

## 快速开始

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
    // 打开或创建数据库
    db, err := tsdb.Open(tsdb.Config{
        Path: "./my_tsdb_data",
        Segment: tsdb.SegmentConfig{
            MaxSize:                64 * 1024 * 1024, // 64MB 段大小
            MaxSegmentTimeInterval: 3600,              // 最长时间跨度（秒）
        },
        Wal: tsdb.WalConfig{
            MaxCacheBufferSize: 128 * 1024 * 1024, // 128MB WAL 缓存
            MaxWalFileNumber:   10,
        },
    }, context.Background())
    if err != nil {
        panic(err)
    }
    defer db.Close()

    // 写入数据点
    db.Write("default", "cpu_usage", time.Now().UnixNano(), variant.New(42.5))

    // 查询数据（带降采样）
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

## 配置项

### Config

| 字段 | 类型 | 说明 |
|------|------|------|
| `Path` | `string` | 数据库数据存储路径 |
| `Segment` | `SegmentConfig` | 段文件配置 |
| `Wal` | `WalConfig` | WAL 配置 |
| `DataExpirationDays` | `int32` | 数据过期天数（0 = 不过期） |
| `Dedup` | `bool` | 是否启用窗口内去重 |
| `DedupWindowMs` | `int64` | 去重窗口（毫秒） |
| `MinIntervalMs` | `int64` | 最小写入间隔（毫秒） |
| `SecondaryCompression` | `bool` | 是否启用磁盘数据二次压缩 |

### SegmentConfig

| 字段 | 类型 | 说明 |
|------|------|------|
| `MaxSize` | `int64` | 单个段文件最大大小（字节） |
| `MaxSegmentTimeInterval` | `int64` | 单个段文件最大时间跨度（秒） |

### WalConfig

| 字段 | 类型 | 说明 |
|------|------|------|
| `MaxCacheBufferSize` | `int64` | WAL 内存缓存最大大小（字节） |
| `MaxWalFileNumber` | `int32` | 最大 WAL 文件数量 |
| `CloseBuffer` | `bool` | 是否关闭内存缓冲 |
| `MaxBufferBatchSize` | `int64` | 最大批量写入大小 |

## 压缩算法

| 数据类型 | 策略 |
|----------|------|
| 时间戳 | delta-of-delta + 缩放 + simple8b / RLE |
| 整数 | delta + zigzag + simple8b / RLE |
| 浮点数 | XOR-delta + 尾数截断（基于小数精度） |
| 布尔值 | 位打包（每字节 8 个）或全 True/False RLE |
| 字符串 | uvarint 长度前缀拼接 + Snappy |
| JSON | variant 二进制序列化 + LZ4 |
| 固定结构体 | 按列独立编码，每列使用对应类型的子编码器 |
| 自适应结构体 | 自动发现 Map 中的列，递归嵌套，自描述格式 |

## 数据编码

### 磁盘布局

```
{db_path}/
  table.json              # 表元数据
  default/                # 默认表目录
    meta.json             # 标签字典
    data/                 # 段数据文件
      1234567890.tsb      # 段文件（BlockFile 格式）
      1234567890.tsb.idx  # 段块索引
    wal/                  # WAL 目录
      1234567890.wal      # WAL 日志文件
```

### 段文件 (.tsb) — BlockFile

每个 `.tsb` 文件是一个 **BlockFile**，包含若干压缩数据块，末尾附有块级索引：

```
┌──────────────────────────────────────────────────────┐
│                    BlockFile (.tsb)                    │
├───────────────┬──────────────────────────────────────┤
│ FileHeader    │ Magic、版本、事务状态、压缩算法等       │
├───────────────┼──────────────────────────────────────┤
│ 物理块 0       │ ┌ LenPrefix ───────────────────────┐ │
│               │ │ CompressedPayload                │ │
│               │ │  = 多个逻辑块拼接后一起压缩         │ │
├───────────────┤ └──────────────────────────────────┘ │
│ 物理块 1       │ ┌ LenPrefix ───────────────────────┐ │
│               │ │ CompressedPayload                │ │
│               │ │  ...                              │ │
├───────────────┤ └──────────────────────────────────┘ │
│     ...       │                                       │
├───────────────┼──────────────────────────────────────┤
│ 块索引         │ Count + IndexEntry × M               │
│               │  · RawOff  → 解压后逻辑偏移            │
│               │  · CompOff → 压缩后物理偏移            │
│               │  · CompLen → 压缩后大小               │
│               │  · Crc32   → 校验码                   │
└───────────────┴──────────────────────────────────────┘
```

**块索引**位于文件末尾，记录每个物理块在文件中的物理位置（CompOff）与解压后的逻辑偏移（RawOff）的映射关系，支持按偏移直接定位到任意块。

### 段索引文件 (.tsb.idx)

与 `.tsb` 段文件配套存在，提供时间范围和标签维度的快速过滤，无需读取段内数据：

```
┌──────────────────────────────────────────────────┐
│              段索引 (.tsb.idx)                      │
├──────────────┬───────────────────────────────────┤
│ Header       │ Magic、BlockCount、MinTime、MaxTime │
├──────────────┼───────────────────────────────────┤
│ Entry [0]    │ TagCode、MinTime、MaxTime、         │
│              │ Offset、DataSize                   │
├──────────────┼───────────────────────────────────┤
│ Entry [1]    │ ...                                │
├──────────────┼───────────────────────────────────┤
│     ...      │  （每个逻辑块一条记录）               │
└──────────────┴───────────────────────────────────┘
```

每条记录描述一个逻辑块属于哪个标签列、覆盖的时间范围、以及在 `.tsb` 文件中的位置。查询时通过二分搜索快速定位相关块并跳过无关数据。

### 逻辑块

逻辑块是列编码器写入的基本单元，多个逻辑块拼接后压缩为一个物理块：

```
┌──────────────────────────────────────────────────────────┐
│                     一个逻辑块                              │
├────────────────┬─────────────────────────────────────────┤
│ SegmentHeader  │           Payload（压缩后的数据）          │
│                │  ┌─────────────────┬──────────────────┐ │
│  · TagCode     │  │ CompressedValue │ CompressedTime   │ │
│  · MinTime     │  │ （类型编码器）    │ （delta-of-delta）│ │
│  · MaxTime     │  └─────────────────┴──────────────────┘ │
│  · DataSize    │                                          │
│  · CRC32       │                                          │
└────────────────┴──────────────────────────────────────────┘
```

### 负载格式

根据值类型分为两种布局：

**标量类型**（Int、Float、String、Bool、Json）：

```
┌────────────────────┬─────────────────────┬──────────────────┐
│ ValueByteLength    │ CompressedValueData │ CompressedTime   │
│  （长度前缀）        │ （类型编码器压缩）    │ （TimeEncoder）   │
└────────────────────┴─────────────────────┴──────────────────┘
```

**结构体类型**（固定 / 自适应结构体）：

```
┌────────┬──────────┬──────────┬─────┬──────────────────┬──────────────────┐
│ Marker │ Field0Len│ Field1Len│ ... │ FieldData Concat │ CompressedTime   │
│ (1B)   │  （各 8B）           │     │ （各字段独立压缩）  │ （TimeEncoder）   │
└────────┴──────────┴──────────┴─────┴──────────────────┴──────────────────┘
```

结构体的每个字段使用各自类型对应的压缩器独立编码后拼接。Marker 标记区分固定结构体（预注册列）和自适应结构体（运行时自动发现 Map 中的列）。

## 依赖

| 包 | 用途 |
|---|------|
| `github.com/mababaNiubi/variant` | 动态类型系统，用于值表示 |
| `github.com/golang/snappy` | Snappy 压缩（字符串、块文件） |
| `github.com/jwilder/encoding/simple8b` | 整数/时间戳的 simple8b 编码 |
| `github.com/dgryski/go-bitstream` | 位级读写（浮点数压缩） |
| `github.com/pierrec/lz4/v4` | LZ4 压缩（JSON、块文件） |
| `github.com/klauspost/compress/zstd` | Zstd 压缩（块文件） |
