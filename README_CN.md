<p align="center">
  <img src="./logo.png" alt="qv-lite logo" width="200" />
</p>

<h1 align="center">qv-lite</h1>

<p align="center">
  <strong>嵌入式 KV 时序数据库引擎 — 适用于边缘计算网关等性能受限设备部署使用。</strong>
</p>

<p align="center">
  <strong>中文</strong> | <a href="./README.md">English</a>
</p>

## 目录

- [特性](#特性)
- [安装](#安装)
- [使用方法](#使用方法)
  - [打开与关闭](#打开与关闭)
  - [写入](#写入)
  - [查询](#查询)
  - [表管理](#表管理)
  - [数据类型](#数据类型)
  - [聚合](#聚合)
  - [条件过滤](#条件过滤)
- [配置项](#配置项)
- [压缩算法](#压缩算法)
- [数据编码](#数据编码)
- [依赖](#依赖)

## 特性

- **嵌入轻量** — 专为边缘网关、工业控制器、IoT 设备等 CPU/内存/磁盘受限场景设计。
- **代码精简** — 紧凑、可审计的核心代码，极少三方依赖。直观的目录布局：每表一个元数据文件、时间戳命名的段文件与 WAL 日志 — 不依赖重型数据库引擎，便于排查、备份和迁移。
- **高性能写入** — 单线程同步设计。单点写入性能 **800 万+ 点/秒**。
- **自适应类型编码** — 运行时自动识别输入数据的结构，根据数据类型选择合适的压缩编码器，达到最优存储效率。
- **高压缩率** — 紧凑的数据编码加二次块压缩，实现极致的存储效率。
- **多表支持** — 管理多个独立表，每个表拥有独立的 Schema 定义和列集合。
- **降采样查询** — 长时间范围查询支持滑动窗口聚合（avg / min / max）。
- **数据过期** — 可配置的基于时间的数据自动清理。
- **去重与最小间隔** — 可配置的去重窗口和最小写入间隔，防止重复数据。
- **块级索引** — 基于二分搜索的块索引，快速过滤时间范围，无需扫描无关数据。

## 安装

```bash
go get github.com/mababaNiubi/qv-lite/tsdb
```

## 使用方法

### 打开与关闭

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

`Open` 在给定路径创建或打开数据库。`default` 表在首次使用时自动创建。`Close()` 会刷新所有缓冲数据并释放资源。

### 写入

```go
import (
    "time"
    "github.com/mababaNiubi/variant"
)

// 写入 default 表（tableName = "" 或 "default"）
written, err := db.Write("", "sensor_temp", time.Now().UnixNano(), variant.New(25.6))

// 写入指定的表
written, err := db.Write("metrics", "cpu_usage", time.Now().UnixNano(), variant.New(42.5))
```

- `tableName` — 空字符串或 `"default"` 写入自动创建的默认表。
- `tag` — 时序标识（如传感器名称、指标 key）。
- `timestamp` — Unix 时间戳，单位为**纳秒**。
- `value` — 使用 `variant.New(v)` 包装任意支持的 Go 值。
- 返回 `(true, nil)` 表示写入成功；`(false, nil)` 表示因去重或最小间隔规则被跳过。

### 查询

```go
// 范围查询 + 降采样 — 适用于长时间范围
points, err := db.Query("default", "sensor_temp",
    time.Now().Add(-1*time.Hour).UnixNano(), // startTime (ns)
    time.Now().UnixNano(),                   // endTime (ns)
    1000,                                    // maxNumber 返回最大点数
    tsdb.AvgFusion,                          // 聚合模式
    nil,                                     // 条件过滤（nil = 无过滤）
)

// 获取全部原始数据（不降采样）
points, err := db.QueryAll("default", "sensor_temp",
    time.Now().Add(-30*time.Minute).UnixNano(),
    time.Now().UnixNano(),
    nil,
)

// 获取 tag 的最新值
point, err := db.QueryLatest("default", "sensor_temp")
if point != nil {
    fmt.Printf("latest: time=%d, value=%v\n", point.Tms, point.V)
}
```

| 方法 | 说明 |
|------|------|
| `Query(tableName, tag, startTime, endTime, maxNumber, polymerization, cond)` | 范围查询。时间跨度 > 1 小时时返回最多 `maxNumber` 个降采样点；≤ 1 小时时直接返回全部原始数据。 |
| `QueryAll(tableName, tag, startTime, endTime, cond)` | 返回范围内的全部原始数据点，不做数量限制。 |
| `QueryLatest(tableName, tag)` | 返回指定 tag 的最新一条数据。 |

所有时间戳单位为**纳秒**（UnixNano）。`maxNumber` 设为 0 时默认 10000。

### 表管理

```go
// 创建简化指标表，拥有最高的写入性能
err := db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "device",
        Desc: "attributes",
        Type: tsdb.ColumnTypeFloat,
        FloatPrecision: 2,
    },
})


// 创建多列表
err = db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "metrics",
        Desc: "Dev01.CPU",
        Type: tsdb.ColumnTypeStructure,
        Structure: []tsdb.ColumnAttribute{
            {Name: "value",   Type: tsdb.ColumnTypeFloat, FloatPrecision: 2},
            {Name: "quality", Type: tsdb.ColumnTypeInt},
            {Name: "status",  Type: tsdb.ColumnTypeString},
        },
    },
})

// 创建自适应 Schema 的表（运行时自动发现字段，比固定结构慢）
err = db.CreateTable(tsdb.TableInfo{
    ColumnAttribute: tsdb.ColumnAttribute{
        Name: "events",
        Desc: "动态事件日志",
        Type: tsdb.ColumnTypeUnknown, // 自适应 — 运行时发现字段格式
    },
})
```

### 数据类型

| 常量 | 类型 | 说明 |
|------|------|------|
| `ColumnTypeUnknown` (0) | 自适应 | 运行时自动检测嵌套结构 |
| `ColumnTypeInt` (1) | 整数 | 有符号 64 位整数 |
| `ColumnTypeFloat` (2) | 浮点数 | 64 位浮点数，可配置小数精度 |
| `ColumnTypeString` (3) | 字符串 | UTF-8 字符串 |
| `ColumnTypeBool` (4) | 布尔值 | true / false |
| `ColumnTypeJson` (5) | JSON | 任意嵌套 variant |
| `ColumnTypeStructure` (6) | 固定结构体 | 预定义列 Schema |

### 聚合

| 常量 | 值 | 说明 |
|------|-----|------|
| `AvgFusion` | 0 | 滑动窗口取平均值 |
| `MinFusion` | 1 | 滑动窗口取最小值 |
| `MaxFusion` | 2 | 滑动窗口取最大值 |

### 条件过滤

查询方法的 `cond` 参数支持按值过滤数据点：

```go
// 等值过滤 — 仅返回 value == "ok" 的点
cond := tsdb.Condition{
    Operator: tsdb.OpEqual,
    Value:    variant.New("ok"),
}
points, err := db.QueryAll("default", "status", startTs, endTs, cond)

// 逻辑与 — 组合多个条件
logicalCond := tsdb.LogicalCondition{
    Op:   tsdb.LogicalAnd,
    Cond: []any{
        tsdb.Condition{Operator: tsdb.OpGreaterThan, Value: variant.New(80)},
        tsdb.Condition{Operator: tsdb.OpLessThan,    Value: variant.New(100)},
    },
}
points, err := db.QueryAll("default", "cpu", startTs, endTs, logicalCond)
```

## 配置项

### Config

| 字段 | 类型 | 默认值 | 说明                                                  |
|------|------|--------|-----------------------------------------------------|
| `Path` | `string` | `"./qvLite-data"` | 数据库数据存储路径                                           |
| `WalConfig` | `WalConfig` | — | WAL 配置（见下）                                          |
| `MaxSegmentSize` | `int64` | `67108864` (64MB) | 单个段文件最大大小（字节）                                       |
| `MaxSegmentTimeInterval` | `int64` | `0`（不限制） | 单个段文件最大时间跨度（秒）                                      |
| `MaxStorageTime` | `int64` | `3600`（1 小时） | 拒绝时间戳远超当前时间的数据写入                                      |
| `ExpirationMinuteTime` | `int64` | `0`（禁用） | 数据过期时间（分钟），每次写入时自动清理超出时间的数据 |
| `DedupWindowMs` | `int64` | `0`（禁用） | 去重窗口（毫秒），同一 tag 相同值在此窗口内重复写入时跳过                            |
| `MinIntervalMs` | `int64` | `0`（禁用） | 最小写入间隔（毫秒），两次写入间隔小于该值时跳过                               |
| `SecondaryCompressionName` | `string` | `"zstd"` | 块压缩算法：`"zstd"`、`"lz4"`、`"snappy"`、`"gzip"`、`"none"` |

### WalConfig

| 字段 | 类型 | 默认值 | 说明 |
|------|------|--------|------|
| `MaxCacheSize` | `int64` | `67108864` (64MB) | WAL 内存缓存最大大小（字节） |
| `MaxFileNumber` | `int` | — | 最大 WAL 文件数量 |
| `CloseBuffer` | `bool` | `false` | 是否关闭内存 WAL 缓冲 |
| `MaxBufferBatchSize` | `int` | `10000` | 排序刷盘前的最大缓冲条目数 |

**`CloseBuffer` 行为说明：**

| 场景 | `CloseBuffer = true` | `CloseBuffer = false` |
|------|---------------------|----------------------|
| 乱序写入 | 同 key 下禁止乱序时间写入 | `MaxBufferBatchSize` 批处理范围内允许乱序写入 |
| 写入/查询性能 | 较低 | 较高 |
| 异常中断 | 数据安全 | 可能丢失部分缓冲数据 |
| 内存占用 | 通常在 `MaxCacheSize` 范围内 | 约 `MaxCacheSize` 的 3~4 倍 |

> 可通过限制 `MaxCacheSize` 大小来控制数据库内存使用。

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

**块索引**位于文件末尾，记录每个物理块的物理位置与解压后逻辑偏移的映射关系，支持按偏移直接定位到任意块。

### 段索引文件 (.tsb.idx)

与 `.tsb` 段文件配套存在，用于时间范围和标签维度的快速过滤：

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

列编码器写入的基本单元，多个逻辑块拼接后压缩为一个物理块：

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
