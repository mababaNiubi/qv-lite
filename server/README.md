# tsdb-server — qv-lite 时序数据库服务端

> **状态：Beta**（API 可能随版本演进调整）
>
> 组件总览见仓库根目录 [README](../README.md)；服务端只是 qv-lite 的一个接入
> 方式，嵌入式引擎能力以 [`tsdb`](../tsdb) 包为准。

`tsdb-server` 把 qv-lite 的嵌入式时序引擎（`tsdb` 包）包装成一个**独立运行的网络服务进程**，
对外提供基于 HTTP/JSON 的时序数据读写能力（含流式查询与 Line Protocol 写入）。
服务端内置了为读写性能优化的默认参数，也允许通过命令行参数或配置文件逐项调优。

## 构建与运行

```bash
# 构建（在仓库根目录执行）
go build -o bin/tsdb-server ./server/cmd

# 默认运行（数据目录 ./qvLite-data，监听 :8686）
./bin/tsdb-server

# 自定义端口与数据目录
./bin/tsdb-server -listen 0.0.0.0:8686 -db-path /var/lib/qv-tsdb

# 使用配置文件
./bin/tsdb-server -config ./server/config.example.json
```

## 配置

命令行 flag 与 JSON 配置文件（`-config`）等价，配置文件优先级更高。

| Flag | 默认 | 说明 |
|---|---|---|
| `-listen` | `:8686` | 监听地址 |
| `-db-path` | `./qvLite-data` | 数据目录 |
| `-async-flush` | `true` | 后台异步落盘（高吞吐写） |
| `-async-cleanup` | `true` | 后台定期清理过期数据 |
| `-max-segment-size` | `67108864` | 段文件大小上限（字节） |
| `-max-segment-interval` | `0` | 段最大时间跨度（秒，0=不限） |
| `-data-expiration` | `0` | 数据保留时长（分钟，0=不过期） |
| `-dedup-window` | `0` | 去重窗口（毫秒，0=关闭） |
| `-min-interval` | `0` | 最小写入间隔（毫秒，0=关闭） |
| `-max-storage-time` | `0` | 允许存储的时间戳与当前的最大差值（秒） |
| `-ingest-shards` | `16` | 引擎攒批分片锁数量（自动向上取整为 2 的幂） |
| `-ingest-batch-size` | `4096` | 触发异步 WAL 批次的点数 |
| `-ingest-flush-ms` | `5` | 引擎活动批次最大等待时间（毫秒） |
| `-ingest-queue-size` | `8` | 等待 WAL worker 的冻结批次数上限 |
| `-write-buffer-ms` | `0` | 可选 server 解码流水线；默认关闭，避免把分片写重新串行化 |
| `-compression` | `zstd` | 块压缩算法：zstd/lz4/snappy/gzip/none |
| `-max-body` | `67108864` | 请求体上限（字节） |
| `-pprof` | `false` | 开启 `/debug/pprof` |
| `-token` | 空 | 要求所有请求携带该 `X-Auth-Token` |

引擎级参数（`wal_config`、`max_file_size` 等）通过配置文件设置，见 `config.example.json`。

## HTTP API（`/api/v1`）

### 健康检查

```
GET /api/v1/health
→ {"status":"ok","uptime":"...","tables":[...],"version":"1"}
```

### 建表

```
POST /api/v1/tables
{
  "name": "sensor",
  "desc": "温度传感器",
  "float_precision": 2,
  "columns": [{"name":"value","desc":"","type":2}]   // 可选：结构化列
}
→ 201 {"created":true,"name":"sensor"}
```

### 表列表

```
GET /api/v1/tables
→ {"tables":[{"name":"sensor","desc":"...","type":0,"float_precision":2}]}
```

### 写单点

```
POST /api/v1/write
{
  "table": "sensor",           // 空 = default 表
  "tag": "cpu",
  "timestamp": 1700000000000,  // 毫秒（与引擎一致，任意时间单位均可）
  "value": 36.5                // 原生 JSON 值，类型自动推断
}
→ {"written":true}
```

### 批量写（高性能路径）

```
POST /api/v1/batch?table=sensor
{
  "points": [
    {"tag":"cpu","timestamp":1700000000000,"value":36.5},
    {"tag":"cpu","timestamp":1700000001000,"value":36.6}
  ]
}
→ {"written":2}
```

**表名来源**：`?table=` 查询参数优先（推荐，避免依赖 JSON 字段顺序）；body 中的
`table` 字段也支持，但需出现在 `points` 之前（Go 的 `map` 序列化键序随机，建议用
查询参数或 struct 保证顺序）。

**流式处理**：所有批量写入通道（二进制 / JSON / Line Protocol）都是流式的——
服务端边接收边解码，攒满约 5 万点立即入库，不等待整个 body 到达，内存峰值恒定
（≈5 万点，与请求总点数量级无关）。单请求可一次上传任意多点（如 200 万点，
实测 ~302 万点/秒，内存不随请求增大而增长）。多表混用（Line Protocol 每行独立
表名）时按表分别累积，达到总量阈值统一入库——连续交替的多表流（A B A B…）
不会退化为逐行小写。

### Line Protocol 写入（跨语言通用）

InfluxDB Line Protocol 兼容子集，任何语言拼字符串即可，无需 JSON 库：

```
POST /api/v1/write/line
Content-Type: text/plain

sensor,tag=cpu value=36.5 1700000000000000000
sensor,tag=cpu count=42i 1700000001000000000
sensor,tag=cpu ok=true 1700000002000000000
sensor,tag=cpu msg="hello" 1700000003000000000
→ {"written":4}
```

- 每行一个点：`<measurement>[,<tag>=<v>...] <field>=<value>[,<field2>=<value2>...] [<timestamp>]`
- measurement → 表名；`tag=<v>` 键 → qv-lite 的 tag 标识（多 tag 时用整个 tag set）
- 单 field → 值直接作为点值；多 field → 打包为结构体
- 类型由字面量推断：`42i` int64、`1.5`/`1e3` float64、`true/false` bool、`"str"` string
- 时间戳缺省用服务器时间；注释行以 `#` 开头

### 查询（流式响应）

```
POST /api/v1/query
{
  "table": "sensor",
  "tag": "cpu",
  "start": 1700000000000,
  "end": 1700000060000,
  "window": 0,          // >0 时按该毫秒窗口降采样
  "aggregation": 0,     // 0=avg 1=min 2=max（window>0 时生效）
  "limit": 0,           // 可选：>0 时最多返回 limit 个点
  "offset": 0,          // 可选：按时间序跳过前 offset 个点（仅与 limit 配合有意义）
  "condition": {        // 可选：单条件或逻辑条件
    "column": "",       // 空 = 对点值本身过滤；结构化数据可填 "a.b"
    "op": ">",
    "value": 36.7       // 原生 JSON 值
  }
}
→ {"points":[{"timestamp":...,"value":36.5}],"count":N}
```

**流式下发**：`window<=0`（原始点）时，响应体由引擎的 `QueryIter` 流式迭代器
逐点增量编码（`{"points":[...],"count":N}`，`count` 在最后补写），超大结果集
不会在内存中物化，响应按块刷新边查边发；`window>0`（窗口聚合）输出天然有界
（≤ 范围/窗口数），同样以流式形状输出。`limit`/`offset` 作用于原始点查询，
对应引擎 `QueryIter` + `QueryOptions`。

逻辑条件（AND/OR）：

```json
{
  "opLogical": "and",
  "conditions": [
    {"column":"a","op":">","value":1},
    {"column":"b","op":"<","value":10}
  ]
}
```

### 查最新点

```
POST /api/v1/query/latest
{"table":"sensor","tag":"cpu"}
→ {"timestamp":1700000000000,"value":36.5}
```

## 值编码（跨语言友好）

**请求侧**：`value` 字段直接用**原生 JSON 值**，类型由 JSON 类型推断：

| JSON 值 | 推断类型 |
|---|---|
| `36.5` / `1e3`（含小数点/指数） | float64 |
| `123`（JSON 整数，\|v\|≤2^53） | int64 |
| `"abc"` | string |
| `true` / `false` | bool |
| `{...}` / `[...]` | json 结构（map / list） |
| `null` | empty |

int64 **超过 2^53** 时 JSON number 会丢精度，改用**可选字段 `valueType`** 显式声明（value 为字符串或数字）：

```json
{"table":"sensor","tag":"c","timestamp":1700000000000,"value":"9007199254740993","valueType":"int"}
{"table":"sensor","tag":"u","timestamp":1700000000000,"value":"18446744073709551615","valueType":"uint"}
```

`valueType` 支持 `int` / `uint` / `float` / `string` / `bool` / `json`；为空时按 JSON 类型推断。条件（condition）的 `value` 同样支持 `valueType`。

**响应侧**：`points[].value` 输出原生 JSON；仅 int/uint 携带 `vtype` 字段，
且超 2^53 时 value 输出为字符串保精度：

```json
{"timestamp":1700000000000,"value":36.5}
{"timestamp":1700000000000,"value":123,"vtype":"int"}
{"timestamp":1700000000000,"value":"9007199254740993","vtype":"int"}
```

跨语言客户端只需处理标准 JSON（数字/字符串/布尔），int64 精度场景读取 `vtype`。

## 写入流水线

默认写入路径由引擎内的分片 MemTable 负责：

- 请求 goroutine 只校验数据、按 tag 哈希到分片，并在分片锁下追加原始 tag 点；
- 达到 `ingest-batch-size` 或 `ingest-flush-ms` 后，活动分片与新空缓冲快速交换；
- 单个有序后台 worker 对冻结批次做 tag→tagCode 映射、按 tag/time 排序、先持久化
  Meta，再把批次写入 WAL；此时新请求继续写下一批，不等待排序和文件 I/O；
- `ingest-queue-size` 和点数水位共同提供背压，避免磁盘变慢时无限占用内存；
- 查询与关闭会建立提交屏障，因此正常的写后读和优雅关闭不会漏掉活动批次。

写接口返回表示数据已被引擎接收，不表示后台 WAL 批次已经完成；进程异常退出时，
尚未达到阈值且未到 `ingest-flush-ms` 的活动批次可能丢失。需要确认可见时执行查询
（查询自带屏障），正常关闭也会完整提交活动与排队批次。

旧的 server 级解码→入库合并器仍可通过 `-write-buffer-ms > 0` 显式开启，适合
需要让超大请求的解码与入库重叠的场景。它默认关闭，因为单后台 goroutine 会把
多个请求重新串行化，并与引擎自身攒批重复。

吞吐基准（批量写 / Line 协议 / 查询 / 并发）见 `server/bench_test.go` 与
`server/client/client_test.go`；E2E 基准见 `tsdb/db_bench_test.go` 的
`BenchmarkE2E_*`。

## Go 客户端

```go
import "github.com/mababaNiubi/qv-lite/server/client"

c, _ := client.New("http://127.0.0.1:8686")

// 批量写 10 万点
pts := make([]client.TagPoint, 0, 100_000)
for i := 0; i < 100_000; i++ {
    pts = append(pts, client.TagPoint{
        Tag:       "cpu",
        Timestamp: base + int64(i),
        Value:     client.Float(v),
    })
}
n, err := c.WriteBatch(ctx, "sensor", pts)

// 查询
points, err := c.Query(ctx, "sensor", "cpu", start, end, 0, 0, nil)

// 降采样
agg, err := c.Query(ctx, "sensor", "cpu", start, end, 1000, 0, nil)
```

完整 API 见 `server/client/client.go`（`Write`/`WriteBatch`/`WriteBatchJSON`/
`WriteLine`/`Query`/`QueryLatest`/`CreateTable`/`ListTables`/`Health`），用法示例
见 `server/client/client_test.go` 与 `server/server_test.go`。

## 性能保障要点

1. **批量写**：单请求打包尽量多的点（建议 1K–10K/批），一次批量只取一次 WAL 锁。
2. **AsyncFlush**：默认开启，写入路径不阻塞在段编码/压缩上。
3. **段大小**：`max_segment_size` 越大，块索引越少，顺序读越快（64MB 起步）。
4. **压缩**：默认 zstd；追求极致写性能可换 `snappy` 或 `none`。
5. **去重/最小间隔**：按业务需要开启，可显著减少落盘数据量。
6. **连接复用**：Go 客户端默认连接池复用 HTTP keep-alive。

