# TSDB Server

基于 TCP 和 HTTP 的时序数据库服务端，支持通过 SQL 语句或 REST API 操作 TSDB 数据库，同时提供交互式 CLI 模式。

## 启动服务

### 默认启动（TCP + HTTP 双协议）

```bash
go run ./cmd/server/
```

默认监听端口：
- TCP SQL 服务：`:8811`
- HTTP API 服务：`:8822`

### 通过 JSON 配置文件

```bash
go run ./cmd/server/ -config ./config.json
```

配置文件格式：

```json
{
  "tcp_addr": ":8811",
  "http_addr": ":8822",
  "db_config": {
    "Path": "./data",
    "MaxSegmentSize": 67108864,
    "SecondaryCompressionName": "zstd"
  }
}
```

| 字段 | 类型 | 默认值 | 说明 |
|---|---|---|---|
| `tcp_addr` | `string` | `":8811"` | TCP SQL 服务监听地址，设为空字符串则不启动 |
| `http_addr` | `string` | `":8822"` | HTTP API 服务监听地址，设为空字符串则不启动 |
| `db_config` | `object` | — | TSDB 数据库配置，字段同 [tsdb.Config](../README.md#configuration) |

### CLI 交互模式

```bash
go run ./cmd/server/ -cli
```

进入交互式命令行，支持输入 SQL 语句，输入 `help` 查看帮助，`quit` 或 `exit` 退出。

## TCP SQL 协议

使用 telnet / netcat / 自定义 TCP 客户端连接，一行 SQL 对应一行 JSON 响应：

```bash
echo "CREATE TABLE metrics TYPE float PRECISION 2" | nc localhost 8811
echo "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999" | nc localhost 8811
```

## HTTP API

所有接口使用 POST 方法，请求和响应均为 JSON。

### 健康检查

```
GET /health
```

```json
{"success": true, "message": "ok"}
```

### 创建表

```
POST /create-table
```

请求：

```json
{
  "name": "temperature",
  "type": "float",
  "precision": 3
}
```

`type` 可选值：`int`、`float`、`string`、`bool`、`json`、`structure`、`unknown`

响应：

```json
{"success": true, "message": "table temperature created"}
```

### 批量写入

```
POST /write/batch
```

请求：

```json
{
  "table": "temperature",
  "rows": [
    {"tag": "sensor_01", "timestamp": 1700000000000, "value": 25.6},
    {"tag": "sensor_01", "timestamp": 1700000001000, "value": 26.1},
    {"tag": "sensor_02", "timestamp": 1700000000000, "value": 31.2}
  ]
}
```

支持 gzip 压缩（设置 `Content-Encoding: gzip`）。

响应：

```json
{"success": true, "message": "inserted 3"}
```

### 范围查询

```
POST /query
```

请求：

```json
{
  "table": "temperature",
  "tag": "sensor_01",
  "startTime": 1700000000000,
  "endTime": 1700003600000,
  "limit": 100,
  "polymerization": "avg",
  "condition": {
    "column": "value",
    "operator": ">",
    "value": 30
  }
}
```

| 字段 | 类型 | 必填 | 说明 |
|---|---|---|---|
| `table` | `string` | ✓ | 表名 |
| `tag` | `string` | ✓ | 测点标签 |
| `startTime` | `int64` | ✓ | 起始时间戳（纳秒） |
| `endTime` | `int64` | ✓ | 结束时间戳（纳秒） |
| `limit` | `int64` | | 最大返回点数，默认 10000 |
| `polymerization` | `string` | | 聚合方式：`avg` / `min` / `max` |
| `condition` | `object` | | 值过滤条件，`operator` 支持 `=` `!=` `>` `>=` `< ` `<=` |

响应：

```json
{
  "success": true,
  "data": [
    {"time": 1700000000000, "value": 25.6},
    {"time": 1700000001000, "value": 26.1}
  ],
  "count": 2
}
```

## SQL 语法

### CREATE TABLE — 创建表

```sql
CREATE TABLE table_name TYPE float|int|integer|string|bool|json [PRECISION n]
```

| 类型 | 说明 | 支持 PRECISION |
|---|---|---|
| `float` | 浮点数 | 是 |
| `int` / `integer` | 整数 | 否 |
| `string` | 字符串 | 否 |
| `bool` | 布尔值 | 否 |
| `json` | JSON 对象 | 否 |

示例：

```sql
CREATE TABLE temperature TYPE float PRECISION 3
CREATE TABLE counts TYPE int
CREATE TABLE events TYPE string
CREATE TABLE status TYPE bool
```

### INSERT — 插入数据

```sql
-- 单行插入
INSERT INTO table_name (tag, time, value) VALUES ('tag_name', timestamp, value)

-- 多行插入（逗号分隔）
INSERT INTO table_name (tag, time, value) VALUES
  ('tag_1', 1000, 25.6),
  ('tag_1', 2000, 26.1),
  ('tag_2', 1000, 31.2)
```

- `tag` — 测点标签，字符串
- `time` — Unix 时间戳（纳秒），整数
- `value` — 值，支持数字、字符串、布尔值（`true` / `false`）

示例：

```sql
INSERT INTO temperature (tag, time, value) VALUES ('sensor_01', 1700000000000, 25.6)
INSERT INTO counts (tag, time, value) VALUES ('counter', 1700000000000, 42)
INSERT INTO events (tag, time, value) VALUES ('device_1', 1700000000000, 'startup')
INSERT INTO status (tag, time, value) VALUES ('switch_1', 1700000000000, true)
```

### SELECT — 范围查询

```sql
SELECT * FROM table_name
  WHERE tag = 'tag_name'
  AND time >= start_timestamp
  AND time <= end_timestamp
  [LIMIT max_points]
  [POLYMERIZATION avg|min|max]
  [HAVING column operator value]
```

| 子句 | 说明 |
|---|---|
| `LIMIT n` | 限制返回数据点数（默认 10000） |
| `POLYMERIZATION avg\|min\|max` | 聚合方式：平均值 / 最小值 / 最大值 |
| `HAVING col op val` | 按值过滤，`op` 支持 `=` `!=` `>` `>=` `<` `<=` |

示例：

```sql
-- 查询所有数据
SELECT * FROM temperature
  WHERE tag = 'sensor_01'
  AND time >= 1700000000000
  AND time <= 1700003600000

-- 限制返回 100 个点
SELECT * FROM temperature
  WHERE tag = 'sensor_01'
  AND time >= 1700000000000
  AND time <= 1700003600000
  LIMIT 100

-- 按小时聚合最大值
SELECT * FROM temperature
  WHERE tag = 'sensor_01'
  AND time >= 1700000000000
  AND time <= 1700003600000
  LIMIT 24
  POLYMERIZATION max

-- 过滤低于阈值的点
SELECT * FROM temperature
  WHERE tag = 'sensor_01'
  AND time >= 1700000000000
  AND time <= 1700003600000
  HAVING value > 30
```

### SELECT LATEST — 最新值查询

```sql
SELECT LATEST table_name 'tag_name'
```

示例：

```sql
SELECT LATEST temperature 'sensor_01'
```

## 响应格式

所有响应均为单行 JSON：

```json
// 成功
{"success": true, "message": "inserted 1"}

// 查询成功
{"success": true, "data": [{"time": 1000, "value": 42.5}, {"time": 2000, "value": 43.1}], "count": 2}

// 错误
{"success": false, "message": "table not exists"}
```

## 程序化调用示例

### Go TCP 客户端

```go
conn, _ := net.Dial("tcp", "localhost:8811")
defer conn.Close()

// 发送 SQL
fmt.Fprintln(conn, "CREATE TABLE test TYPE float")

// 读取响应
scanner := bufio.NewScanner(conn)
scanner.Scan()
fmt.Println(scanner.Text())
// {"success": true, "message": "table test created"}
```

### Go HTTP 客户端

```go
// 批量写入
body, _ := json.Marshal(map[string]interface{}{
    "table": "test",
    "rows": []map[string]interface{}{
        {"tag": "cpu", "timestamp": 1000, "value": 42.5},
        {"tag": "cpu", "timestamp": 2000, "value": 43.1},
    },
})
resp, _ := http.Post("http://localhost:8822/write/batch", "application/json",
    bytes.NewReader(body))
defer resp.Body.Close()

// 范围查询
q, _ := json.Marshal(map[string]interface{}{
    "table": "test", "tag": "cpu",
    "startTime": 0, "endTime": 9999999,
    "limit": 100,
})
resp, _ = http.Post("http://localhost:8822/query", "application/json",
    bytes.NewReader(q))
```

## 完整测试流程

```bash
# 1. 启动服务
go run ./cmd/server/ &
# 输出: TSDB SQL server listening on [::]:8811
# 输出: HTTP server listening on [::]:8822

# 2. 创建表
echo "CREATE TABLE t1 TYPE float PRECISION 2" | nc localhost 8811

# 3. 写入数据
echo "INSERT INTO t1 (tag, time, value) VALUES ('cpu', 1000, 42.5)" | nc localhost 8811
echo "INSERT INTO t1 (tag, time, value) VALUES ('cpu', 2000, 43.1)" | nc localhost 8811

# 4. 查询
echo "SELECT * FROM t1 WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999" | nc localhost 8811

# 5. HTTP 方式写入
curl -X POST http://localhost:8822/write/batch \
  -H "Content-Type: application/json" \
  -d '{"table":"t1","rows":[{"tag":"cpu","timestamp":3000,"value":44.0}]}'

# 6. HTTP 方式查询
curl -X POST http://localhost:8822/query \
  -H "Content-Type: application/json" \
  -d '{"table":"t1","tag":"cpu","startTime":0,"endTime":9999999,"limit":10}'
```

## 运行测试

```bash
# 单元测试（解析器、格式化）
go test ./server/ -v -run "TestParse|TestLexer|TestFormat"

# 集成测试（完整服务端流程）
go test ./server/ -v -run "TestServer"

# 全部测试
go test ./server/ -v
```
