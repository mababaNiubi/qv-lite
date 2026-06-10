# TSDB SQL Server

基于 TCP 的时序数据库 SQL 服务端，支持通过标准 SQL 语句操作 TSDB 数据库。

## 连接方式

使用 telnet / netcat / 自定义 TCP 客户端连接，一行 SQL 对应一行 JSON 响应：

```bash
echo "CREATE TABLE metrics TYPE float PRECISION 2" | nc localhost 9876
echo "SELECT * FROM metrics WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999" | nc localhost 9876
```

## SQL 语法

### CREATE TABLE — 创建表

```sql
CREATE TABLE table_name TYPE float|int|string|bool|json [PRECISION n]
```

| 类型 | 说明 | 支持 PRECISION |
|------|------|:---:|
| `float` | 浮点数 | ✓ |
| `int` / `integer` | 整数 | |
| `string` | 字符串 | |
| `bool` | 布尔值 | |
| `json` | JSON 对象 | |

示例：
```sql
CREATE TABLE temperature TYPE float PRECISION 3
CREATE TABLE counts TYPE int
CREATE TABLE events TYPE string
CREATE TABLE status TYPE bool
```

### INSERT — 插入数据

```sql
INSERT INTO table_name (tag, time, value) VALUES ('tag_name', timestamp, value)
```

- `tag` — 测点标签，字符串
- `time` — Unix 时间戳（毫秒），整数
- `value` — 值，支持数字、字符串、布尔

示例：
```sql
INSERT INTO temperature (tag, time, value) VALUES ('sensor_01', 1700000000000, 25.6)
INSERT INTO counts (tag, time, value) VALUES ('counter', 1700000000000, 42)
INSERT INTO events (tag, time, value) VALUES ('device_1', 1700000000000, 'startup')
INSERT INTO status (tag, time, value) VALUES ('switch_1', 1700000000000, true)
```

### SELECT — 查询数据

```sql
SELECT * FROM table_name
  WHERE tag = 'tag_name'
  AND time >= start_timestamp
  AND time <= end_timestamp
  [LIMIT max_points]
  [POLYMERIZATION avg|min|max]
  [HAVING value operator condition_value]
```

| 子句 | 说明 |
|------|------|
| `LIMIT n` | 限制返回数据点数（默认 10000） |
| `POLYMERIZATION avg\|min\|max` | 聚合方式：平均值/最小值/最大值 |
| `HAVING value op val` | 按值过滤，`op` 支持 `=` `!=` `>` `>=` `<` `<=` |

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

## 响应格式

```json
// 成功
{"success":true,"message":"inserted"}

// 查询成功
{"success":true,"data":[{"time":1000,"value":42.5},{"time":2000,"value":43.1}],"count":2}

// 错误
{"success":false,"message":"table not exists"}
```

## 程序化调用示例

### Go 客户端

```go
conn, _ := net.Dial("tcp", "localhost:9876")
defer conn.Close()

// 发送 SQL
fmt.Fprintln(conn, "CREATE TABLE test TYPE float")

// 读取响应
scanner := bufio.NewScanner(conn)
scanner.Scan()
fmt.Println(scanner.Text())
// {"success":true,"message":"table test created"}
```

### 完整测试流程

```bash
# 1. 启动服务
go run ./cmd/server/ -data ./data &

# 2. 创建表
echo "CREATE TABLE t1 TYPE float PRECISION 2" | nc localhost 9876

# 3. 写入数据
echo "INSERT INTO t1 (tag, time, value) VALUES ('cpu', 1000, 42.5)" | nc localhost 9876
echo "INSERT INTO t1 (tag, time, value) VALUES ('cpu', 2000, 43.1)" | nc localhost 9876

# 4. 查询
echo "SELECT * FROM t1 WHERE tag = 'cpu' AND time >= 0 AND time <= 9999999" | nc localhost 9876
```

## 运行测试

```bash
# 单元测试（解析器、格式化）
go test ./tsdb/server/ -v -run "TestParse|TestLexer|TestFormat"

# 集成测试（完整服务端流程）
go test ./tsdb/server/ -v -run "TestServer"

# 全部测试
go test ./tsdb/server/ -v
```
