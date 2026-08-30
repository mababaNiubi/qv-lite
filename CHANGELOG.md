# Changelog

## v1.3.0 (2026-08-31)

本次发布合并 `feature/tsdb-server-query` 分支，共 28 个提交，新增 server 端
（Beta）与流式查询能力，修复查询读路径正确性问题，并大幅优化查询/写入性能
（server Line 协议写入提速约 2.5 倍）。

### 新功能

- **新增 server 端（Beta）**：全新 HTTP 服务与 Go 客户端库（详见 `server/README.md`）
  - HTTP API：`/api/v1/health`、`/api/v1/tables`（建表/列表）、`/api/v1/write`
    （JSON 单点）、`/api/v1/write/line`（Line 协议）、`/api/v1/batch`（JSON/
    二进制批量）、`/api/v1/query`（原始/窗口聚合）、`/api/v1/query/latest`、
    `/debug/pprof/*`（性能剖析）
  - 流式入库 `StreamIngestor`：边解码边分批（50K 点/批）写入引擎，内存峰值
    恒定，不随请求体量增长；Line/JSON/二进制三种协议共用同一管线
  - Go 客户端库（`server/client`）与命令行入口（`server/cmd`）、配置文件示例
- **流式查询**：引擎层新增 `QueryIter`/`PointIter` 拉取式流接口——disk→WAL
  两相流逐点输出，不物化全量结果，支持条件过滤、offset/limit、ctx 取消与
  查询超时；server 端 `POST /api/v1/query` 以增量编码的 JSON 流
  （`{"points":[...],"count":N}`）下发结果，超大结果集内存恒定
- 查询超时（`QueryTimeout`）与结果点数上限（`MaxQueryPoints`）：
  物化与流式查询路径均支持，每 4096 点批量检查 ctx，超时响应延迟有界
- server 写入管线优化：批通道流式化（恒定内存）、缓存与预处理降低内存占用与
  GC 压力、tagCode 缓存命中率提升、列 map 改数组

### 性能

- **server Line 协议写入提速约 2.5 倍**：`ReadSlice` 行拆分 + 预分配批缓冲 +
  分隔符位掩码；解析状态机内联（parseSeries/parseFields/值扫描合并 + 定长整数
  快速路径）；整数 field 值快速路径（|v|≤2^53 直转 float64）；`spanEq` 用
  memequal 替代手写循环
- 查询优化：解压缓冲复用、零拷贝读块、时间窗整块跳过、分片 chunk 池；窗口
  avg 快路径实测比泛型路径快 52-71%；高基数写入内存降低
- E2E 基准增加流式进度输出，长跑（如 tags10000 读阶段约 9 分钟）实时可见

### 修复

- **CloseBuffer 查询尾部丢点**（重要）：流式迭代器复用了首个 WAL 文件句柄，
  快照中其余文件被静默跳过，AsyncFlush 下首查丢失连续尾部数据；已修复并新增
  回归测试，E2E TagScale 4 配置 5000 万点全部对账通过
- `QueryWindow` 以 ts=0 起始时首窗口丢失（`lastTms==0` 哨兵误判）；补非数值
  采样与 ts=0 测试
- 分片 chunk 池轮转错位致小结果查询每轮重建块；分片池懒初始化 data race；
  NoCompressor 解压缓冲别名重叠
- readerCache 同路径覆盖句柄泄漏；Rebind 失败路径防坏块文件入缓存
- segment 时间切分判断（0=不限，统一纳秒并处理溢出）；窗口查询跳过首个 WAL
  缓存点；WAL `NeedFlush`、配置更新、读写关闭状态纳入互斥保护

### 重构

- `point_iter.go` 重构：四层嵌套拆平、慢路径去重、全中文注释，行为不变；
  热路径内联决策均经微基准验证（直调 0.55ns/点 vs 泛型分发 10.8ns/点等）
