# benchmark — qv-lite TSDB 基准测试框架

用于**改动前后对比**写/读吞吐、压缩率、内存的成熟测试工具。全部走 `tsdb` 公开 API，**新旧实现通吃**，同一份代码即可度量当前引擎和任何重写版本。

```
benchmark/
  scenario.go        场景定义 + 确定性数据生成（seed 固定，新旧运行点序列完全一致）
  harness.go         RunScenario：写/读测量、内存采样、磁盘压缩率统计、正确性自检
  compare.go         新旧 Report diff，PASS/FAIL 判定
  profiler.go        net/http/pprof 集成 + CPU/heap/goroutine 定点采集 + 内存采样器
  suites.go          QuickSuite（快冒烟） / DefaultSuite（完整对比矩阵）
  *_test.go          go test 直跑入口
  cmd/bench/main.go  命令行：run / compare / pprof
```

## 快速开始

```bash
# 冒烟（快，几秒）
go test ./doc/benchmark/ -run TestQuickSuite -v

# 完整矩阵（BENCH_FULL=1 才跑，慢）
BENCH_FULL=1 go test ./doc/benchmark/ -run TestBaselineSuite -v

# 标准 benchmark（配合 -benchmem / -cpuprofile）
go test ./doc/benchmark/ -bench BenchmarkQuickSuite -benchtime=1x -benchmem -cpuprofile cpu.out -memprofile mem.out
```

## 命令行对比流程

```bash
# 1) 改动前采集 baseline（默认 -repeat 3 取中位数，磁盘繁忙轮次自动剔除；权威对比用 -repeat 5）
go run ./doc/cmd/bench run -label baseline -out results -version head -repeat 3

# 2) 改动后同一台机器重跑
go run ./doc/cmd/bench run -label v2-m1 -out results -version m1 -repeat 3 -pprof :6060

# 3) 对比（默认允许 20% 回归；-write 0 表示写不允许回归）
go run ./doc/cmd/bench compare -write 0 -ratio 0 results/baseline_all.json results/v2-m1_all.json

# 只跑部分场景
go run ./doc/cmd/bench run -scenarios small,medium -out results
```

**磁盘停顿处理**：磁盘繁忙会污染单次结果。框架对每轮记录 `write_stall_ratio`（单轮内最长调用/平均调用比），并在 `MedianReports` 里做**跨轮次检测**——某轮 write/read 速率低于本轮批次中位数的一半即判停顿、取中位数时自动剔除，输出标 `(N stalled runs dropped)`。若某场景标了 `STALLED`（全部轮次都被判停顿）说明磁盘持续繁忙，应重跑。

产物：`results/<label>_<scenario>.json`（单个报告）+ `results/<label>_all.json`（数组），可存档、可 diff。

## 指标说明

| 字段 | 含义 |
|---|---|
| `write_rate_pts_per_sec` | 点数 / 纯 Write/WriteBatch 调用耗时（排除数据生成） |
| `read_rate_pts_per_sec` | 点数 / 查询耗时 |
| `compression_ratio` | raw_input_bytes / on_disk_bytes（`raw` = 12B WAL 头 + variant 二进制） |
| `bytes_per_point` | on_disk / points（与模型无关，跨实现最可靠的可比量） |
| `peak_heap_bytes` | 运行期间 50ms 采样到的最大 HeapAlloc |
| `heap_after_gc_bytes` | 关闭后 GC 一次的 HeapAlloc（常驻） |
| `total_alloc_bytes` / `num_gc` | 累计分配 / GC 次数 |
| `segment_count` / `segment_bytes` / `index_bytes` / `wal_bytes` | 文件统计（观察段膨胀 / 索引 / WAL 残留占比） |
| `correct` | QueryFull 时读回点数 == 写入点数 |
| `write_stall_ratio` / `max_write_call_ns` | 单轮内最长写调用 / 平均写调用比（磁盘繁忙特征） |
| `disk_stalled` / `stalled_runs` | 跨轮次判定的停顿标记 / 被剔除的轮次数 |

## 场景矩阵（DefaultSuite）

见 [doc/02_testing.md](../02_testing.md) §2：小/中/大数据量、高基数 10 万 tag、多次 flush、
突发时间、字符串低基数、结构体、异步 flush、聚合查询、GOMEMLIMIT 内存受限。

新增场景：在 `suites.go` 的 `DefaultSuite()` 里加一行 `Scenario{...}` 即可。
数据生成模型见 `scenario.go`（值模式 / 时间模型 / tag 分布，seed 默认 42）。

## pprof 集成

```bash
# 跑场景时挂 pprof
go run ./doc/cmd/bench run -label v2-m1 -out results -pprof :6060

# 单独起 pprof 服务
go run ./doc/cmd/bench pprof :6060

# 采样
go tool pprof -top http://localhost:6060/debug/pprof/profile?seconds=30
go tool pprof -top http://localhost:6060/debug/pprof/heap
```

分析流程与已知瓶颈签名表见 [doc/03_profiling.md](../03_profiling.md)。

## 资源受限模拟

- `GOMEMLIMIT`：场景 `MemoryBudget` 字段 → 运行时 `debug.SetMemoryLimit`。
- 单核：场景 `MaxProcs` 字段 → `runtime.GOMAXPROCS`。
- 例：`memlimited-float-10tag-1M`（128MB 预算）。
