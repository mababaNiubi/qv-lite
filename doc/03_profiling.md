# 性能分析工作流（net/http/pprof）

> 目标：在 benchmark 场景运行期间**持续采样**，定位写/读/内存瓶颈，改完一轮再测再采。
> 工具支持见 `benchmark/profiler.go`，命令集成见 `benchmark/README.md`。

---

## 1. 启动 pprof 服务

跑场景时挂上 HTTP pprof，运行期间就能实时采样：

```bash
go run ./doc/cmd/bench run -label v2-m1 -out results -pprof :6060
```

或长场景单独采样：

```bash
# 起服务（等价于场景内的 -pprof）
go run ./doc/cmd/bench pprof :6060
```

验证：`curl http://localhost:6060/debug/pprof/` 应返回可用 profile 列表。

## 2. 采集哪些 profile（按需）

| Profile | 命令 | 回答的问题 |
|---|---|---|
| CPU | `go tool pprof -top http://localhost:6060/debug/pprof/profile?seconds=30` | 写/读各阶段 CPU 烧在哪 |
| 堆 | `go tool pprof -top http://localhost:6060/debug/pprof/heap` | 内存被谁占着（配合 `-alloc_space` 看累计分配） |
| 分配 | `go tool pprof -alloc_space -top http://localhost:6060/debug/pprof/heap` | 分配热点（GC 压力来源） |
| 协程 | `go tool pprof -top http://localhost:6060/debug/pprof/goroutine` | goroutine 泄漏 / 堆积 |
| 锁竞争 | `go tool pprof -top http://localhost:6060/debug/pprof/mutex` | 锁热点（需 `-mutexprofilefraction`） |
| 阻塞 | `go tool pprof -top http://localhost:6060/debug/pprof/block` | 阻塞热点（需 `-blockprofilerate`） |

web 可视化：`go tool pprof -http=:8080 http://localhost:6060/debug/pprof/profile?seconds=30`（火焰图/调用图）。

## 3. 定位流程（推荐节奏）

```
1. 跑 baseline，-pprof :6060
2. 写阶段：CPU 采样 30s → 看 top
3. 读阶段：CPU 采样 30s → 看 top
4. 对比「写阶段 top」与「读阶段 top」，各自首热函数即瓶颈
5. 对照下面的瓶颈签名表判断根因
6. 改代码 → 重跑同场景 compare → 确认指标方向 → 再采样
```

## 4. 瓶颈签名表（当前代码已知的高危点）

| 签名 | 现象（profile 特征） | 根因 | 对应优化 |
|---|---|---|---|
| 分配密集 | CPU top 是 `mallocgc`、`make([]byte`、`bytes.Buffer` | `block_file.go:671` Decode 每次新分配；`block_compressor.go` 每次 new Buffer | 解压缓冲复用 / sync.Pool（计划 M1） |
| fsync 阻塞 | CPU 不高但写吞吐上不去，`syscall`/`runtime.kevent` 高 | `block_file.go:365` 每次 commit Sync | WAL 分组 fsync（M1） |
| 二次压缩白跑 | 写阶段 zstd 相关函数占 CPU | `block_file.go:188` 整块再压 | 去二次压缩 / 按系列压（M1/M4） |
| 锁竞争 | `sync.(*Mutex).Lock` 高，写多并发时吞吐塌方 | `flushCache` 持 `queryMute.Lock()` 全表锁 | COW 段替换（M1/M3） |
| 每次查询读盘 | 读阶段 `os.ReadFile`/`open` 高 | `segment.go:87` GetIndex 现读 `.idx` | 内存 catalog（M3） |
| GC 压力 | `num_gc` 高、`scanobject`/`gcBgMarkWorker` 高 | 高基数每列 128KB 预分配；全 WAL 物化 | memtable 预算 + 惰性分配（M1） |
| 字符串压缩收益低 | snappy 压缩占比高但 ratio 不涨 | `segment_compression_strings.go` 无字典 | 字符串字典编码（M4） |
| 解压整块浪费 | 读单点却整块解压、CPU 在 Decode | 块粒度过大 + 无块缓存 | 块 LRU 缓存（M3） |

## 5. 文件型 profile（不依赖 HTTP，CI 友好）

```bash
# 用 -cpuprofile 方式跑 go test（benchmark 直接支持）
go test ./doc/benchmark/ -bench . -cpuprofile cpu.out -memprofile mem.out -benchtime=1x
go tool pprof -top cpu.out
go tool pprof -top mem.out
```

或借助 `benchmark/profiler.go` 的 `CPUProfile(path, dur)` / `HeapProfile(path)` 在场景内定点采集。

## 6. 闭环节奏

```
基线采样(找热点) → 改代码 → 同场景 compare(验指标) → 再采样(确认热点消失 / 出现新热点) → 下一轮
```

每个里程碑收尾前：**profile 首热函数必须与「该里程碑想优化的点」一致**，否则说明方向错了，先回退假设再动手。
