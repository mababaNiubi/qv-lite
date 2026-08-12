# 测试方案：改动前后对比 + 全场景覆盖

> 原则一句话：**先 baseline，再改动；同场景、同配置、同一台机器；结构化输出，存档 diff，不靠肉眼。**
> 测试代码在 [`benchmark/`](benchmark/)，可直接运行，见 [benchmark README](benchmark/README.md)。

---

## 1. 测试原则

1. **Baseline 先行**：任何改动前，先在当前 `HEAD` 跑一遍完整场景矩阵，结果存档为 `baseline`。
2. **改动后可重复**：每个里程碑后重跑同一矩阵，存档为 `v2-*` 之类 label。
3. **同机同配置**：对比结果只在同一台机器、同一 Go 版本下有效。机器变更时**必须重新采集 baseline**。
4. **确定性数据**：所有场景用固定 seed（默认 42）生成数据，新旧两次运行写入的是**完全相同的点序列**。
5. **结构化输出**：每个场景产出一个 JSON `Report`，diff 工具自动判定 PASS/FAIL。

## 2. 场景矩阵（覆盖你点名的所有情况）

| 场景 | 点数 | 基数 | 值类型 | 时间模型 | batch | flush | 覆盖点 |
|---|---|---|---|---|---|---|---|
| `small-float-1tag-100k` | 100k | 1 | float | 均匀 | 单点 | 默认 | 小数据量、单 tag 串行 |
| `medium-float-10tag-1M` | 1M | 10 | float | 均匀 | 单点 | 默认 | 中等数据量、中基数 |
| `large-float-100tag-10M` | 10M | 100 | float | 均匀 | 单点 | 默认 | 大数据量、多段、全量读 |
| `highcard-float-100ktag-10M` | 10M | **100k** | float | 均匀 | 单点 | 默认 | **高基数**、内存与基数解耦 |
| `multiflush-int-10tag-1M-wal256k` | 1M | 10 | int | 均匀 | 单点 | **256KB** | **多次 flush**、段文件数 |
| `bursty-float-10tag-1M` | 1M | 10 | float | **突发** | 单点 | 默认 | 突发写入、非均匀时间 |
| `string-lowcard-10tag-1M` | 1M | 10 | **字符串低基数** | 均匀 | 单点 | 默认 | 字典压缩率 |
| `struct-10tag-1M` | 1M | 10 | **结构体** | 均匀 | **1000** | 默认 | 结构列、批量写 |
| `async-float-10tag-1M` | 1M | 10 | float | 均匀 | 单点 | 默认 | **AsyncFlush** 异步路径 |
| `window-float-10tag-1M` | 1M | 10 | float | 均匀 | 单点 | 默认 | **聚合查询** QueryWindow |
| `memlimited-float-10tag-1M` | 1M | 10 | float | 均匀 | 单点 | 默认 | **资源受限**（GOMEMLIMIT=128MB） |

矩阵覆盖了：**大数据量 / 小数据量 / 中等数据量 / 多次 flush / 高基数 / 突发 / 字符串 / 结构体 / 异步 / 聚合 / 内存受限**。

补充场景（`suites.go`，按需用 `-scenarios` 指定）：
- `single-float-1tag-1M-defaultwal` / `batch-float-10tag-1M-defaultwal`：**默认 WAL（不强制 flush）**，数据全留 WAL 内存，隔离纯写路径与 WAL 内存占用（`mem_wal_*` 字段）。
- `medium-float-10tag-1M-snappy` / `medium-float-10tag-1M-none`：**block 层压缩 codec 对比**（配合 `CompressionName` 场景字段）。

## 3. 指标定义

每次场景运行输出以下指标（`Report`，JSON）：

| 指标 | 定义 | 意义 |
|---|---|---|
| `write_rate` | 点数 / 纯 Write/WriteBatch 调用耗时（排除数据生成） | 写吞吐 |
| `read_rate` | 点数 / 查询耗时 | 读吞吐 |
| `raw_input_bytes` | Σ(12B WAL 头 + variant 二进制长度) | 未压缩输入量 |
| `on_disk_bytes` | 表目录下所有文件字节和（`*.tsb` + `*.idx` + `*.wal`） | 落盘量 |
| `ratio` | raw_input / on_disk | 压缩率 |
| `bytes_per_point` | on_disk / points | 每点落盘字节（与模型无关的可比量） |
| `peak_heap` | 运行期间采样到的最大 HeapAlloc | 内存峰值 |
| `heap_after_gc` | 关闭后 GC 一次的 HeapAlloc | 常驻内存 |
| `total_alloc` / `num_gc` | 累计分配 / GC 次数 | 分配压力、GC 开销 |
| `segment_count` / `index_bytes` / `wal_bytes` | 文件统计 | 段膨胀、索引开销、WAL 残留 |
| `correct` | QueryFull 时 ReadCount == Points | 正确性自检 |
| `mem_catalog_bytes` / `mem_wal_entries` / `mem_wal_bytes_est` / `mem_encoder_bytes_est` / `mem_reader_cache_open` | `DB.MemoryStats()` 拆解 | 读/写内存估算（写内存公式见 README） |

## 4. 数据生成模型（确定性）

- **seed**：每个场景固定（默认 42），`go test -run` 与 CLI 两次运行产生相同点序列。
- **值模式**（`ValueType`）：
  - `FloatSlow`：缓变温度类（高可压）
  - `FloatWave`：正弦（中可压）
  - `IntCounter` / `IntRandom`：单调计数（RLE 友好） / 随机
  - `StringLowCard`：32 个取值轮换（字典压缩友好）
  - `StringHighEntropy`：32 位随机 hex（几乎不可压，测下界）
  - `BoolMostlyTrue`：90% true（RLE 友好）
  - `StructTwoCol`：`{name, value}` 结构体列
- **时间模型**：`TSRegular`（固定 step）/ `TSBursty`（64 点突发 + 8× 间隔）/ `TSJitter`（带抖动）。
- **tag 分布**：round-robin，每 tag 点数均匀，新旧运行完全一致。

## 5. Baseline 采集（改动前必做）

```bash
# 确保工作区在待评估的 commit（改动前为当前 HEAD）
git stash -u            # 或 git checkout <before-commit>
go test ./...           # 先确认改动前测试全绿

# 全矩阵 baseline（产出 results/baseline_*.json + results/baseline_all.json）
go run ./doc/cmd/bench run -label baseline -out results -version head
```

> 大数据量场景耗时：10M 场景约 1~3 分钟，全矩阵约 5~15 分钟。可先跑
> `-quick` 或 `-scenarios small,medium` 验证链路，再跑全量。

## 6. 改动后对比

```bash
# 完成某个里程碑后，同一台机器重跑（建议 -repeat 3，取中位数，压掉 GC/缓存噪声）
go run ./doc/cmd/bench run -label v2-m1 -out results -version m1 -repeat 3

# 对比（默认阈值：写/读/比率/堆均允许回归 20%）
go run ./doc/cmd/bench compare results/baseline_all.json results/v2-m1_all.json

# 收紧/放松阈值，例如写不允许回归、压缩率不允许回归
go run ./doc/cmd/bench compare -write 0 -ratio 0 results/baseline_all.json results/v2-m1_all.json
```

> **为什么 -repeat**：磁盘繁忙（fsync 竞争、页缓存刷盘、杀毒扫描、其他进程 IO）会污染单次结果。框架用**跨轮次停顿检测**处理：某轮 write/read 速率低于本轮批次中位数的一半（默认 0.5）即视为磁盘停顿，取中位数时自动剔除并在输出里标 `(N stalled runs dropped)`。合法的重活（如当前引擎内联 flush）每轮都发生、吞吐一致，不会被误判。
>
> 同一引擎跑两次，小场景 read_rate 可波动 ±50%；`-repeat 3~5` 取中位数后，写/读速率基本收敛到 ±10% 内。中大型场景（≥1M 点）噪声明显更小。**权威对比建议 `-repeat 5`**。

输出形如：

```
scenario                         write(old→new)    read(old→new)    ratio(old→new)  heap(old→new)  verdict
highcard-float-100ktag-10M       +12.3%            +8.1%            +5.2%           -68.4%         PASS
small-float-1tag-100k            +2.1%             +1.4%            +0.3%           -5.0%          PASS
```

## 7. go test 直跑（每里程碑回归用）

```bash
# 正确性自检 + 小场景冒烟（快，几秒）
go test ./doc/benchmark/ -run TestQuickSuite -v

# 全矩阵（等价于 run 命令），默认跳过，需显式开启
BENCH_FULL=1 go test ./doc/benchmark/ -run TestBaselineSuite -v
```

## 8. 各里程碑验收标准（判定改动是否合理）

| 里程碑 | 必须 PASS 的场景 | 硬性门槛（不满足即回退/继续调） |
|---|---|---|
| M1 | 全部，重点 `highcard` / `multiflush` / `memlimited` | 高基数 `peak_heap` 显著下降；`write_rate` 不劣化；现有测试全绿 |
| M2 | `multiflush` / `large` / `bursty` | `segment_count` 收敛到阈值；`read_rate` 不劣化 |
| M3 | `QueryFull` / `QueryRecent` / `reopen` | 重复查询 `read_rate` 提升；冷/热查询差距缩小 |
| M4 | `string-lowcard` / `struct` / `json` | `ratio` 不劣化，期望提升；写/读吞吐不劣化 |
| M5 | `large` 冷查询 | `read_rate` 提升 |

**判定规则**（`compare` 默认阈值，均可调）：
- `write_rate`、`read_rate`、`ratio`、`peak_heap` 相对 baseline 回归超过 20% → FAIL。
- `correct == false` → FAIL（数据都错了，性能无意义）。
- 里程碑「期望提升」的指标，建议把阈值收紧到 `0`（不允许回归）来强制推进。

## 9. 注意事项

- **结果目录**：默认 `results/`，建议提交到 git 或单独目录归档，避免被 `rm`。**每次留档**（`-label` 命名 + 结果 JSON + 对应 commit）。
- **机器变更**：换机器/换 Go 版本必须重采 baseline，旧结果不可跨机对比。
- **小场景 heap 噪声大**：几万点的小场景，`peak_heap` 会被 Go 运行时首次接触/GC 节律主导（实测同引擎波动可达 +80%）。**比内存请以 medium/large（≥1M 点）场景为准**。
- **磁盘繁忙污染**：`run -repeat 3`（默认）会自动剔除速率明显偏慢的停顿轮次（跨轮次判据，阈值 0.5，见 §6）。若某场景输出标了 `STALLED`（全部轮次都被判停顿），说明磁盘持续繁忙，重跑或换时段。报告里的 `write_stall_ratio` / `max_write_call_ns` 是单轮内部的最长调用/平均调用比，供排查用。
- **压缩率要测到「段压缩」而非 WAL**：当前引擎数据不落 WAL 就测不到 segment 编码。DefaultSuite 已按场景设了 `WalSize` 强制多次 flush；若自定义场景，用 `flushWalSize` 设一个会触发多次落盘的 `WalSize`，并留意 `segment_bytes` / `wal_bytes` 的占比。
- **磁盘**：10M 场景落盘约几十 MB，`go test` 临时目录会自动清理；CLI 用 `-keep` 保留现场便于排查。
- **pprof**：长场景运行时挂 `-pprof :6060`，配合 `03_profiling.md` 定位瓶颈。
