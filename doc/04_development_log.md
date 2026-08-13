# 开发日志（历史档案）

> **本文是历史开发档案**：每次改动的过程、测量与结论。当前状态（架构、已完成优化、用户边界、下一步）请看 [README.md](README.md) 和 [01_plan.md](01_plan.md)。新改动请继续在本文件追加条目。

---

## 2026-08-13 — flat-buffer WAL 实验（分支 `exp/flat-buffer-wal`，未合入本分支）

**背景**：WAL 内存里存 variant 对象（恒定 32B/个，float 序列化才 10B）。与用户讨论后确认：variant 拆壳（AsInt64 等）与字节拆壳（BigEndian.Uint64）都是 ~1ns 零分配，性能差可忽略；之前 3 次 byte-memtable 失败的真正原因是**每点 `AppendBinary(nil)` 分配**（GC 压力），不是拆壳开销。

**改动**（实验分支两个提交，基于本分支 903e6ba）：
- `879cc62` flat-buffer：`walDataEntry` 去掉 variant，改 `{off, length}` 引用 chunk 共享字节缓冲；Write 时 `value.AppendBinary(chunk.data)` 直接 append——**零每点分配**（之前 3 次失败版没做对的关键）。`memoryEstimate` 从估算变**精确**。
- `df4b7d7` WriteBytes：**用户实测发现第一版在刷盘时仍有一次 variant 装箱**（`decodeValue → variant → 编码器.AsFloat64`），正确诊断。修复：`Encoder` 接口加 `WriteBytes([]byte)`，标量编码器（int/float/string/bool）直接解析字节（~1ns 重解释，语义与 AsXxx 一致），容器编码器（json/column/adapt/unknown）回退 decode+Write；`ssColumn.WriteBytes` + `forEachCompleteFile` 全链路字节直通。

**实测**（默认 WAL，1M 点，repeat 3，同机）：

| 场景 | 旧（variant WAL） | 新（flat-buffer + WriteBytes） |
|---|---|---|
| single-float-1tag 写 | ~1.35M pts/s | **2.23M pts/s（+65%）** |
| batch-float-10tag 写 | ~890k pts/s | **1.0M pts/s（+13%）** |
| single peakHeap | 136~176MB | **128.7MB** |
| batch peakHeap | 105~145MB | **96.8MB** |
| WAL 内存（1M 点） | 48MB（估算） | **34.0MB（精确：10B 值 + 24B 元数据/条）** |

**诚实修正**：单条内存实际省 ~1.4×（元数据 ~24-28B 两种设计都有，不是之前说的 3-10× 纯结构体对比）。写吞吐提升来自「零每点分配 + 无装箱 + 少 GC」。

**结论**：flat-buffer + WriteBytes 是**双赢**（写更快 + 内存更省 + 估算变精确），`go test ./tsdb/` 与 benchmark 全绿。**保留在实验分支供审阅，后续可合入主线。** 剩余相关工作：方案 B 列式 memtable（彻底消除序列化）、compaction（M2）。

---

> 持续记录每次优化改动、基准测试结果与结论。配合 [02_testing.md](02_testing.md) 的对比流程，每个优化点 = 改动 + baseline/改后对比 + 结论，全部留档。
> 时间基准：2026-08-12。机器：本机（性能以本机为准，跨机对比需重采 baseline）。

---

## 2026-08-12 — 建立基准测试框架（分支 feat/tsdb-optimize）

**改动**：新增 `doc/`（重构计划 + 测试方案 + pprof 工作流）+ `doc/benchmark/`（基准框架）+ `doc/cmd/bench`（CLI）+ `.gitignore`。未改 `tsdb/` 生产代码。

**验证**：`go build ./...`、`go vet ./doc/...`、`go test ./doc/benchmark/` 全绿；CLI `run -quick` + `compare` 端到端跑通。

**当前引擎 QuickSuite 冒烟基线**（`-repeat 3`，同一引擎两次对比验证了中位数 + 停顿剔除生效）：

| 场景 | write pts/s | read pts/s | ratio | bytes/pt | peakHeap |
|---|---|---|---|---|---|
| small-float-1tag-20k | ~395k | ~887k | 4.63 | 4.75 | ~2MB |
| small-float-100tag-20k-batch | ~35k | ~72k | 3.35 | 6.58 | ~8MB |
| small-string-10tag-10k | ~63k | ~163k | 3.70 | 7.21 | ~2MB |

**两个基线观察**：
1. 批量写远慢于单点（35k vs 395k pts/s）——`flushWalSize` 强制多次 flush 时，每次 flush 内联做「列编码 + zstd 整块压缩 + fsync」，是当前写路径的主要成本。
2. 压缩率（段内）float 慢变 ≈ 4.6×、字符串 ≈ 3.7×，正常；但字节主要落在 segment，WAL 残留很少（close 后 truncate）。

---

## M1 第一步 — 编码缓冲惰性/小容量分配（已完成）

**目标**：高基数表内存可控。当前 `newSSColumn` 每 tag 一创建就按 `MaxBufferBatchSize`（默认 4096）预分配编码缓冲：TimeEncoder 32KB + 值编码器 32~64KB ≈ **96KB/tag**。10 万 tag → ~9.6GB，与数据量无关。

**改动**（`tsdb/column.go`、`tsdb/table.go`、`tsdb/segment_compression.go`）：
- 新增常量 `encoderInitCap = 256`，`newSSColumn` 各编码器初始容量从 `batchSize`（4096）改为 256，append 自动增长，`Reset` 保留已增长容量 → 只有稀疏/未写 tag 保持小容量。
- 删除 `ssTable.batchSize` 字段及其传递管道（WAL 分块仍用 `MaxBufferBatchSize`，不受影响）。

**验证**（新增 `doc/benchmark/highcard_test.go`，5000 个稀疏 tag 各写 1 点）：

| | peakHeap | 每 tag | 5 万 tag 外推 | 10 万 tag 外推 |
|---|---|---|---|---|
| 改前 (cfa1dc5) | 180.2MB | 36.0 KB/tag | ~1.8GB | ~3.6GB |
| 改后 | 26.0MB | 5.2 KB/tag | ~260MB | ~520MB |

稀疏 tag 内存 **~7× 下降**。`go test ./tsdb/`（含 8.6M 点大测试）与 `go test ./doc/benchmark/` 全绿。

**顺带发现（M1 后续点）**：`Meta.addTag` 每新建一个 tag 就 `file.Write + file.Sync()`（`table_meta.go:64`），实测 ~3ms/tag——建 1 万个 tag 要 ~30s。高基数写入场景应批量创建或去掉逐 tag fsync，属于 M1 后续项。

---

## 旧引擎全量 Baseline（已归档 `results/baseline-old-engine/`）

> 引擎：提交 `cfa1dc5`（未优化）；`-repeat 3` 取中位数（排除停顿轮）。10M 的 highcard 场景因旧引擎 tag 创建 fsync 单轮 ~15min，故全量 baseline 剔除该场景，单独用 2 万 tag 量化高基数。

| 场景 | write pts/s | read pts/s | ratio | bytes/pt | peakHeap |
|---|---|---|---|---|---|
| small-float-1tag-100k | 582k | 1821k | 45.6 | 0.48 | 5.5MB |
| medium-float-10tag-1M | 355k | 1268k | 14.2 | 1.55 | 22.2MB |
| large-float-100tag-10M | 445k | 1496k | 11.3 | 1.95 | 147.6MB |
| multiflush-int-10tag-1M-wal256k | 297k | 1537k | 123.5 | 0.18 | 12.6MB |
| bursty-float-10tag-1M | 226k | 775k | 5.6 | 3.97 | 21.3MB |
| string-lowcard-10tag-1M | 355k | 844k | 7.5 | 3.57 | 30.9MB |
| struct-10tag-1M | 132k | 115k | 23.3 | 2.49 | 131.5MB |
| async-float-10tag-1M | 465k | 1187k | 14.8 | 1.49 | 23.1MB |
| window-float-10tag-1M | 351k | 376* | 14.2 | 1.55 | 18.1MB |
| memlimited-float-10tag-1M | 901k | 6715k | 14.2 | 1.55 | 36.5MB |

\* window 的 read_rate 是「返回的聚合点数/秒」不是扫描点数/秒，绝对值无意义，仅用于新旧同口径对比。

**高基数内存对比（`TestHighCardMemory`，2 万稀疏 tag 各写 1 点）**：

| | peakHeap | 每 tag | 10 万 tag 外推 |
|---|---|---|---|
| 旧引擎 (cfa1dc5) | 715.9MB | 35.8 KB/tag | ~3.6GB |
| 新引擎（M1 第一步后） | 100.0MB | 5.0 KB/tag | ~500MB |

**baseline 关键观察**：
1. 写吞吐旧引擎 130k~900k pts/s 量级；压缩率按类型差异极大（int 计数 123×、float 慢变 45×、float 正弦 5.6×、字符串 7.5×、结构体 23×）。
2. `struct` 场景 peakHeap 131MB 是除高基数外最大的内存——ColumnEncoder + 字符串编码路径，M1 值得关注。
3. 所有场景 `segs=1`（未超 maxSegmentSize，全部块进同一 segment）→ segment 数指标要等 M2 compaction 才有意义。
4. `memlimited`（GOMEMLIMIT=128MB）写吞吐 901k 反而最高——内存上限改变了 GC/分配行为，可能也有噪声，需更多轮次确认。
5. 高基数（20k tag）内存被 M1 第一步从 715.9MB 压到 100MB（7.2×），但运行耗时 ~63s 未变——因为 `Meta.addTag` 逐 tag fsync 是独立瓶颈。

---

## M1 第二步候选（分析完成，设计待定）

### A. `Meta.addTag` 逐 tag fsync —— 高基数写入的致命瓶颈

`table_meta.go:64` 每新建一个 tag 就 `file.Write + file.Sync()`。实测 ~3ms/tag：
- 建 1 万个 tag ≈ 30s，建 10 万个 tag ≈ 5 分钟。
- 高基数写入场景（设计目标之一）被 tag 创建完全卡死。baseline 的 highcard 场景（10M 点 × 10 万 tag）单轮就要 ~15 分钟，几乎全部耗在 tag 创建。

**为什么不能直接删掉 fsync**：tag→code 映射只存在 meta 文件里，WAL 只存 tagCode 不存 tag 名。若 meta 条目丢失而对应 WAL 数据存活，重启后 code 会被复用 → 两个 tag 数据混写（数据损坏）。所以「meta 必须比 WAL 数据先持久化」。

**正确设计（并入 M1 重写）**：让 meta 持久化搭 WAL 的便车——WAL 分组提交时先 fsync meta 再落 WAL 数据，保证「code 先于数据持久」。或更进一步：新 tag 首次出现时把 tag 名写进 WAL，重启从 WAL 重建 Meta，meta 持久性 == WAL 持久性，无需单独 fsync。**注意：当前 WAL 用 bufio 自动刷盘（4KB 边界），任何「只在旋转时 fsync meta」的方案都会被 bufio 溢出破坏顺序，必须用显式分组提交替代。**

### B. 当前 fsync 全景（改前基线）

| 位置 | 频率 | 代价 |
|---|---|---|
| `Meta.addTag` `file.Sync()` | 每新建一个 tag | ~3ms/tag，高基数致命 |
| `BlockFile.Commit` `file.Sync()` | 每次 segment flush | 每次 flushCache 一次 |
| WAL 写入（bufio） | 从不显式 Sync，仅在旋转/Close 时 `Flush()` 到 OS | 无 fsync，但 OS 页缓存未持久 |

→ M1 的写路径目标是：**显式分组提交（group commit）**，每次 WAL 落盘一次 fsync（顺带刷 meta），segment 从 WAL 派生无需独立 fsync。

### C. batch 写 vs 单点写 的基准对比缺陷（已识别，baseline 完成后修正场景）早期冒烟「batch 35k vs 单点 395k pts/s」**不是** batch 路径本身慢，而是场景混入了：
1. 首个 batch 创建 100 个 tag（100 次 fsync ~300ms 一次性）；
2. `flushWalSize` 把 WalSize 压到 120KB → 每 ~4600 点一次内联 segment flush。

修正方向：batch/single 场景用**相同基数 + 相同 WalSize**（用默认 64MB 隔离「纯写入路径」），再单独设「建表成本」场景量化 tag 创建。

---

## 写路径 CPU profile（pprof 实证）

`doc/benchmark/profiling_test.go`（`PROFILE_WRITE=1`）抓取 2M 点、10 tag、WalSize 512KB（强制内联 flush）的写路径 CPU profile，`go tool pprof -top`：

| 热点 | 占比 | 含义 |
|---|---|---|
| `runtime.cgocall` | 25% | 系统调用开销（flush 的 fsync + 磁盘写） |
| `flushPending` sort（partition/insertion/Swapper） | ~10% | WAL 每 4096 条分块按 (Key,Time) 排序 |
| `ZFloatEncoder.Write` | 3.7% | 值编码 |
| `mapaccess2_fast32` | 2.4% | tag→maxTs 查表 |

**结论**：写路径 CPU 大头是 flush 的系统调用 + WAL 排序，印证 M1 方向——分组提交去掉每次 flush 的 fsync；`flushPending` 排序在 WriteBatch 已有序时可跳过（buffered 模式目前每 4096 条都排）。

---

## M1 第二步 — `Meta.addTag` 分组缓冲持久化（已完成）

**目标**：去掉高基数写入的致命瓶颈——每新建一个 tag 就 `file.Sync()`（实测 ~3ms/tag）。

**改动**（`tsdb/table_meta.go`、`tsdb/file_wal.go`、`tsdb/table.go`）：
- `Meta.addTag` 不再写文件/fsync，改为把条目追加到 `pending []byte` 内存缓冲，立即更新内存 map。
- 新增 `Meta.FlushPending()`（写 pending + 一次 fsync），并接到 WAL `flushPending` 的 `preFlush` 钩子上：**WAL 字节到达 OS 之前先刷 meta**。保证「tag code 先于引用它的数据持久化」——崩溃时不会出现 code 丢失后又被复用导致两个 tag 数据混写。
- `Close()` 前 flush pending，保证关库即持久。

**为什么安全**：WAL 落盘只发生在 `flushPending`（bufio 4KB 边界溢出也只在此时写入），meta 先 fsync 再写 WAL，顺序不变量成立。steady-state（无新 tag）时 preFlush 是空操作，不加额外 fsync。

**验证**（`TestHighCardMemory`，2 万稀疏 tag 各写 1 点）：

| | 耗时 | peakHeap |
|---|---|---|
| 改前（逐 tag fsync） | 64.0s | 715.9MB |
| M1 第一步后（编码缓冲） | 62.5s | 100.0MB |
| **本步后（meta 分组）** | **0.29s** | 100.0MB |

tag 创建 **~220× 提速**（10 万 tag 从 ~5min 到 ~1.5s）；内存维持 5KB/tag。

**吞吐连带提升**（QuickSuite，multi-tag 场景受益于首轮不再逐 tag fsync）：
- `small-float-100tag-20k-batch`：35k → **172k pts/s**（~5×）
- `small-string-10tag-10k`：63k → **123k pts/s**（~2×）
- 单 tag 场景持平（噪声内）。`go test ./tsdb/` 全绿。

---

## M1 同条件验证（old vs new 背靠背，10 场景）

> **教训**：跨时段对比会被磁盘状态污染。归档 baseline（`results/baseline-old-engine/`）是在磁盘更快时跑的，直接拿它比 M1 结果会得出「M1 变慢」的**错误结论**（实测同代码重跑就"变慢"）。正确做法：**同一机器状态背靠背**跑 old 和 new。因此又跑了一份 `results/old-engine-rerun/`（同一时段）作为公平基线，对比 `results/v2-m1/`。

`compare old-engine-rerun vs v2-m1`（同条件，repeat 3，阈值 20%）：

| 场景 | write | read | ratio | heap | B/pt | verdict |
|---|---|---|---|---|---|---|
| small-float-1tag-100k | +15.0% | +10.8% | 0.0% | **-21.0%** | 0.0% | PASS |
| medium-float-10tag-1M | +18.3% | +0.9% | 0.0% | -9.1% | 0.0% | PASS |
| large-float-100tag-10M | +20.0% | +3.1% | 0.0% | +1.5% | 0.0% | PASS |
| multiflush-int-10tag-1M-wal256k | -2.4% | +24.7% | 0.0% | -13.6% | 0.0% | PASS |
| bursty-float-10tag-1M | +32.2% | +5.6% | 0.0% | +0.8% | 0.0% | PASS |
| string-lowcard-10tag-1M | -4.3% | -6.9%* | 0.0% | -9.4% | 0.0% | PASS |
| struct-10tag-1M | +17.0% | +10.5% | 0.0% | -1.0% | 0.0% | PASS |
| async-float-10tag-1M | -1.8% | +0.9% | -3.1% | +0.1% | +3.2% | PASS |
| window-float-10tag-1M | +4.2% | +3.4% | 0.0% | -0.6% | 0.0% | PASS |
| memlimited-float-10tag-1M | +21.0% | +5.0% | 0.0% | +15.6% | 0.0% | PASS |

\* string 读在 repeat-3 对比时曾显示 -31.5%（FAIL），repeat-5 复核后收敛到 -6.9%（PASS）——确认是磁盘停顿噪声（旧引擎重跑该场景还剔除过一个停顿轮）。**小样本场景单次读速率噪声大，判定用 repeat-3 以上 + 停顿剔除。**

**结论**：M1 三步（编码缓冲 / meta 分组 / 零分配序列化）在同等条件下**无回归**：写吞吐多数场景 +15%~+32%（async/multiflush 持平），堆内存下降（small -21%、multiflush -14%），压缩率与 bytes/pt 不变。

**对比工具两个修复**：① 非 QueryFull 场景不再被 `Correct` 误判 FAIL；② `bytes_per_point` 阈值从 0（误报 WAL 残留波动）放宽到 10%。

---

## M1 byte-memtable 探索：三次设计均带写回归，已回退（结论记录）

**目标**：WAL 内存里存编码字节而非 variant 对象（~2.5× 内存节省）。**结论：当前结构下无法无回归实现，已回退到 7e74746 状态，放弃本轮。**

**三次尝试**（都实测写吞吐回退）：
1. **写时序列化存字节**：`walFile.Write` 每点 `AppendBinary(nil)` 存 Data。写吞吐 1.27M→~0.4M pts/s（~3× 回退）。根因：每点一次分配 + GC 压力（profile：nextFreeFast/tryDeferToSpanScan/memclr 抬头）。
2. **惰性转换**（Write 存 variant，flushPending 转字节）：写回归 ~44%（single 1.27M→719k），内存反升（136→216MB）。根因：flushPending 每点 `payload := AppendBinary(nil)` 分配（破坏 7e74746 的零分配优化）+ 转换期 variant 垃圾堆积。
3. **正确设计（未实现）**：flat buffer——序列化直接进 chunk 字节缓冲（零每点分配），entry 用 offset 引用，flushPending 零拷贝写文件。能同时满足「快写 + 省内存 + 零分配」，但需重构 walReadBuffer（chunk → {data, entries}），是又一次中等规模改动。

**判断**：WAL 内存本身已受 `MaxFileSize` 约束（可控），byte-memtable 只省 `variant 对象 vs 字节` 的 ~2× 常数。为它引入写回归不划算；flat buffer 设计适合与 M2 group-commit 一起做（彼时 flush 语义重写，天然融合）。**保留原版 WAL（variant + 零分配 flushPending）。**

**新增场景**（`suites.go`，保留）：`single/batch-*-defaultwal`（WalSize=0，数据全留 WAL 内存）——隔离纯写路径与 WAL 内存，供后续对比。

---

## M4 先行项 — 字符串字典压缩（已完成）

**目标**：字符串是压缩率最弱的一环（ratio 7.5 vs float 45 / int 123）。低基数重复字符串（status/host/level 等）先建字典、后续存整数下标。

**改动**（`tsdb/segment_compression_strings.go` + 3 个分派点）：
- `StringEncoder` 自适应：Write 同时累积原始流与 dict；`Bytes()` 取「dict 编码 vs snappy 压缩」中较小者（新增 marker `stringCompressedDict = 11`）。
- `StringDecoder` 改为解码成 `[]string` 列表，兼容两种 marker。
- 分派点更新：`PointDiskPack.AddSegment`、`AdaptColumnDecoder.decoderForMarker`、`ColumnDecoder.SetBytes`。

**验证**（`string-lowcard-10tag-1M`，repeat 3）：

| | 改前（snappy） | 改后（自适应） |
|---|---|---|
| ratio | 7.47 | **11.21** |
| bytes/pt | 3.57 | **2.38** |

低基数字符串压缩 **~1.5×**。高熵字符串 dict 更大时自动回退 snappy，不劣化。`go test ./tsdb/` 与 `./doc/benchmark/` 全绿。

---

## block 层压缩权衡实测（zstd vs snappy vs none）

**目标**：量化「二次压缩」（对已压缩列数据再整块压）的 CPU/压缩率取舍，给资源受限设备选 codec。codec 每文件记录（`FileHeader.CodecID`），可配置，无需改格式。

**改动**：`Scenario.CompressionName` 字段 + harness 透传 `SecondaryCompressionName` + 两个对比场景。

**实测**（`medium-float-10tag-1M`，repeat 3）：

| codec | write pts/s | read pts/s | ratio | bytes/pt |
|---|---|---|---|---|
| zstd（默认） | 239k | 911k | 14.19 | 1.55 |
| **snappy** | 284k (+19%) | **1264k (+39%)** | 13.42 (-5%) | 1.64 |
| none | 291k (+22%) | 1523k (+67%) | 5.79 (-59%) | 3.80 |

**结论**：snappy 是资源受限设备的最优默认——压缩率只降 5%，读快 39%、写快 19%（zstd 解压开销大）。none 的压缩率损失太大，仅在列压缩足够、完全不关心块层时用。**未改默认**（仍是 zstd，保压缩率）；设备上建议 `SecondaryCompressionName: "snappy"`。

---

## M1 写路径优化 — 跳过有序数据的 flushPending 排序（多 tag 写 2.2×）

**问题**：pprof 显示多 tag 写路径第一瓶颈是 `flushPending` 每次的 (Key, Timestamp) 排序（partition 22% + insertion 14% + swapper 16% ≈ **40%**）。10 tag 轮询写入时 chunk 全局无序 → 每次都排。

**关键洞察**：排序对正确性**不是必须的**——列编码时各 tag 独立累加、读时结果自排序、WAL 文件顺序无关。排序唯一作用是「容忍乱序写入」（重排保留乱序点，否则被 dedup 丢弃）。对有序数据（每 tag 时间递增，正常场景）是纯浪费。

**改动**（`tsdb/file_wal.go`）：新增 `sortedChunk`（全局有序，单 tag 快路径）+ `perTagMonotonic`（每 tag 时间单调，处理多 tag 交错有序）；两者都不满足才排序（保留乱序容忍）。

**验证**（默认 WAL，1M 点，repeat 3）：

| 场景 | 改前 | 改后 |
|---|---|---|
| batch-float-10tag-1M-defaultwal | 409k pts/s | **889k pts/s（2.2×）** |
| single-float-1tag-1M-defaultwal | ~1.2M | ~1.2M（不变，全局检查短路） |

`go test ./tsdb/` 全绿（含乱序写入测试，容忍语义保留）。

---

## 内存可追踪 — `DB.MemoryStats()` + 读侧上限配置（已完成）

**背景（用户问题）**：写内存能不能用 tag + WAL 大致算出来？——能。

**写内存估算公式**：
- **WAL ≈ 内存条数 × 每条成本**（每条 ≈ 48B 结构体 + 值大小：float/int 内联、字符串 +16+长度、map 更大）。条数受 WAL 文件上限约束。
- **编码器 ≈ tag 数 × ~5KB（固定，M1 后） + 缓冲点数 × 8B（时间戳）**。
- 注意：WAL 存的是「对象」不是序列化字节，估算 ~5× 磁盘大小，是上下限而非精确值。

**改动**：
- `DB.MemoryStats()`：返回 读侧（catalog 段索引字节 + 打开的读句柄数）与 写侧（WAL 条数/估算字节 + 编码器估算）。
- `Config.MaxOpenReaders`：可配置读文件句柄上限（默认 64，每个句柄缓冲 ~64KB）——读内存控制旋钮，低内存设备可调小。
- benchmark 每个场景报告这些字段。

**验证**（batch-float-10tag-1M-defaultwal）：报告 WAL **1M 条 / ~48MB 估算**、编码器 20KB、catalog 0（数据全在 WAL 未落盘）——与公式吻合。`go test ./tsdb/` 全绿。

---

## M1 后续候选（未开始）

- WAL 分组 fsync / flush 成本摊薄（当前每次 flush 内联「列编码 + zstd 整块压缩 + fsync」）。
- `Meta.addTag` 逐 tag fsync → 批量/去 fsync（高基数建表提速）。
- flush 期间 `queryMute` 全表锁阻塞查询 → 写时 COW 段替换。
