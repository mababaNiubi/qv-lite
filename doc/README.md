# qv-lite TSDB 优化项目

> **换电脑后从这里开始。** 这个 README 是当前状态的权威入口；历史开发过程见 [04_development_log.md](04_development_log.md)。

## 一句话状态

基于 `tsdb/` 的时间序列数据库做性能优化，分支 **`feat/tsdb-optimize`**。已完成 **M1（写吞吐 + 内存可控）** + **字符串字典压缩** + **内存可追踪（`DB.MemoryStats`）**，全部实测验证、无回归。设计目标：**内存可控、可运行在资源受限设备上、写/读吞吐高、压缩率高**。

## 快速开始（换电脑后第一步）

```bash
# 构建 + 全量测试（改过代码后必须跑）
go build ./...
go test ./tsdb/          # ~60s，含 8.6M 点大测试
go test ./doc/benchmark/ # 基准框架自检（含正确性 round-trip）

# 基准测试：跑场景 → 对比
go run ./doc/cmd/bench run -label myrun -out results -repeat 3 -scenarios medium-float-10tag-1M
go run ./doc/cmd/bench compare results/old_all.json results/new_all.json

# 写路径 CPU 剖析（net/http/pprof 工作流见 03_profiling.md）
PROFILE_WRITE=1 go test ./doc/benchmark/ -run TestProfileWritePath -v
go tool pprof -top doc/benchmark/write_cpu.out
```

完整基准方法论（场景矩阵 / baseline / 磁盘噪声处理 / 阈值）见 [02_testing.md](02_testing.md)。

## 已完成的优化 + 实测数据

| 优化 | 文件 | 实测效果 |
|---|---|---|
| 编码缓冲小容量分配 | `column.go` / `segment_compression.go` | 高基数内存 **7.2×**（20k tag 715.9→100MB，5KB/tag） |
| Meta.addTag 分组 fsync | `table_meta.go` / `file_wal.go` | tag 创建 **220×**（20k tag 64s→0.29s）；meta 先于 WAL 数据落盘保序 |
| WAL 零分配序列化 | `file_wal.go` | 消除 flushPending 每点分配（AppendBinary 直接进缓冲） |
| **flushPending 排序跳过** | `file_wal.go` | **多 tag 写 2.2×**（409k→889k pts/s）；保留乱序容忍 |
| 字符串字典压缩 | `segment_compression_strings.go` | 低基数字符串 ratio 7.5→11.2（~1.5×） |
| DB.MemoryStats + MaxOpenReaders | `db.go` 等 | 读/写内存可估算、读句柄上限可配 |

## 用户定下的边界（别越界）

1. **不能接受断电丢数据** → **不做 group commit**。当前只有 segment commit + meta 是 fsynced 的，WAL 从不 fsync；去 segment fsync 会把掉电持久性降到近零。
2. **默认压缩不变**（zstd）。实测 snappy 读快 39%、写快 19%、压缩率只降 5%（资源受限设备可自行设 `SecondaryCompressionName: "snappy"`，但不改默认）。
3. **写 >> 读** → 边读边写优化（COW queryMute）优先级低，暂不做。
4. **byte-memtable 是错误权衡**（内存换写回归，实测三次都拖慢写 40%~3 倍），不做；**block 缓存低价值**（写多读少命中率低 + 添内存），不做。

## 写内存估算公式（`DB.MemoryStats()` 已实现）

```
写内存 ≈ WAL（条数 × ~48B/条；float/int 内联，字符串 +16+len，map 更大）
       + 编码器（tag 数 × ~5KB 固定 + 缓冲点数 × 8B 时间戳）
注意：WAL 存「对象」不是字节，实际 ~5× 磁盘大小，是上下限估算。
```

## 当前写/读性能基线（本机，默认配置）

- 单 tag 写 ~1.3M pts/s；多 tag 写 ~890k pts/s（排序修复后）
- 压缩率按类型：float 慢变 45×、int 计数 123×、字符串 7.5×（dict 后 11×）、struct 23×
- 存档 baseline：`results/baseline-old-engine/`（旧引擎）、`results/v2-m1/`（M1 后）、`results/old-engine-rerun/`（同条件对比）

## 文档地图

- **[01_plan.md](01_plan.md)** — 当前架构（写/读路径、内存模型、数据格式）+ 剩余里程碑与权衡
- **[02_testing.md](02_testing.md)** — 测试方案：场景矩阵、baseline 采集、对比判定、磁盘噪声处理
- **[03_profiling.md](03_profiling.md)** — pprof 工作流 + 已知瓶颈签名表
- **[04_development_log.md](04_development_log.md)** — 历史开发日志（档案，含每次改动的测量与结论）
- **[benchmark/](benchmark/README.md)** — 基准测试框架（确定性数据生成、内存采样、压缩率统计、新旧对比）
