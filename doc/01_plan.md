# 架构与计划（更新版：反映当前研究后的状态）

> 这份文档是「现在代码长什么样 + 还能做什么」的权威说明。最初的原始计划已并入历史（见 04_development_log.md）。

## 1. 当前架构（优化后）

### 写路径

```
Write(point)
  → ssTable.Write：Meta.Load(tag→code) → walFile.Write
  → walFile.Write（buffered）：存进 walReadBuffer chunk（内存）
       chunk 满 4096 → flushPending：
         preFlush（meta 先 fsync，保证 tag code 先于数据持久）
         排序检查（全局有序 / 每 tag 单调 → 跳过排序；真乱序才排）
         零分配序列化 → bufio 写 WAL 文件
  → WAL 文件满 MaxFileSize → rotate → NeedFlush
  → flushCache（同步或 AsyncFlush 后台）：
         遍历完整 WAL 文件 → 各列编码器累加 → 编码写 segment（BlockFile）
         Commit → zstd 整块压 + fsync → 截断已消费 WAL 文件
```

关键点：
- **WAL 是草稿，segment 是正式压缩数据**。WAL 只在 rotate/Close 时 bufio.Flush 到 OS（不 fsync）；segment commit 每次 fsync。
- **持久性模型**：segment commit + meta 是 fsynced 的（断电不丢）；WAL 数据断电会丢（只在页缓存）。这是当前权衡，用户明确「不能接受大范围丢数据」→ 不改这个模型。
- 每 tag 一个编码器（`ssColumn`），初始容量 256（小，内存可控），flush 时 Reset。

### 读路径

```
Query → queryCache（WAL 内存，ReadByTime 过滤+排序）+ queryDisk（segment）
queryDisk → forEachBlock：用段索引（内存 catalog，每个 fileSegment.index）
          → 命中块读 → BlockFile.readAt → zstd 解压 → 解码列
```

- 段索引在内存（启动加载 .idx 或首次 GetIndex 后缓存）——这就是「catalog」。
- 读句柄数受 `Config.MaxOpenReaders`（默认 64）限制。

### 数据格式

- WAL 记录：`[4B len][4B key][8B ts][variant binary]`，无 CRC。
- segment：`BlockFile`（magic/header/TxState/CodecID + 压缩块 + 块索引），块内 = SegmentHeader + 列压缩数据（时间用 delta/simple8b/RLE，值按类型：float XOR、int delta、字符串 snappy/字典、bool 位图）。
- meta：append-only，per-entry CRC，crash 恢复截断半条。

## 2. 已完成（M1 + 压缩 + 内存追踪）

见 [README.md](README.md)「已完成的优化」表和 [04_development_log.md](04_development_log.md)。核心：

- M1 内存可控：编码器惰性分配（高基数 7.2×）、Meta 分组 fsync（tag 创建 220×）
- M1 写吞吐：零分配序列化、flushPending 排序跳过（多 tag 写 2.2×）
- 字符串字典压缩（ratio 7.5→11.2）
- 内存可追踪：`DB.MemoryStats()` + `Config.MaxOpenReaders`

## 3. 剩余里程碑（已按研究结果修正）

### M2 — compaction（合并段）【最大剩余收益，未做】
- 现状：segment 只增不合并，段数随 flush 线性增长，查询 O(段数)、索引膨胀、时间序列短压缩率受限。
- 收益：读侧内存（catalog）可真正压住、查询更快、压缩率更高。
- 风险：与写并发竞态（需 COW 段替换或 flushMute 协调）、正确性敏感。**建议下一步做。**

### M3 — 读路径 catalog/块缓存【已部分完成，块缓存放弃】
- 已做：catalog（段索引内存缓存）+ `MaxOpenReaders` 控制读句柄。
- 已放弃：解压块 LRU 缓存（写多读少命中率低 + 添内存，用户确认不做）。
- 剩余：无大项；`GetIndex` 负结果缓存是微小项（多数情况下 w.index 已设置，收益近零）。

### M4 — 压缩率【字符串字典已完成】
- 已做：字符串字典（低基数 1.5×）。
- 剩余候选：
  - block 层二次压缩改成可配置（已可配：`SecondaryCompressionName`；实测 snappy 读快 39% 写快 19% 压缩率只降 5%，用户选择保持 zstd 默认）。
  - JSON 回退路径（`segment_compression_json.go`）LZ4 换 zstd 或重复项折叠——低优先级。

### M5 — mmap【未做，Windows 复杂】
- 冷查询读性能。降级方案：大块对齐 ReadAt 页缓存兜底。低优先级。

## 4. 已评估但放弃/搁置的项（别重复尝试）

| 项 | 结论 | 原因 |
|---|---|---|
| group commit（去 segment fsync） | 搁置 | 掉电持久性降到近零，用户否决 |
| byte-memtable（WAL 存字节） | 放弃 | 三次设计实测写回退 40%~3 倍，正确设计（flat buffer）需并入 compaction 重写才划算 |
| block LRU 缓存 | 放弃 | 写多读少命中率低 + 添内存 |
| flushPending 排序移除 | 部分做 | 排序是「容忍乱序」正确性依赖；已做「有序跳过」（多 tag 2.2×），不能完全删 |
| COW 边读边写 | 搁置 | 复杂度高，用户定写 >> 读、优先级低 |

## 5. 性能瓶颈现状（已实测定位）

- 写路径（默认配置）：单点 ~1.3M pts/s、多 tag ~890k pts/s。剩余开销：每点 mutex + sync.Map tag 查询 + readBuffer append（结构性，需大改才降）。
- 写路径（flush 重负载）：flushCache 的编码 + zstd + fsync 是主要成本；fsync 每 WAL rotate 一次（默认 64MB → 约 0.3s 一次，非瓶颈）。
- 读路径：解压是主要成本（zstd 解压贵）；snappy 可快 39%（用户选保持 zstd）。
- 详细 profile 数据见 04_development_log.md 各条 + 03_profiling.md。
