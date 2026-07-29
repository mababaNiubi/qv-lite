# BufferMode 统一配置：Flush 模式数据丢失问题分析

## 背景

将 `CloseBuffer bool` 配置统一为 `BufferMode string`，支持三种模式：

| 模式 | 行为 | 内存 | 读路径 |
|------|------|------|--------|
| `"buffer"` (默认) | 完全内存缓冲，批量排序后刷盘 | 高 | readBuffer (内存) |
| `"flush"` | 缓冲写入用于排序，刷盘后丢弃 readBuffer | 低 | 磁盘 (forEachWalFile) |
| `"close"` | 直接写盘，无缓冲 | 最低 | 磁盘 (forEachWalFile) |

## 问题

`"flush"` 模式下写入 5000 个点，查询只返回 4976 个，固定丢失 24 个点。

`"close"` 和 `"buffer"` 模式不受影响。

## 复现

```
MaxFileSize:        4 * 1024
MaxBufferBatchSize: 50
BufferMode:         "flush"
写入: 5000 个 variant.NewInt(i), 时间戳单调递增
结果: 查询返回 4976, 丢失 24 个
```

## 诊断数据

### 丢失的时间戳索引

```
[200, 401, 602, 803, 1004, 1205, 1406, 1607, 1808, 2009,
 2210, 2411, 2612, 2813, 3014, 3215, 3416, 3617, 3818, 4019,
 4220, 4421, 4622, 4823]
```

### 规律分析

- 间隔固定为 **201** (不是 50 的倍数)
- 201 = 4 * 50 + 1 = 4 个 batch + 1 条 SyncToDisk 记录
- 每个文件包含 **201 条记录**，丢失的是每个文件的 **第 1 条** (SyncToDisk 写入的记录)
- 5000 / 201 = 24.9，共 24 个完整文件，每个丢 1 条 = 24 条

### 数据分布

| 位置 | 记录数 |
|------|--------|
| Segments (已编码) | 4800 |
| WAL active 文件 (磁盘) | 176 |
| 丢失 | 24 |
| **总计** | **4976** (应为 5000) |

### 记录大小

- 每条记录 26 字节: 4 (length) + 4 (key) + 8 (timestamp) + 10 (binaryValue)
- MaxFileSize=4096, 每个文件可存 ~157 条
- 但 batch 是 50 条一批写入, rotateIfFull 在 batch 后检查
- 3 batch (150条) + 1 SyncToDisk = 151条, 151*26=3926 < 4096
- 4 batch (200条) + 1 SyncToDisk = 201条, 201*26=5226 >= 4096, 触发轮转

## 根因分析

### 数据流

flush 模式下 `flushCache` 的执行流程:

```
1. SyncToDisk():
   a. flushPending() - 将 active chunk 写入 writeBuffer, 丢弃 chunk
   b. writeBuffer.Flush() - 刷盘到 active 文件
   c. (修复尝试) 如果 active 文件有数据, 强制轮转

2. forEachCompleteFile():
   - 快照 walFiles[:len-1] (排除 active 文件)
   - flush 模式: 通过 forEachWalFile 从磁盘读取

3. truncate(consumed):
   - 删除已处理的 complete 文件
```

### 核心矛盾

`forEachCompleteFile` **排除 active (最后) 文件**。

在 flush 模式中, `SyncToDisk` 将 active chunk 刷到 active 文件的磁盘上,
但 `forEachCompleteFile` 不读 active 文件。这些数据留在 WAL 中, 等
active 文件轮转后变为 complete 才会被读取。

### 关键发现

诊断显示: 丢失的 24 条记录 **不在磁盘上** (在所有写入完成后检查)。

这些记录曾在 complete 文件中, 但 complete 文件已被 `truncate` 删除。
说明 `forEachCompleteFile` 读取这些文件时 **没有读到** SyncToDisk 写入的那条记录。

### 尝试的修复: 强制轮转

在 `SyncToDisk` 末尾添加强制轮转:

```go
if ws.walFiles[len(ws.walFiles)-1].length > 0 {
    return ws.addWalFile()
}
```

**结果: 仍然丢失 24 条, 模式完全一致。**

可能原因:
1. `addWalFile()` 关闭旧文件前, `writeBuffer.Flush()` 的数据可能未完全落盘
2. `forEachWalFile` 的 `bufio.NewReader` 可能在文件边界处有缓冲问题
3. `flushPending` 中 `ent.length += dataLen` 在 `writeBuffer.Write` 之前执行,
   可能导致 `rotateIfFull` 提前触发, 数据实际还在 writeBuffer 中未落盘

## 待排查方向

### 1. addWalFile 中 writeBuffer 未 Flush 就 Close

`addWalFile()` 的代码:
```go
func (ws *walFile) addWalFile() error {
    // ...
    err = ws.writeFile.Close()  // 直接关闭旧文件!
    // ...
    ws.writeBuffer.Reset(file)  // 重置 writeBuffer (清除未刷数据)
}
```

**关键**: `addWalFile` 在 `Close()` 旧文件之前 **没有调用 `writeBuffer.Flush()`**。
`rotateIfFull` 虽然在调用 `addWalFile` 前调了 `Flush()`, 但 `SyncToDisk`
在 `flushPending` 之后又可能往 `writeBuffer` 写了新数据 (来自 active chunk)。
如果 `flushPending` 内部的 `rotateIfFull` 没有触发 (文件未满), 数据留在
`writeBuffer` 中。然后 `SyncToDisk` 调 `writeBuffer.Flush()` 刷盘。
但 `SyncToDisk` 的强制轮转调 `addWalFile` 时, `writeBuffer` 已被 Flush,
理论上没有残留。

**但**: `flushPending` 内部先 `writeBuffer.Write(batchBuf)` 再调
`rotateIfFull`。如果 `rotateIfFull` 触发轮转, 它先 `Flush` 再 `addWalFile`。
这个顺序是对的。但如果 `flushPending` 在 `rotateIfFull` 之后还有代码执行
(有: 丢弃 chunk), 这些不影响 writeBuffer。

**最可疑**: `SyncToDisk` 中 `writeBuffer.Flush()` 之后强制轮转 `addWalFile()`,
`addWalFile` 中 `ws.writeFile.Close()` 关闭的是 **已被 Flush 的文件**。
数据应该在磁盘上。但 Windows 上 `Close()` 是否保证数据落盘需要验证。

### 2. flushPending 中 ent.length 提前更新

```go
for i := range chunk {
    // ...
    ent.length += dataLen  // <-- 在 Write 之前更新
    // ...
}
if len(batchBuf) > 0 {
    ws.writeBuffer.Write(batchBuf)  // <-- 实际写入
}
```

`ent.length` 在 `writeBuffer.Write` **之前** 累加。`rotateIfFull` 用
`ent.length` 判断是否轮转。这意味着 `rotateIfFull` 可能在数据实际写入
writeBuffer 之前就触发轮转。

但由于 `rotateIfFull` 在 `writeBuffer.Write` **之后** 调用 (在 `flushPending`
末尾), 数据已在 writeBuffer 中。`rotateIfFull` 的 `Flush()` 会将其刷盘。
**这个顺序是对的。**

### 3. 推荐验证方法

在 `addWalFile` 中, `Close()` 前加 `Sync()`:

```go
func (ws *walFile) addWalFile() error {
    // ...
    if err := ws.writeBuffer.Flush(); err != nil {
        return err
    }
    if err := ws.writeFile.Sync(); err != nil {
        return err
    }
    err = ws.writeFile.Close()
    // ...
}
```

## 推荐修复方案

### 方案 A: 修复 addWalFile 时序 (最优先尝试)

在 `addWalFile` 关闭旧文件前, 显式 `Flush` + `Sync`:

```go
func (ws *walFile) addWalFile() error {
    tm := time.Now().UnixNano()
    fileName := filepath.Join(ws.filePath, strconv.FormatInt(tm, 10)+".wal")
    file, err := os.OpenFile(fileName, os.O_WRONLY|os.O_APPEND|os.O_CREATE, 0644)
    if err != nil {
        return err
    }
    // 确保旧文件的所有缓冲数据落盘
    if err := ws.writeBuffer.Flush(); err != nil {
        return err
    }
    if err := ws.writeFile.Sync(); err != nil {
        return err
    }
    if err := ws.writeFile.Close(); err != nil {
        return err
    }
    ws.walFiles = append(ws.walFiles, walFileEnty{
        fileName:   fileName,
        length:     0,
        readBuffer: newWalReadBuffer(ws.config.MaxBufferBatchSize),
    })
    ws.writeFile = file
    ws.writeBuffer.Reset(file)
    return nil
}
```

### 方案 B: flush 模式下 forEachCompleteFile 读所有文件

在 `forEachCompleteFile` 中, flush 模式读取 **所有文件** (含 active),
编码后截断 active 文件内容而非删除:

```go
func (ws *walFile) forEachCompleteFile(...) (int, error) {
    ws.mutex.Lock()
    var snapshot []walFileEnty
    if ws.config.diskReads() {
        snapshot = ws.walFiles  // 读所有文件
    } else {
        snapshot = ws.walFiles[:len(ws.walFiles)-1]
    }
    ws.mutex.Unlock()
    // ...
}
```

同时修改 `truncate` 在 flush 模式下截断 active 文件而非仅删除 complete 文件。

### 方案 C: 不丢弃 active chunk

flush 模式下 `flushPending` 不丢弃 active chunk, 保留在 readBuffer 中,
`forEachCompleteFile` 从 readBuffer 读取。代价: 一个 chunk 的额外内存。

## 实现备注

### db.go 需要的改动

```go
type WalConfig struct {
    MaxFileSize        int64  `json:"max_file_size"`
    MaxFileNumber      int    `json:"max_file_number"`
    BufferMode         string `json:"buffer_mode"`  // 替换 CloseBuffer bool
    MaxBufferBatchSize int    `json:"max_buffer_batch_size"`
}

const (
    BufferModeBuffer = "buffer"
    BufferModeFlush  = "flush"
    BufferModeClose  = "close"
)

func (c WalConfig) diskReads() bool {
    return c.BufferMode == BufferModeFlush || c.BufferMode == BufferModeClose
}
func (c WalConfig) directWrite() bool {
    return c.BufferMode == BufferModeClose
}
```

### file_wal.go 需要的改动

1. 接口添加 `diskReads() bool` 和 `SyncToDisk() error`
2. `NewWalFile`: 先初始化 tagMaxTimestamp, 再按 `diskReads()` 丢弃 readBuffer
3. `Write`/`WriteBatch`: `CloseBuffer` -> `directWrite()`
4. `flushPending`: `CloseBuffer` -> `diskReads()`
5. `ReadByTime`: 3 处替换
6. `forEachCompleteFile`: `CloseBuffer` -> `diskReads()`
7. 添加 `SyncToDisk` 方法 (含强制轮转)
8. 添加 `diskReads()` 方法

### table.go 需要的改动

`flushCache` 开头添加:
```go
if s.walFile.diskReads() {
    if err := s.walFile.SyncToDisk(); err != nil {
        return err
    }
}
```

### NewWalFile 重构 (重要)

原代码在丢弃 readBuffer **之后** 才初始化 tagMaxTimestamp, 导致
flush/close 模式下 tagMaxTimestamp 为空。需调整为先初始化再丢弃:

```
原: load -> discard readBuffer -> init tagMaxTimestamp (空!)
新: load -> init tagMaxTimestamp -> discard readBuffer
```
