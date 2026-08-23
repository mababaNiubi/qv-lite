package server

import (
	"bufio"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"

	"github.com/mababaNiubi/variant"

	"github.com/mababaNiubi/qv-lite/tsdb"
)

// Binary batch wire format ("application/x-tsdb-batch", version 1).
//
// Fixed layout, big-endian, zero-copy parseable:
//
//	[0:2]   magic 0x5453 ("TS")
//	[2]     version = 1
//	[3]     value type (batchValue* below) — homogeneous batch
//	[4:6]   table name length (uint16)
//	[6:6+n] table name (UTF-8; may be empty → default table)
//	[6+n:10+n] point count (uint32)
//	then count × point:
//	  [0:2]   tag length (uint16)
//	  [2:2+n] tag (UTF-8)
//	  [2+n:10+n] timestamp (int64, big-endian)
//	  [10+n:…]  value payload per value type:
//	    float64: 8 bytes (IEEE-754 bits)
//	    int64:   8 bytes (two's complement)
//	    uint64:  8 bytes
//	    bool:    1 byte (0/1)
//	    string:  uint16 len + UTF-8 bytes
//	    json:    uint32 len + JSON bytes (parsed as variant structure)
//
// Homogeneous value type keeps parsing branch-free in the hot loop and makes
// the payload fixed-size for the numeric cases (the common telemetry path).
const (
	batchMagic0    = 0x54
	batchMagic1    = 0x53
	batchVersion   = 1
	batchHeaderLen = 6 // magic(2)+version(1)+valueType(1)+tableLen(2)，后接 table 与 count

	batchValueFloat  = 1
	batchValueInt    = 2
	batchValueUint   = 3
	batchValueBool   = 4
	batchValueString = 5
	batchValueJSON   = 6

	// maxBatchPoints caps a single request to bound memory usage.
	maxBatchPoints = 2_000_000
)

const BatchContentType = "application/x-tsdb-batch"

// handleBatchBinary serves the binary batch write endpoint. It is the
// high-throughput path: manual parsing, no reflection, no per-point
// allocation for numeric values. 流式处理：边读边逐点解析，攒满一批立即
// 入库——无论请求多大，内存恒定、传输与写入并行、引擎每次锁时长短。
func (s *Server) handleBatchBinary(w http.ResponseWriter, r *http.Request) {
	br := bufio.NewReader(http.MaxBytesReader(w, r.Body, s.cfg.MaxBodyBytes))
	table, vt, count, err := readBatchHeader(br)
	if err != nil {
		writeErr(w, http.StatusBadRequest, err)
		return
	}
	if count == 0 {
		writeJSON(w, http.StatusOK, map[string]int{"written": 0})
		return
	}
	g := s.newStreamIngestor()
	g.firstHint = min(count, streamBatchSize) // 头部已知点数：预分配首个表缓冲
	blk := newBlockReader(br)                 // 批量读缓冲：每点不再单独 ReadFull
	var p tsdb.TagPoint
	for i := 0; i < count; i++ {
		if err := parseBinaryPoint(blk, vt, g, &p); err != nil {
			writeErr(w, http.StatusBadRequest, fmt.Errorf("batch: point %d: %w", i, err))
			return
		}
		if err := g.Add(table, p); err != nil {
			writeErr(w, http.StatusInternalServerError, err)
			return
		}
	}
	written, err := g.Finish()
	if err != nil {
		writeErr(w, http.StatusInternalServerError, err)
		return
	}
	writeJSON(w, http.StatusOK, map[string]int{"written": written})
}

// readBatchHeader 读取二进制批量请求头，返回 (表名, 值类型, 点数)。
func readBatchHeader(br *bufio.Reader) (string, byte, int, error) {
	var hdr [batchHeaderLen]byte
	if _, err := io.ReadFull(br, hdr[:]); err != nil {
		return "", 0, 0, errors.New("batch: body too short")
	}
	if hdr[0] != batchMagic0 || hdr[1] != batchMagic1 {
		return "", 0, 0, errors.New("batch: bad magic")
	}
	if hdr[2] != batchVersion {
		return "", 0, 0, fmt.Errorf("batch: unsupported version %d", hdr[2])
	}
	vt := hdr[3]
	tl := int(binary.BigEndian.Uint16(hdr[4:6]))
	tb := make([]byte, tl)
	if _, err := io.ReadFull(br, tb); err != nil {
		return "", 0, 0, errors.New("batch: truncated table name")
	}
	var cnt [4]byte
	if _, err := io.ReadFull(br, cnt[:]); err != nil {
		return "", 0, 0, errors.New("batch: truncated count")
	}
	count := int(binary.BigEndian.Uint32(cnt[:]))
	if count > maxBatchPoints {
		return "", 0, 0, fmt.Errorf("batch: too many points (%d)", count)
	}
	return string(tb), vt, count, nil
}

// binaryBlockSize 是二进制批量路径的块读缓冲初始大小。逐块读入 + 内存内
// 切片解析，把每点 4 次 io.ReadFull（接口分发/系统调用）摊薄到每块 1 次。
const binaryBlockSize = 64 * 1024

// blockReader 从底层 bufio.Reader 批量读入复用缓冲，take(n) 保证连续 n
// 字节可用并推进游标。跨块边界自动搬移剩余数据；超大点（超长 tag/string/
// json）按需扩容。
type blockReader struct {
	br  *bufio.Reader
	buf []byte
	pos int // 已消费位置
	end int // 有效数据末尾
}

func newBlockReader(br *bufio.Reader) *blockReader {
	return &blockReader{br: br, buf: make([]byte, binaryBlockSize)}
}

// take 返回并消费接下来的 n 字节（必要时触发批量读）。
func (r *blockReader) take(n int) ([]byte, error) {
	if r.end-r.pos < n {
		if err := r.refill(n); err != nil {
			return nil, err
		}
	}
	b := r.buf[r.pos : r.pos+n]
	r.pos += n
	return b, nil
}

// refill 把剩余数据搬到缓冲头部并尽量读满 need 字节。
func (r *blockReader) refill(need int) error {
	left := r.end - r.pos
	if need > len(r.buf) {
		// 罕见超大点（超长 tag/string/json）：按需扩容。
		nb := make([]byte, need)
		copy(nb, r.buf[r.pos:r.end])
		r.buf = nb
	} else {
		copy(r.buf, r.buf[r.pos:r.end])
	}
	r.end = left
	r.pos = 0
	for r.end < need {
		m, err := r.br.Read(r.buf[r.end:])
		r.end += m
		if err != nil {
			// TCP 末段常把剩余数据与 io.EOF 一起返回：先看数据是否已够，
			// 够则视为正常结束，否则才是真的截断。
			if errors.Is(err, io.EOF) && r.end >= need {
				return nil
			}
			if errors.Is(err, io.EOF) {
				return errors.New("truncated binary point")
			}
			return err
		}
	}
	return nil
}

// parseBinaryPoint 从块缓冲解析一个点并填充到 *out。
// 值类型 vt 在头部声明，同批同型。tag 走零拷贝驻留（命中不分配）。
func parseBinaryPoint(r *blockReader, vt byte, g *StreamIngestor, out *tsdb.TagPoint) error {
	h, err := r.take(2)
	if err != nil {
		return err
	}
	tl := int(binary.BigEndian.Uint16(h))
	tb, err := r.take(tl)
	if err != nil {
		return err
	}
	out.Tag = g.internBytes(tb) // 零拷贝驻留：重复 tag 不再每点分配字符串

	h, err = r.take(8)
	if err != nil {
		return err
	}
	out.Timestamp = int64(binary.BigEndian.Uint64(h))

	switch vt {
	case batchValueFloat:
		h, err := r.take(8)
		if err != nil {
			return err
		}
		out.Value = variant.NewFloat64(math.Float64frombits(binary.BigEndian.Uint64(h)))
	case batchValueInt:
		h, err := r.take(8)
		if err != nil {
			return err
		}
		out.Value = variant.NewInt64(int64(binary.BigEndian.Uint64(h)))
	case batchValueUint:
		h, err := r.take(8)
		if err != nil {
			return err
		}
		out.Value = variant.NewUInt64(binary.BigEndian.Uint64(h))
	case batchValueBool:
		h, err := r.take(1)
		if err != nil {
			return err
		}
		out.Value = variant.NewBool(h[0] != 0)
	case batchValueString:
		h, err := r.take(2)
		if err != nil {
			return err
		}
		slen := int(binary.BigEndian.Uint16(h))
		sb, err := r.take(slen)
		if err != nil {
			return err
		}
		out.Value = variant.NewString(string(sb))
	case batchValueJSON:
		h, err := r.take(4)
		if err != nil {
			return err
		}
		jlen := int(binary.BigEndian.Uint32(h))
		jb, err := r.take(jlen)
		if err != nil {
			return err
		}
		vv, err := nativeToVariant(jb)
		if err != nil {
			return err
		}
		out.Value = vv
	default:
		return fmt.Errorf("unsupported value type %d", vt)
	}
	return nil
}
