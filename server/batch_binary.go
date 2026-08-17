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
	for i := 0; i < count; i++ {
		p, err := readBinaryPoint(br, vt)
		if err != nil {
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

// readBinaryPoint 流式读取一个点（tag / 时间戳 / 值）。
func readBinaryPoint(br *bufio.Reader, vt byte) (tsdb.TagPoint, error) {
	var lenBuf [2]byte
	if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
		return tsdb.TagPoint{}, errors.New("truncated tag length")
	}
	tl := int(binary.BigEndian.Uint16(lenBuf[:]))
	tb := make([]byte, tl)
	if _, err := io.ReadFull(br, tb); err != nil {
		return tsdb.TagPoint{}, errors.New("truncated tag")
	}
	var ts [8]byte
	if _, err := io.ReadFull(br, ts[:]); err != nil {
		return tsdb.TagPoint{}, errors.New("truncated timestamp")
	}

	var v variant.Variant
	switch vt {
	case batchValueFloat:
		var buf [8]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated float value")
		}
		v = variant.NewFloat64(math.Float64frombits(binary.BigEndian.Uint64(buf[:])))
	case batchValueInt:
		var buf [8]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated int value")
		}
		v = variant.NewInt64(int64(binary.BigEndian.Uint64(buf[:])))
	case batchValueUint:
		var buf [8]byte
		if _, err := io.ReadFull(br, buf[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated uint value")
		}
		v = variant.NewUInt64(binary.BigEndian.Uint64(buf[:]))
	case batchValueBool:
		var b [1]byte
		if _, err := io.ReadFull(br, b[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated bool value")
		}
		v = variant.NewBool(b[0] != 0)
	case batchValueString:
		var lenBuf [2]byte
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated string length")
		}
		slen := int(binary.BigEndian.Uint16(lenBuf[:]))
		sb := make([]byte, slen)
		if _, err := io.ReadFull(br, sb); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated string value")
		}
		v = variant.NewString(string(sb))
	case batchValueJSON:
		var lenBuf [4]byte
		if _, err := io.ReadFull(br, lenBuf[:]); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated json length")
		}
		jlen := int(binary.BigEndian.Uint32(lenBuf[:]))
		jb := make([]byte, jlen)
		if _, err := io.ReadFull(br, jb); err != nil {
			return tsdb.TagPoint{}, errors.New("truncated json value")
		}
		vv, err := nativeToVariant(jb)
		if err != nil {
			return tsdb.TagPoint{}, err
		}
		v = vv
	default:
		return tsdb.TagPoint{}, fmt.Errorf("unsupported value type %d", vt)
	}
	return tsdb.TagPoint{
		Tag:       string(tb),
		Timestamp: int64(binary.BigEndian.Uint64(ts[:])),
		Value:     v,
	}, nil
}
