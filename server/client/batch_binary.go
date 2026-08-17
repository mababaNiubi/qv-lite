package client

import (
	"encoding/json"
	"errors"
	"math"
	"strconv"
)

// Binary batch protocol, mirroring the server's "application/x-tsdb-batch"
// format (version 1). See server/batch_binary.go for the layout spec.
const (
	batchContentType = "application/x-tsdb-batch"

	batchMagic0  = 0x54
	batchMagic1  = 0x53
	batchVersion = 1

	byteFloat  = 1
	byteInt    = 2
	byteUint   = 3
	byteBool   = 4
	byteString = 5
	byteJSON   = 6

	maxBatchPoints = 2_000_000
)

// encodeBatchBinary encodes points into the binary batch format. ok=false
// means the batch is not binary-compatible (mixed types / unsupported value)
// and the caller should fall back to JSON. A non-nil error is fatal.
func encodeBatchBinary(table string, points []TagPoint) (body []byte, ok bool, err error) {
	if len(points) > maxBatchPoints {
		return nil, false, errors.New("client: batch too large (max 2000000 points)")
	}
	vt := points[0].Value.Type
	if vt == "" || vt == "empty" {
		return nil, false, nil // empty values have no payload form
	}
	typ, ok := binaryValueType(vt)
	if !ok {
		return nil, false, nil // unknown type → JSON
	}
	for _, p := range points {
		if p.Value.Type != vt {
			return nil, false, nil // mixed types → JSON fallback
		}
	}

	// Header: magic(2) version(1) type(1) tableLen(2) table count(4)
	size := 10 + len(table) + 4
	for _, p := range points {
		size += 2 + len(p.Tag) + 8 + valuePayloadSize(typ, p.Value)
	}
	buf := make([]byte, 0, size)
	buf = append(buf, batchMagic0, batchMagic1, batchVersion, typ)
	buf = appendUint16(buf, uint16(len(table)))
	buf = append(buf, table...)
	buf = appendUint32(buf, uint32(len(points)))

	for i := range points {
		p := &points[i]
		buf = appendUint16(buf, uint16(len(p.Tag)))
		buf = append(buf, p.Tag...)
		buf = appendInt64(buf, p.Timestamp)
		buf, err = appendValuePayload(buf, typ, p.Value)
		if err != nil {
			return nil, false, err
		}
	}
	return buf, true, nil
}

func binaryValueType(t string) (byte, bool) {
	switch t {
	case "float":
		return byteFloat, true
	case "int":
		return byteInt, true
	case "uint":
		return byteUint, true
	case "bool":
		return byteBool, true
	case "string":
		return byteString, true
	case "json":
		return byteJSON, true
	default:
		return 0, false
	}
}

func valuePayloadSize(typ byte, v Value) int {
	switch typ {
	case byteFloat, byteInt, byteUint:
		return 8
	case byteBool:
		return 1
	case byteString:
		return 2 + len(v.Raw)
	case byteJSON:
		return 4 + len(v.Raw)
	default:
		return 0
	}
}

func appendValuePayload(buf []byte, typ byte, v Value) ([]byte, error) {
	switch typ {
	case byteFloat:
		f, err := parseFloat(v.Raw)
		if err != nil {
			return buf, err
		}
		return appendUint64(buf, math.Float64bits(f)), nil
	case byteInt:
		i, err := parseInt(v.Raw)
		if err != nil {
			return buf, err
		}
		return appendInt64(buf, i), nil
	case byteUint:
		u, err := parseUint(v.Raw)
		if err != nil {
			return buf, err
		}
		return appendUint64(buf, u), nil
	case byteBool:
		b, err := parseBool(v.Raw)
		if err != nil {
			return buf, err
		}
		if b {
			return append(buf, 1), nil
		}
		return append(buf, 0), nil
	case byteString:
		s, err := parseString(v.Raw)
		if err != nil {
			return buf, err
		}
		buf = appendUint16(buf, uint16(len(s)))
		return append(buf, s...), nil
	case byteJSON:
		buf = appendUint32(buf, uint32(len(v.Raw)))
		return append(buf, v.Raw...), nil
	default:
		return buf, errors.New("client: unsupported binary value type")
	}
}

// --- raw JSON scalar parsing (both bare and quoted forms) ---

func parseFloat(raw json.RawMessage) (float64, error) {
	s := string(raw)
	if len(s) > 0 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return 0, err
		}
		s = str
	}
	return strconv.ParseFloat(s, 64)
}

func parseInt(raw json.RawMessage) (int64, error) {
	s := string(raw)
	if len(s) > 0 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return 0, err
		}
		s = str
	}
	return strconv.ParseInt(s, 10, 64)
}

func parseUint(raw json.RawMessage) (uint64, error) {
	s := string(raw)
	if len(s) > 0 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return 0, err
		}
		s = str
	}
	return strconv.ParseUint(s, 10, 64)
}

func parseBool(raw json.RawMessage) (bool, error) {
	s := string(raw)
	if len(s) > 0 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return false, err
		}
		s = str
	}
	return strconv.ParseBool(s)
}

func parseString(raw json.RawMessage) (string, error) {
	s := string(raw)
	if len(s) > 0 && s[0] == '"' {
		var str string
		if err := json.Unmarshal(raw, &str); err != nil {
			return "", err
		}
		return str, nil
	}
	return s, nil
}

// --- big-endian writers ---

func appendUint16(buf []byte, v uint16) []byte {
	return append(buf, byte(v>>8), byte(v))
}

func appendUint32(buf []byte, v uint32) []byte {
	return append(buf, byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendUint64(buf []byte, v uint64) []byte {
	return append(buf, byte(v>>56), byte(v>>48), byte(v>>40), byte(v>>32),
		byte(v>>24), byte(v>>16), byte(v>>8), byte(v))
}

func appendInt64(buf []byte, v int64) []byte {
	return appendUint64(buf, uint64(v))
}
