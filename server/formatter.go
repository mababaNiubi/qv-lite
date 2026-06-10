package server

import (
	"encoding/json"
	"qvdb/tsdb"
)

type pointJSON struct {
	Time  int64       `json:"time"`
	Value interface{} `json:"value"`
}

func FormatPoints(points []tsdb.Point) []pointJSON {
	result := make([]pointJSON, 0, len(points))
	for _, p := range points {
		result = append(result, pointJSON{
			Time:  p.Tms,
			Value: p.V.AsInterface(),
		})
	}
	return result
}

func FormatResultJSON(result *Result) string {
	data, err := json.Marshal(result)
	if err != nil {
		return `{"success":false,"message":"json marshal error"}`
	}
	return string(data)
}
