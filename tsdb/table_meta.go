package tsdb

import (
	"encoding/json"
	"os"
	"path/filepath"

	"github.com/mababaNiubi/qv-lite/container"
)

type Meta struct {
	MaxPointDict                       tagCode `json:"max_point_dict"`
	Path                               string  `json:"path"`
	container.SyncMap[string, tagCode] `json:"-"`
	PointDict                          map[string]tagCode `json:"point_dict"`
}

func (s *Meta) addTag(tag string) (tagCode, error) {
	s.MaxPointDict += 1
	s.PointDict[tag] = s.MaxPointDict
	// Persist metadata to disk.
	marshal, err := json.Marshal(s)
	if err != nil {
		return 0, err
	}
	create, err := os.Create(s.Path)
	if err != nil {
		return 0, err
	}
	defer create.Close()
	_, err = create.Write(marshal)
	if err != nil {
		return 0, err
	}
	s.Store(tag, s.MaxPointDict)
	return s.MaxPointDict, nil
}

func NewMeta(path string) (*Meta, error) {
	m := &Meta{
		Path:      filepath.Join(path, metaFile),
		PointDict: make(map[string]tagCode),
	}
	fileData, err := os.ReadFile(m.Path)
	if err != nil && !os.IsNotExist(err) {
		return m, err
	}
	if fileData != nil {
		if err = json.Unmarshal(fileData, &m); err != nil {
			return m, err
		}
	}
	for key, tc := range m.PointDict {
		m.Store(key, tc)
	}
	return m, nil
}
