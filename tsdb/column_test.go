package tsdb

import (
	"testing"
	"time"
)

func TestSegmentTimeExceeded(t *testing.T) {
	hour := int64(time.Hour)
	tests := []struct {
		name     string
		minTime  int64
		maxTime  int64
		interval int64
		want     bool
	}{
		{name: "zero means unlimited", minTime: 0, maxTime: 24 * hour, interval: 0, want: false},
		{name: "below interval", minTime: hour, maxTime: 2*hour - 1, interval: hour, want: false},
		{name: "at interval", minTime: hour, maxTime: 2 * hour, interval: hour, want: true},
		{name: "above interval", minTime: hour, maxTime: 3 * hour, interval: hour, want: true},
		{name: "range crosses zero", minTime: -hour, maxTime: hour, interval: 2 * hour, want: true},
		{name: "reversed range", minTime: 2 * hour, maxTime: hour, interval: hour, want: false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			if got := segmentTimeExceeded(tt.minTime, tt.maxTime, tt.interval); got != tt.want {
				t.Fatalf("segmentTimeExceeded(%d, %d, %d) = %v, want %v", tt.minTime, tt.maxTime, tt.interval, got, tt.want)
			}
		})
	}
}

func TestNewSSColumnKeepsUnlimitedSegmentInterval(t *testing.T) {
	column := newSSColumn(1, nil, 0, 0)
	if column.maxSegmentTimeInterval != 0 {
		t.Fatalf("maxSegmentTimeInterval = %d, want 0 (unlimited)", column.maxSegmentTimeInterval)
	}
}
