package metric_reporter

import (
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"sync"
	"time"
)

type MetricsCollection struct {
	name            string
	aggregatedPoint metric_drivers.AggregatedPoint
	tags            map[string]string
	hash            uint64
	timeFrame       time.Time
	updatedAt       time.Time
	flushedAt       time.Time
	sync.Mutex
}

func newMetricsCollection(timeFrame int64, name string, value float64, valueTags map[string]string, baseTags map[string]string, hash uint64) *MetricsCollection {
	tags := map[string]string{}
	for k, v := range baseTags {
		tags[k] = v
	}
	for k, v := range valueTags {
		tags[k] = v
	}
	r := MetricsCollection{
		timeFrame: time.Unix(0, timeFrame),
		name:      name,
		tags:      tags,
		aggregatedPoint: metric_drivers.AggregatedPoint{
			Min: value,
			Max: value,
		},
		flushedAt: time.Now(),
		hash:      hash,
	}
	return &r
}

func (mc *MetricsCollection) merge(metric Metric) {
	mc.Lock()
	defer mc.Unlock()

	mc.aggregatedPoint.Sum += metric.value
	mc.aggregatedPoint.Count += 1
	mc.aggregatedPoint.Last = metric.value
	if mc.aggregatedPoint.Min > metric.value {
		mc.aggregatedPoint.Min = metric.value
	}
	if mc.aggregatedPoint.Max < metric.value {
		mc.aggregatedPoint.Max = metric.value
	}

	mc.updatedAt = time.Now()
}

func (mc *MetricsCollection) points() metric_drivers.AggregatedPoint {
	mc.Lock()
	defer mc.Unlock()
	return mc.aggregatedPoint
}

func (mc *MetricsCollection) lastUpdated() time.Time {
	mc.Lock()
	defer mc.Unlock()
	return mc.updatedAt
}

func (mc *MetricsCollection) lastFlushed() time.Time {
	mc.Lock()
	defer mc.Unlock()
	return mc.flushedAt
}

func (mc *MetricsCollection) resetFlushed() {
	mc.Lock()
	defer mc.Unlock()
	mc.flushedAt = time.Now()
}
