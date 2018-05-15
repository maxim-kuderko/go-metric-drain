package metric_reporter

import (
	"github.com/maxim-kuderko/metric-reporter/reporter_drivers"
	"sync"
)

type MetricReporter struct {
	driver     reporter_drivers.DriverInterface
	metricsMap map[string]*MetricsCollection
	open       bool
	interval   float64
	maxMetrics int
	prefix     string
	isStub     bool
	sync.RWMutex
}

func NewMetricsReporter(driver reporter_drivers.DriverInterface, interval float64, maxMetrics int, prefix string, isStub bool) *MetricReporter {
	mc := &MetricReporter{
		driver:     driver,
		metricsMap: map[string]*MetricsCollection{},
		interval:   interval,
		maxMetrics: maxMetrics,
		prefix:     prefix,
		isStub:     isStub,
	}
	return mc
}

func (mr *MetricReporter) Send(name string, val int64, tags map[string]string) {
	metric := newMetricsCollection(mr.prefix+"."+name, val, tags, mr.interval, mr.maxMetrics, mr.driver, mr.isStub)
	v, ok := mr.safeRead(metric)
	if !ok {
		v, ok = mr.safeWrite(metric)
		if ok{
			return
		}
		// If !ok then some other thread created the collection in the map, and we need to merge the two
	}
	v.merge(metric)
}

func (mr *MetricReporter) Wait() {
	wg := sync.WaitGroup{}
	mr.RLock()
	defer mr.RUnlock()
	for _, v := range mr.metricsMap {
		wg.Add(1)
		go func(v *MetricsCollection) {
			v.flush(false,true)
			wg.Done()
		}(v)
	}
	wg.Wait()
}

func (mr *MetricReporter) safeRead(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.RWMutex.RLock()
	defer mr.RUnlock()
	v, ok := mr.metricsMap[metric.hash]
	return v, ok
}

// returns true if written return false if other thread written first
func (mr *MetricReporter) safeWrite(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.RLock()
	v, ok := mr.metricsMap[metric.hash]
	mr.RUnlock()
	if ok {
		return v, false
	}
	mr.Lock()
	mr.metricsMap[metric.hash] = metric
	mr.Unlock()
	go func(metric *MetricsCollection) { metric.flushTime() }(metric)

	return metric, true

}