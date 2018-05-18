package metric_reporter

import (
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"sync"
	"bytes"
)

type MetricReporter struct {
	metricDrivers  []metric_drivers.DriverInterface
	counterDrivers []metric_drivers.DriverInterface
	mMap           map[string]*MetricsCollection
	cMap           map[string]*MetricsCollection
	open           bool
	interval       int
	maxMetrics     int
	prefix         string
	errors         chan error
	m              sync.RWMutex
	c              sync.RWMutex
}

func NewMetricsReporter(
	metricDrivers []metric_drivers.DriverInterface,
	counterDrivers []metric_drivers.DriverInterface,
	interval int, maxMetrics int, prefix string) (mc *MetricReporter, errors chan error) {
	errors = make(chan error, 1000)
	mc = &MetricReporter{
		metricDrivers:  metricDrivers,
		counterDrivers: counterDrivers,
		mMap:           map[string]*MetricsCollection{},
		cMap:           map[string]*MetricsCollection{},
		interval:       interval,
		maxMetrics:     maxMetrics,
		prefix:         prefix,
		errors:         errors,
	}
	return mc, errors
}

// backward comparability
func (mr *MetricReporter) Send(name string, val int64, tags map[string]string, args ...int) {
	mr.Metric(name, float64(val), tags, args...)
}

func (mr *MetricReporter) Metric(name string, val float64, tags map[string]string, args ...int) {
	interval, maxMetrics := mr.getCollectionParams(args...)
	metric := newMetricsCollection(mr.fullName(name), val, tags, interval, maxMetrics, mr.metricDrivers, mr.errors)
	v, ok := mr.safeReadM(metric)
	if !ok {
		v, ok = mr.safeWriteM(metric)
		if ok {
			return
		} // If !ok then some other thread created the collection in the map, and we need to merge the two
	}
	v.merge(metric)
}

func (mr *MetricReporter) Count(name string, val float64, tags map[string]string, args ...int) {
	interval, maxMetrics := mr.getCollectionParams(args...)
	metric := newMetricsCollection(mr.fullName(name), val, tags, interval, maxMetrics, mr.counterDrivers, mr.errors)
	v, ok := mr.safeReadC(metric)
	if !ok {
		v, ok = mr.safeWriteC(metric)
		if ok {
			return
		} // If !ok then some other thread created the collection in the map, and we need to merge the two
	}
	v.merge(metric)
}

func (mr *MetricReporter) Wait() {
	mr.m.Lock()
	//defer mr.m.Unlock()
	mr.c.Lock()
	//defer mr.c.Unlock()
	wg := sync.WaitGroup{}
	wg.Add(len(mr.mMap) + len(mr.cMap))
	go func() {
		for _, v := range mr.mMap {
			go func(v *MetricsCollection) {
				defer func() {
					wg.Done()
				}()
				v.flush(false, true, true)
			}(v)
		}
	}()


	go func() {
		for _, v := range mr.cMap {
			go func(v *MetricsCollection) {
				defer func() {
					wg.Done()
				}()
				v.flush(false, true, true)
			}(v)
		}
	}()


	wg.Wait()
}

func (mr *MetricReporter) safeReadM(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.m.RLock()
	defer mr.m.RUnlock()
	v, ok := mr.mMap[metric.hash]
	return v, ok
}

// returns true if written return false if other thread written first
func (mr *MetricReporter) safeWriteM(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.m.Lock()
	defer mr.m.Unlock()
	v, ok := mr.mMap[metric.hash]
	if ok {
		return v, false
	}
	mr.mMap[metric.hash] = metric
	go metric.flushTime()

	return metric, true
}

func (mr *MetricReporter) safeReadC(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.c.RLock()
	defer mr.c.RUnlock()
	v, ok := mr.cMap[metric.hash]
	return v, ok
}

// returns true if written return false if other thread written first
func (mr *MetricReporter) safeWriteC(metric *MetricsCollection) (*MetricsCollection, bool) {
	mr.c.Lock()
	defer mr.c.Unlock()
	v, ok := mr.cMap[metric.hash]
	if ok {
		return v, false
	}
	mr.cMap[metric.hash] = metric
	go metric.flushTime()

	return metric, true
}

func (mr *MetricReporter) fullName(name string) string {
	buf := bytes.Buffer{}
	buf.WriteString(mr.prefix)
	buf.WriteString(".")
	buf.WriteString(name)
	return buf.String()
}

func (mr *MetricReporter) getCollectionParams(args ...int) (interval, maxMetrics int) {
	interval = mr.interval
	maxMetrics = mr.maxMetrics
	if len(args) > 0 {
		interval = args[0]
		if len(args) > 1 {
			maxMetrics = args[1]
		}
	}
	return interval, maxMetrics
}
