package metric_reporter

import (
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"sync"
	"time"
)

type MetricReporter struct {
	metricDrivers []metric_drivers.DriverInterface
	mMap          map[int64]map[uint64]*MetricsCollection
	resolution    time.Duration
	prefix        string
	passThrough   bool
	baseTags      map[string]string
	errors        chan error

	m  sync.RWMutex
	tf sync.RWMutex
}

func NewMetricsReporter(
	metricDrivers []metric_drivers.DriverInterface,
	resolution time.Duration,
	interval time.Duration, gcFreq time.Duration, prefix string, baseTags map[string]string) (mc *MetricReporter, errors chan error) {
	errors = make(chan error, 1000)
	mc = &MetricReporter{
		metricDrivers: metricDrivers,
		resolution:    resolution,
		mMap:          map[int64]map[uint64]*MetricsCollection{},
		prefix:        prefix,
		baseTags:      baseTags,
		errors:        errors,
	}
	go mc.gc(gcFreq)
	go mc.flushInterval(interval)
	return mc, errors
}

// backward comparability
func (mr *MetricReporter) gc(gcFreq time.Duration) {
	ticker := time.NewTicker(gcFreq)
	for range ticker.C {
		func() {
			mr.m.Lock()
			defer mr.m.Unlock()
			tmp := mr.mMap
			mr.mMap = map[int64]map[uint64]*MetricsCollection{}
			go func() {
				for _, d := range tmp {
					for _, mc := range d {
						mr.flush(mc)
					}
				}
			}()

		}()
	}
}

// backward comparability
func (mr *MetricReporter) flushInterval(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		func() {
			mr.m.RLock()
			defer mr.m.RUnlock()
			for _, d := range mr.mMap {
				for _, mc := range d {
					mr.flush(mc)
				}
			}
		}()
	}

}

// backward comparability
func (mr *MetricReporter) Send(name string, val int64, tags map[string]string, args ...int) {
	mr.Metric(name, float64(val), tags)
}

func (mr *MetricReporter) Metric(name string, val float64, tags map[string]string) {
	metric := NewMetric(name, val, tags)
	t := time.Now()
	tf := t.UnixNano() - (t.UnixNano() % mr.resolution.Nanoseconds())
	v, ok := mr.safeReadM(tf, &metric)
	if !ok {
		v, ok = mr.safeWriteM(tf, &metric)
		if ok {
			return
		} // If !ok then some other thread created the collection in the map, and we need to merge the two
	}
	v.merge(metric)
}

func (mr *MetricReporter) Wait() {
	mr.m.Lock()
	wg := sync.WaitGroup{}
	wg.Add(len(mr.mMap))
	go func() {
		for _, tf := range mr.mMap {
			for _, v := range tf {
				go func(v *MetricsCollection) {
					defer wg.Done()
					mr.flush(v)
				}(v)
			}

		}
	}()

	wg.Wait()
}

func (mr *MetricReporter) flush(mc *MetricsCollection) {
	for _, d := range mr.metricDrivers {
		d.Send(mc.hash, mc.name, mc.points(), mc.tags, mc.timeFrame)
	}
}

func (mr *MetricReporter) addBaseTags(tags map[string]string) map[string]string {

	return tags
}

func (mr *MetricReporter) safeReadM(tf int64, metric *Metric) (*MetricsCollection, bool) {
	mr.m.RLock()
	defer mr.m.RUnlock()
	mr.addTF(tf)
	v, ok := mr.mMap[tf][metric.hash]
	return v, ok
}

// returns true if written return false if other thread written first
func (mr *MetricReporter) safeWriteM(tf int64, metric *Metric) (*MetricsCollection, bool) {
	mr.m.Lock()
	defer mr.m.Unlock()
	v, ok := mr.mMap[tf][metric.hash]
	if ok {
		return v, false
	}
	mc := newMetricsCollection(tf, mr.prefix+"."+metric.name, metric.value, metric.tags, mr.baseTags)
	mr.mMap[tf][metric.hash] = mc
	return mc, true
}

func (mr *MetricReporter) addTF(tf int64) {
	mr.tf.RLock()
	if _, ok := mr.mMap[tf]; ok {
		mr.tf.RUnlock()
		return
	}
	mr.tf.RUnlock()
	mr.tf.Lock()
	defer mr.tf.Unlock()
	mr.mMap[tf] = map[uint64]*MetricsCollection{}
}
