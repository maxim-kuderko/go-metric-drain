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
	cq         chan *MetricsCollection
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
		cq:         make(chan *MetricsCollection, 500),
	}
	go func() { mc.addCollections() }()
	return mc
}

func (mr *MetricReporter) Send(name string, val int64, tags map[string]string) {
	metric := newMetricsCollection(mr.prefix+"."+name, val, tags, mr.interval, mr.maxMetrics, mr.driver, mr.isStub)
	mr.RWMutex.RLock()
	defer mr.RUnlock()
	v, ok := mr.metricsMap[metric.hash]
	if !ok {
		go func() {
			mr.cq <- metric
		}()
		return
	}
	v.merge(metric)
}

func (mr *MetricReporter) addCollections() {
	for {
		mc := <-mr.cq
		func() {
			mr.RWMutex.Lock()
			defer mr.RWMutex.Unlock()
			v, ok := mr.metricsMap[mc.hash]
			if !ok {
				mr.metricsMap[mc.hash] = mc
				go func() { mc.flushTime() }()
				return
			}
			go func(){
				v.merge(mc)
			}()
		}()

	}
}

func (mr *MetricReporter) Wait() bool {
	wg := sync.WaitGroup{}
	for _, v := range mr.metricsMap {
		wg.Add(1)
		go func() {
			v.flush()
			wg.Done()
		}()
	}
	wg.Wait()
	return true
}
