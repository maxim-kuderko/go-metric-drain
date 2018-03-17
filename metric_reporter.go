package metric_reporter

import (
	"github.com/maxim-kuderko/metric-reporter/reporter_drivers"
	"sync"
	"time"
)

type MetricReporter struct {
	driver     reporter_drivers.DriverInterface
	queue      chan *MetricsCollection
	metricsMap map[string]*MetricsCollection
	open       bool
	interval   float64
	maxMetrics int
	done chan bool
}

func NewMetricsReporter(driver reporter_drivers.DriverInterface, interval float64, maxMetrics int, done chan bool) *MetricReporter {
	mc := &MetricReporter{
		driver:     driver,
		queue:      make(chan *MetricsCollection, 9999),
		metricsMap: map[string]*MetricsCollection{},
		open:       true,
		interval:   interval,
		maxMetrics: maxMetrics,
		done: make(chan bool,1),
	}
	go func() { mc.insert(done) }()
	return mc
}

func (mr *MetricReporter) Send(name string, val int64, tags map[string]string) {
	if mr.open {
		mr.queue <- newMetricsCollection(name, val, tags, mr.interval, mr.maxMetrics, mr.driver)
	}
}

func (mr *MetricReporter) Wait() bool {
	return <- mr.done
}

func (mr *MetricReporter) insert(done chan bool) {
	wg := sync.WaitGroup{}
	wg.Add(1)
	go func() {
		<-done
		mr.shutdown()
		wg.Done()
	}()
	// todo: add semaphore
	for {
		mc, more := <-mr.queue
		if !more {
			break
		}
		v, ok := mr.metricsMap[mc.hash]
		if !ok {
			mr.metricsMap[mc.hash] = mc
			go func(){mc.flushTime()}()
			continue
		}
		wg.Add(1)
		go func() {
			v.merge(mc)
			mc = nil
			wg.Done()
		}()
	}
	wg.Wait()
	mr.done <- true
}

func (mr *MetricReporter) shutdown() {
	mr.open = false
	time.Sleep(time.Millisecond * 100)
	close(mr.queue)
	for _, v := range mr.metricsMap{
		v.flush()
	}
	mr.done <-true
}
