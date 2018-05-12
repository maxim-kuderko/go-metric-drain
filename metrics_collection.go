package metric_reporter

import (
	"time"
	"sync"
	"github.com/maxim-kuderko/metric-reporter/reporter_drivers"
	"crypto/md5"
	"io"
	"sort"
)

type MetricsCollection struct {
	name       string
	points     [][2]int64
	tags       map[string]string
	hash       string
	interval   float64
	maxMetrics int
	birthTime  time.Time
	driver     reporter_drivers.DriverInterface
	isStub     bool
	count      int64
	sync.Mutex
}

func newMetricsCollection(name string, point int64, tags map[string]string, interval float64, maxMetrics int, driver reporter_drivers.DriverInterface, isStub bool) *MetricsCollection {
	pt := [2]int64{time.Now().UTC().Unix(), point}
	r := MetricsCollection{
		name:       name,
		points:     [][2]int64{pt},
		tags:       tags,
		interval:   interval,
		maxMetrics: maxMetrics,
		birthTime:  time.Now(),
		driver:     driver,
		isStub:     isStub,
	}
	r.calcHash()
	return &r
}

func (mc *MetricsCollection) calcHash() {
	hasher := md5.New()

	io.WriteString(hasher, mc.name)
	if mc.tags != nil {
		d := make([]string, 0, len(mc.tags)*2)
		for k, v := range mc.tags {
			d = append(d, k)
			d = append(d, v)
		}
		sort.Strings(d)
		for _, v := range d {
			io.WriteString(hasher, v)
		}
	}

	mc.hash = string(hasher.Sum(nil))
}

func (mc *MetricsCollection) merge(newMc *MetricsCollection) {
	mc.Lock()
	defer mc.Unlock()
	mc.points = append(mc.points, newMc.points...)
	if len(mc.points) >= mc.maxMetrics {
		mc.flush(false, false)
	}
}

func (mc *MetricsCollection) flushTime() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		mc.flush(true, true)
	}

}

func (mc *MetricsCollection) flush(timer bool, shouldLock bool) {
	if shouldLock{
		mc.Lock()
		defer mc.Unlock()
	}
	if timer && time.Since(mc.birthTime).Seconds() < mc.interval || len(mc.points) == 0 {
		return
	}
	pointsToSend := mc.points
	go func() {
		if !mc.isStub {
			mc.driver.Send(mc.name, pointsToSend, mc.tags)
		}
		pointsToSend = nil
	}()

	mc.points = [][2]int64{}
	mc.birthTime = time.Now()
}
