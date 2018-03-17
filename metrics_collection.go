package metric_reporter

import (
	"time"
	"sync"
	"github.com/maxim-kuderko/metric-reporter/reporter_drivers"
	"crypto/md5"
	"io"
	"encoding/hex"
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
	sync.Mutex
}

func newMetricsCollection(name string, point int64, tags map[string]string, interval float64, maxMetrics int, driver reporter_drivers.DriverInterface) *MetricsCollection {
	pt := [2]int64{time.Now().Unix(), point}
	r := MetricsCollection{
		name:       name,
		points:     [][2]int64{pt},
		tags:       tags,
		interval:   interval,
		maxMetrics: maxMetrics,
		birthTime:  time.Now(),
		driver:     driver,
	}
	r.calcHash()
	return &r
}

func (mc *MetricsCollection) calcHash() {
	hasher := md5.New()
	io.WriteString(hasher, mc.name)
	d := make([]string, 0, len(mc.tags)*2)
	for k, v := range mc.tags {
		d = append(d, k)
		d = append(d, v)
	}
	sort.Strings(d)
	for _, v := range d {
		io.WriteString(hasher, v)
	}
	mc.hash = hex.EncodeToString(hasher.Sum(nil))
}

func (mc *MetricsCollection) merge(newMc *MetricsCollection) {
	mc.Lock()
	mc.points = append(mc.points, newMc.points...)
	if len(mc.points) >= mc.maxMetrics {
		mc.Unlock()
		mc.flush()
	} else {
		mc.Unlock()
	}
}

func (mc *MetricsCollection) flushTime() {
	for {
		<-time.After(time.Second)
		func() {
			mc.Lock()
			if time.Since(mc.birthTime).Seconds() > mc.interval && len(mc.points) != 0 {
				mc.Unlock()
				mc.flush()
			} else {
				mc.Unlock()
			}
		}()
	}
}

func (mc *MetricsCollection) flush() {
	mc.Lock()
	defer mc.Unlock()

	pointsToSend := mc.points
	go func() {
		mc.driver.Send(mc.name, pointsToSend, mc.tags)
	}()

	mc.points = [][2]int64{}
	mc.birthTime = time.Now()
}
