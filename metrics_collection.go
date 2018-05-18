package metric_reporter

import (
	"time"
	"sync"
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"crypto/md5"
	"io"
	"sort"
	"fmt"
	"strings"
	"encoding/hex"
)

type MetricsCollection struct {
	name       string
	points     [][2]float64
	tags       map[string]string
	hash       string
	interval   int
	maxMetrics int
	birthTime  time.Time
	drivers     []metric_drivers.DriverInterface
	errors     chan error
	count      int64
	sync.Mutex
}

func newMetricsCollection(name string, point float64, tags map[string]string, interval int, maxMetrics int, drivers []metric_drivers.DriverInterface, errors chan error) *MetricsCollection {
	pt := [2]float64{float64(time.Now().UTC().Unix()), point}
	r := MetricsCollection{
		name:       name,
		points:     [][2]float64{pt},
		tags:       tags,
		interval:   interval,
		maxMetrics: maxMetrics,
		birthTime:  time.Now(),
		drivers:     drivers,
		errors:     errors,
	}
	r.calcHash()
	return &r
}

func (mc *MetricsCollection) calcHash() {
	hasher := md5.New()

	io.WriteString(hasher, mc.name)
	if mc.tags != nil {
		d := make([]string, len(mc.tags)*2)
		c := 0
		for k, v := range mc.tags {
			d[c] = k
			c++
			d[c] = v
			c++
		}
		sort.Strings(d)
		io.WriteString(hasher, strings.Join(d, ""))
	}

	mc.hash = hex.EncodeToString(hasher.Sum(nil))
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
	if shouldLock {
		mc.Lock()
		defer mc.Unlock()
	}
	if timer && time.Since(mc.birthTime).Seconds() < float64(mc.interval) || len(mc.points) == 0 {
		return
	}
	pointsToSend := mc.points
	go func(mc *MetricsCollection, pos [][2]float64) {
		defer func() {
			if r := recover(); r != nil {
				fmt.Println("Recovered in f", r)
			}
		}()
		wg := sync.WaitGroup{}
		wg.Add(len(mc.drivers))
		for _, d := range mc.drivers{
			go func(wg sync.WaitGroup,hash string,  name string, pos [][2]float64, tags *map[string]string) {
				defer func() {
					wg.Done()
				}()
				if err := d.Send(hash, name, pos, tags); err != nil{
					mc.errors <- err
				}
			}(wg, mc.hash, mc.name, pos, &mc.tags)
		}

		pos = nil
	}(mc, pointsToSend)

	mc.points = [][2]float64{}
	mc.birthTime = time.Now()
}