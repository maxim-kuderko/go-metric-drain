package metric_reporter

import (
	"encoding/hex"
	"github.com/cespare/xxhash"
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"io"
	"sort"
	"strings"
	"sync"
	"time"
)

type MetricsCollection struct {
	name       string
	points     []metric_drivers.PtDataer
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
	r := MetricsCollection{
		name:       name,
		points:     []metric_drivers.PtDataer{metric_drivers.NewPoint(time.Now(), point)},
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
	hasher := xxhash.New()

	io.WriteString(hasher, mc.name)
	if mc.tags != nil {
		d := make([]string, 0, len(mc.tags))
		for _, v := range mc.tags {
			d = append(d,v)
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

		mc.flush(false, false, false)
	}
}

func (mc *MetricsCollection) flushTime() {
	ticker := time.NewTicker(time.Second)
	for {
		<-ticker.C
		mc.flush(true, true, false)
	}

}

func (mc *MetricsCollection) flush(timer bool, shouldLock bool, shouldWait bool){
	if shouldLock {
		mc.Lock()
		defer mc.Unlock()
	}
	if len(mc.points) == 0 || (timer && time.Since(mc.birthTime).Seconds() < float64(mc.interval)) {
		return
	}
	w := sync.WaitGroup{}
	w.Add(len(mc.drivers))

	for _, d := range mc.drivers{
		go func(d metric_drivers.DriverInterface, pos []metric_drivers.PtDataer,) {
			defer w.Done()
			if err := d.Send(mc.hash, mc.name, pos, &mc.tags); err != nil{
				mc.errors <- err
			}
		}(d, mc.points)
	}


	mc.points = make([]metric_drivers.PtDataer,0,mc.maxMetrics)
	mc.birthTime = time.Now()
	if shouldWait{
		w.Wait()
	}
}