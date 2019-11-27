package metric_drivers

import (
	"context"
	influxdb "github.com/influxdata/influxdb-client-go"
	"log"
	"math/rand"
	"net/http"
	"sync"
	"time"
)

type InfluxDB struct {
	bucket, org string
	influx      *influxdb.Client
	maxSize     int
	cache       []pt
	lastSend    time.Time
	s           sync.Mutex
}

func NewInfluxDB(url, token, bucket, org string, flushInterval time.Duration, maxSize int) *InfluxDB {
	influx, err := influxdb.New(url, token, influxdb.WithHTTPClient(&http.Client{
		Timeout:   time.Second * 10,
		Transport: http.DefaultTransport,
	}))
	if err != nil {
		log.Panic(err)
	}
	ifdb := &InfluxDB{influx: influx, bucket: bucket, org: org, cache: make([]pt, 0, maxSize), maxSize: maxSize, lastSend: time.Now()}
	go ifdb.flushInterval(flushInterval)
	return ifdb
}

type pt struct {
	name  string
	point AggregatedPoint
	tags  map[string]string
	time  time.Time
}

func (ifdb *InfluxDB) flushInterval(interval time.Duration) {
	ticker := time.NewTicker(interval)
	for range ticker.C {
		ifdb.s.Lock()
		if len(ifdb.cache) > 0 && ifdb.lastSend.Add(interval).Before(time.Now()) {
			ifdb.flush()
		}
		ifdb.s.Unlock()
	}
}

func (ifdb *InfluxDB) Send(key uint64, name string, Point AggregatedPoint, tags map[string]string, t time.Time) error {
	ifdb.s.Lock()
	defer ifdb.s.Unlock()
	ifdb.cache = append(ifdb.cache, pt{
		name:  name,
		point: Point,
		tags:  tags,
		time:  t,
	})

	if len(ifdb.cache) > ifdb.maxSize {
		ifdb.flush()
	}
	return nil
}

func (ifdb *InfluxDB) flush() {
	tmp := ifdb.cache
	ifdb.cache = make([]pt, 0, ifdb.maxSize)
	ifdb.lastSend = time.Now()
	batchPoints, err := ifdb.buildBatch(tmp)
	if err != nil {
		return
	}
	go func(bp []influxdb.Metric) {
		if _, err := ifdb.influx.Write(context.Background(), ifdb.bucket, ifdb.org, bp...); err != nil {
			log.Println(err)
		}
	}(batchPoints)

}

func (ifdb *InfluxDB) buildBatch(points []pt) ([]influxdb.Metric, error) {
	bp := make([]influxdb.Metric, 0, len(points))
	for _, pt := range points {
		t := pt.time.Add(time.Millisecond * time.Duration(rand.Intn(1000)))
		p := influxdb.NewRowMetric(
			map[string]interface{}{`count`: pt.point.Count, `sum`: pt.point.Sum, `min`: pt.point.Min, `max`: pt.point.Max, `last`: pt.point.Last, `average`: pt.point.Sum / pt.point.Count},
			pt.name,
			pt.tags,
			t)
		bp = append(bp, p)

	}

	return bp, nil
}
