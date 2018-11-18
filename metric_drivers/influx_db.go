package metric_drivers

import (
	"github.com/influxdata/influxdb/client/v2"
	"math/rand"
	"sync"
	"time"
)

type InfluxDB struct {
	url, username, password string
	database                string
	precision               string
	retention               string
	maxSize                 int
	cache                   []pt
	lastSend                time.Time
	s                       sync.Mutex
}

func NewInfluxDB(url, username, password, database, precision, retention string, flushInterval time.Duration, maxSize int) *InfluxDB {
	ifdb := &InfluxDB{url: url, username: username, password: password, precision: precision, database: database, retention: retention, cache: make([]pt, 0, maxSize), maxSize: maxSize, lastSend: time.Now()}
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
	go func(bp client.BatchPoints) {
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     ifdb.url,
			Username: ifdb.username,
			Password: ifdb.password,
		})
		if err != nil {
			return
		}
		defer c.Close()
		c.Write(bp)
	}(batchPoints)

}

func (ifdb *InfluxDB) buildBatch(points []pt) (client.BatchPoints, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        ifdb.database,
		Precision:       ifdb.precision,
		RetentionPolicy: ifdb.retention,
	})
	if err != nil {
		return nil, err
	}
	for _, pt := range points {
		t := pt.time.Add(time.Millisecond * time.Duration(rand.Intn(1000)))
		p, err := client.NewPoint(pt.name, pt.tags, map[string]interface{}{`count`: pt.point.Count, `sum`: pt.point.Sum, `min`: pt.point.Min, `max`: pt.point.Max, `last`: pt.point.Last, `average`: pt.point.Sum / pt.point.Count}, t)
		if err != nil {
			continue
		}
		bp.AddPoint(p)

	}

	return bp, nil
}
