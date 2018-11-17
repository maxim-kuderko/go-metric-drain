package metric_drivers

import (
	"github.com/influxdata/influxdb/client/v2"
	"sync"
	"time"
)

type InfluxDB struct {
	url, username, password string
	database                string
	precision               string
	retention               string
	aggregationResolution   time.Duration
	flushInterval           time.Duration
	mp                      map[string]map[uint64]map[time.Time]*AggregatedPoint
	maxGroupSize            int
	lastSend                time.Time
	s                       sync.Mutex
}

func NewInfluxDB(url, username, password, database, precision, retention string, aggregationResolution time.Duration, flushInterval time.Duration) *InfluxDB {
	if aggregationResolution <= time.Second {
		aggregationResolution = time.Second
	}
	ifdb := &InfluxDB{url: url, username: username, password: password, precision: precision, database: database, retention: retention, aggregationResolution: aggregationResolution, flushInterval: flushInterval, lastSend: time.Now()}
	ifdb.swapMp()
	return ifdb
}

func (ifdb *InfluxDB) Send(key uint64, name string, Points []PtDataer, tags *map[string]string) error {
	ifdb.s.Lock()
	defer ifdb.s.Unlock()
	ifdb.aggregatePoints(name, key, tags, Points)
	if time.Now().Sub(ifdb.lastSend).Seconds() < ifdb.flushInterval.Seconds() {
		return nil
	}
	aggregatedPoints := ifdb.swapMp()
	ifdb.lastSend = time.Now()
	ifdb.maxGroupSize = 0

	go func() {
		batchPoints, err := ifdb.buildBatch(name, aggregatedPoints)
		if err != nil {
			return
		}
		c, err := client.NewHTTPClient(client.HTTPConfig{
			Addr:     ifdb.url,
			Username: ifdb.username,
			Password: ifdb.password,
			Timeout:  time.Second * 30,
		})
		if err != nil {
			return
		}
		defer c.Close()
		c.Write(batchPoints)
	}()

	return nil
}

func (ifdb *InfluxDB) aggregatePoints(name string, key uint64, tags *map[string]string, Points []PtDataer) {
	for _, p := range Points {
		timekey := time.Unix(0, p.Time().Add(-1*time.Duration(p.Time().UnixNano()%int64(ifdb.aggregationResolution.Nanoseconds()))).UnixNano())
		if _, ok := ifdb.mp[name]; !ok {
			ifdb.mp[name] = make(map[uint64]map[time.Time]*AggregatedPoint)
		}
		if _, ok := ifdb.mp[name][key]; !ok {
			ifdb.mp[name][key] = make(map[time.Time]*AggregatedPoint)
		}
		if _, ok := ifdb.mp[name][key][timekey]; !ok {
			ifdb.mp[name][key][timekey] = &AggregatedPoint{
				sum:   p.Data(),
				count: 1,
				min:   p.Data(),
				max:   p.Data(),
				last:  p.Data(),
				tags:  *tags,
			}
			continue
		}
		ifdb.mp[name][key][timekey].sum += p.Data()
		ifdb.mp[name][key][timekey].count++
		ifdb.mp[name][key][timekey].last = p.Data()
		if p.Data() < ifdb.mp[name][key][timekey].min {
			ifdb.mp[name][key][timekey].min = p.Data()
		}
		if p.Data() > ifdb.mp[name][key][timekey].max {
			ifdb.mp[name][key][timekey].max = p.Data()
		}

		if len(ifdb.mp[name]) > ifdb.maxGroupSize {
			ifdb.maxGroupSize = len(ifdb.mp[name])
		}

	}
}

type AggregatedPoint struct {
	sum, count, last, min, max float64
	tags                       map[string]string
}

func (ifdb *InfluxDB) buildBatch(name string, Points map[string]map[uint64]map[time.Time]*AggregatedPoint) (client.BatchPoints, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        ifdb.database,
		Precision:       ifdb.precision,
		RetentionPolicy: ifdb.retention,
	})
	if err != nil {
		return nil, err
	}

	for name, aggPoints := range Points {
		for _, tf := range aggPoints {
			for ts, point := range tf {
				p, err := client.NewPoint(name, point.tags, map[string]interface{}{`count`: point.count, `sum`: point.sum, `min`: point.min, `max`: point.max, `last`: point.last, `average`: point.sum / point.count}, ts.Add(time.Duration(time.Now().Nanosecond())))
				if err != nil {
					continue
				}
				bp.AddPoint(p)
			}
		}
	}
	Points = nil

	return bp, nil
}

func (ifdb *InfluxDB) swapMp() map[string]map[uint64]map[time.Time]*AggregatedPoint {
	tmp := ifdb.mp
	ifdb.mp = make(map[string]map[uint64]map[time.Time]*AggregatedPoint)
	return tmp
}
