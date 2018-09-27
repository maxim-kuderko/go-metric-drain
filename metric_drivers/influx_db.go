package metric_drivers

import (
	"github.com/influxdata/influxdb/client/v2"
	"log"
	"time"
)

type InfluxDB struct {
	url, username, password string
	database                string
	precision               string
	retention               string
	aggregationResolution   time.Duration
}

func NewInfluxDB(url, username, password, database, precision, retention string, aggregationResolution time.Duration) *InfluxDB {
	if aggregationResolution <= time.Second {
		aggregationResolution = time.Second
	}
	return &InfluxDB{url: url, username: username, password: password, precision: precision, database: database, retention: retention, aggregationResolution: aggregationResolution}
}

func (ifdb *InfluxDB) Send(key string, name string, Points []PtDataer, tags *map[string]string) error {
	batchPoints, err := ifdb.buildBatch(name, ifdb.aggregatePoints(Points), tags)
	if err != nil {
		return err
	}
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     ifdb.url,
		Username: ifdb.username,
		Password: ifdb.password,
		Timeout:  time.Second * 30,
	})
	if err != nil {
		log.Fatal(err)
	}
	defer c.Close()
	if err := c.Write(batchPoints); err != nil {
		return err
	}
	return nil
}

func (ifdb *InfluxDB) aggregatePoints(Points []PtDataer) map[time.Time]*AggregatedPoint {
	mp := map[time.Time]*AggregatedPoint{}
	for _, p := range Points {
		key := time.Unix(0, p.Time().Add(-1*time.Duration(p.Time().UnixNano()%int64(ifdb.aggregationResolution.Nanoseconds()))).UnixNano())
		if _, ok := mp[key]; !ok {
			mp[key] = &AggregatedPoint{
				sum:   p.Data(),
				count: 1,
				min:   p.Data(),
				max:   p.Data(),
				last:  p.Data(),
			}
			continue
		}
		mp[key].sum += p.Data()
		mp[key].count++
		mp[key].last = p.Data()
		if p.Data() < mp[key].min {
			mp[key].min = p.Data()
		}
		if p.Data() > mp[key].max {
			mp[key].max = p.Data()
		}

	}
	return mp
}

type AggregatedPoint struct {
	sum, count, last, min, max float64
}

func (ifdb *InfluxDB) buildBatch(name string, Points map[time.Time]*AggregatedPoint, tags *map[string]string) (client.BatchPoints, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:        ifdb.database,
		Precision:       ifdb.precision,
		RetentionPolicy: ifdb.retention,
	})
	if err != nil {
		return nil, err
	}

	for t, point := range Points {
		t.Add(time.Duration(time.Now().Nanosecond()))
		p, err := client.NewPoint(name, *tags, map[string]interface{}{`count`: point.count, `sum`: point.sum, `min`: point.min, `max`: point.max, `last`: point.last}, t)
		if err != nil {
			continue
		}
		bp.AddPoint(p)
	}
	Points = nil

	return bp, err
}
