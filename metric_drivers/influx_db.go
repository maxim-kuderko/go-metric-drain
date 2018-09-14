package metric_drivers

import (
	"github.com/influxdata/influxdb/client/v2"
	"log"
	"time"
)

type InfluxDB struct {
	c          client.Client
	database   string
	precision string
}

func NewInfluxDB(url, username, password, database string, precision string) *InfluxDB {
	c, err := client.NewHTTPClient(client.HTTPConfig{
		Addr:     url,
		Username: username,
		Password: password,
	})
	if err != nil {
		log.Fatal(err)
	}
	return &InfluxDB{c: c, precision: precision, database: database,}
}

func (ifdb *InfluxDB) Send(key string, name string, Points [][2]float64, tags *map[string]string) error {
	batchPoints, err := ifdb.buildBatch(name, Points, tags)
	if err != nil {
		return err
	}
	if err := ifdb.c.Write(batchPoints); err != nil {
		return err
	}
	return nil
}

func (ifdb *InfluxDB) buildBatch(name string, Points [][2]float64, tags *map[string]string) (client.BatchPoints, error) {
	bp, err := client.NewBatchPoints(client.BatchPointsConfig{
		Database:  ifdb.database,
		Precision: ifdb.precision,
	})
	if err != nil {
		return nil, err
	}

	for _, p := range Points{
		p, err := client.NewPoint(name, *tags, map[string]interface{}{name: p[1]}, time.Unix(int64(p[0]),0))
		if err != nil{
			log.Println("debug: ", err)
			continue
		}
		bp.AddPoint(p)
	}

	return bp, err
}