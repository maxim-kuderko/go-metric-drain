package metric_drivers

import (
	"encoding/json"
)

type Stub struct {

}

func NewStubDriver() *Stub {
	return &Stub{}
}

func (ld *Stub) Send(key string, name string, Points [][2]float64, tags *map[string]string) {
	ld.jsonify(name, Points, tags)
}

func (ld *Stub) jsonify(name string, Points [][2]float64, tags *map[string]string) []byte {
	metrics := make([]*libratoMetric, 0, 2)

	for _, v := range ld.aggregatePoints(name, Points){
		metrics = append(metrics, v)
	}
	ds := libratoRequest{Measurements: metrics, Tags: tags,}
	b, _ := json.Marshal(ds)
	return b

}

func  (ld *Stub) aggregatePoints(name string, Points [][2]float64) map[int64]*libratoMetric{
	mp := map[int64]*libratoMetric{}
	for _, p := range Points {
		key := int64(p[0]) - int64(p[0]) % 60
		v, ok := mp[key]
		if !ok{
			mp[key] = &libratoMetric{}
			v = mp[key]
			v.Name = name
			v.Period = 60
			v.Time = key
		}
		v.Count++
		v.Sum += p[1]
		v.Last = p[1]
		if p[1] < v.Min{
			v.Min = p[1]
		}
		if p[1] > v.Max{
			v.Max = p[1]
		}
	}
	return mp
}