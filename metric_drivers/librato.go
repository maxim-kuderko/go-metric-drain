package metric_drivers

import (
	"net/http"
	"bytes"
	"encoding/json"
	"encoding/base64"
)

type LibratoDriver struct {
	url   string
	token string
}

func NewLibratoMetric(token string) *LibratoDriver {
	enc := base64.StdEncoding
	return &LibratoDriver{url: "https://api.appoptics.com/v1/measurements", token: "Basic " + enc.EncodeToString([]byte(token + ":"))}
}

func (ld *LibratoDriver) Send(key string, name string, Points [][2]float64, tags *map[string]string) error {
	jsonData := ld.jsonify(name, Points, tags)

	req, _ := http.NewRequest("POST", ld.url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", ld.token)

	client := &http.Client{}
	_, err := client.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (ld *LibratoDriver) jsonify(name string, Points [][2]float64, tags *map[string]string) []byte {
	metrics := make([]*libratoMetric, 0, 2)
	for _, v := range ld.aggregatePoints(name, Points){
		metrics = append(metrics, v)
	}
	if tags == nil{
		tags = &map[string]string{"general": "general"}
	}
	ds := libratoRequest{Measurements: metrics, Tags: tags,}
	b, _ := json.Marshal(ds)
	return b

}

func  (ld *LibratoDriver) aggregatePoints(name string, Points [][2]float64) map[int64]*libratoMetric{
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

type libratoRequest struct {
	Measurements []*libratoMetric  `json:"measurements"`
	Tags         *map[string]string `json:"tags"`
}

type libratoMetric struct {
	Name   string `json:"name"`
	Time   int64  `json:"time"`
	Period int64  `json:"period"`
	Sum    float64  `json:"sum"`
	Count  int64  `json:"count"`
	Min    float64  `json:"min"`
	Max    float64  `json:"max"`
	Last   float64  `json:"last"`
}