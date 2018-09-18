package metric_drivers

import (
	"bytes"
	"encoding/base64"
	"encoding/json"
	"net/http"
)

type LibratoDriver struct {
	url   string
	token string
}

func NewLibratoMetric(token string) *LibratoDriver {
	enc := base64.StdEncoding
	return &LibratoDriver{url: "https://api.appoptics.com/v1/measurements", token: "Basic " + enc.EncodeToString([]byte(token + ":"))}
}

func (ld *LibratoDriver) Send(key string, name string, Points []PtDataer, tags *map[string]string) error {
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

func (ld *LibratoDriver) jsonify(name string, Points []PtDataer, tags *map[string]string) []byte {
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

func  (ld *LibratoDriver) aggregatePoints(name string, Points []PtDataer) map[int64]*libratoMetric{
	mp := map[int64]*libratoMetric{}
	for _, p := range Points {
		t := p.Time().UTC().Unix()
		key := int64(t) - int64(t) % 60
		v, ok := mp[key]
		if !ok{
			mp[key] = &libratoMetric{}
			v = mp[key]
			v.Name = name
			v.Period = 60
			v.Time = key
		}
		v.Count++
		v.Sum += p.Data()
		v.Last = p.Data()
		if p.Data() < v.Min{
			v.Min = p.Data()
		}
		if p.Data() > v.Max{
			v.Max = p.Data()
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