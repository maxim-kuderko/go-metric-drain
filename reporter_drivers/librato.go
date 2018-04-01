package reporter_drivers

import (
	"net/http"
	"bytes"
	"log"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"encoding/base64"
	"strings"
)

type LibratoDriver struct {
	url   string
	token string
}

func NewLibratoDriver(token string) *LibratoDriver {
	enc := base64.StdEncoding
	return &LibratoDriver{url: "https://api.appoptics.com/v1/measurements", token: "Basic " + enc.EncodeToString([]byte(token + ":"))}
}

func (ld *LibratoDriver) Send(name string, Points [][2]int64, tags map[string]string) {
	jsonData := ld.jsonify(name, Points, tags)

	req, err := http.NewRequest("POST", ld.url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("Authorization", ld.token)

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusAccepted {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}
}

func (ld *LibratoDriver) jsonify(name string, Points [][2]int64, tags map[string]string) []byte {
	metrics := make([]*libratoMetric, 0, 2)

	for _, v := range ld.aggregatePoints(name, Points){
		metrics = append(metrics, v)
	}
	if tags == nil || len(tags) == 0{
		tags = map[string]string{"general": "general"}
	}
	for k, v := range tags{
		tags[k] = strings.Replace(v, " ", "_", -1)
	}
	ds := libratoRequest{Measurements: metrics, Tags: tags,}
	b, _ := json.Marshal(ds)
	return b

}

func  (ld *LibratoDriver) aggregatePoints(name string, Points [][2]int64) map[int64]*libratoMetric{
	mp := map[int64]*libratoMetric{}
	for _, p := range Points {
		key := p[0] - p[0] % 60
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
	Tags         map[string]string `json:"tags"`
}

type libratoMetric struct {
	Name   string `json:"name"`
	Time   int64  `json:"time"`
	Period int64  `json:"period"`
	Sum    int64  `json:"sum"`
	Count  int64  `json:"count"`
	Min    int64  `json:"min"`
	Max    int64  `json:"max"`
	Last   int64  `json:"last"`
}