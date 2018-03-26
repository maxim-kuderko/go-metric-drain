package reporter_drivers

import (
	"net/http"
	"bytes"
	"log"
	"fmt"
	"io/ioutil"
	"encoding/json"
	"encoding/base64"
)

type LibratoDriver struct {
	url   string
	token string
}

func NewLibratoDriver(token string) *LibratoDriver {
	enc := base64.StdEncoding
	return &LibratoDriver{url: "https://api.appoptics.com/v1/measurements", token: "Basic " + enc.EncodeToString([]byte(token))}
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

	if resp.StatusCode != 202 {
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
	}
}

func (ld *LibratoDriver) jsonify(name string, Points [][2]int64, tags map[string]string) []byte {
	metrics := make([]*libratoMetric, 0, 1000)
	for _, p := range Points {
		m := &libratoMetric{
			Name:   name,
			Time:   p[1] - (p[1] % 60),
			Period: 60,
			Count:  p[0],
			Tags: tags,
		}
		metrics = append(metrics, m)
	}
	ds := libratoRequest{Measurements: metrics}
	b, _ := json.Marshal(ds)
	return b

}

type libratoRequest struct {
	Measurements []*libratoMetric `json:"measurements"`
}

type libratoMetric struct {
	Name   string            `json:"name"`
	Time   int64             `json:"time"`
	Period int               `json:"period"`
	Sum    float64           `json:"sum"`
	Count  int64               `json:"count"`
	Tags   map[string]string `json:"tags"`
}
