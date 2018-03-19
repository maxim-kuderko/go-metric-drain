package reporter_drivers

import (
	"bytes"
	"encoding/json"
	"fmt"
	"net/http"
	"io/ioutil"
	"log"
)

type DatadogDriver struct {
	url string
}

type datadogSeries struct {
	Series []*datadogMetric `json:"series"`
}
type datadogMetric struct {
	Metric string     `json:"metric"`
	Points [][2]int64 `json:"points"`
	Tags   []string   `json:"tags,omitempty"`
}

func NewDatadogDriver(apiKey string) *DatadogDriver {
	return &DatadogDriver{url: "https://app.datadoghq.com/api/v1/series?api_key=" + apiKey}
}

func (dd *DatadogDriver) Send(name string, Points [][2]int64, tags map[string]string) {
	jsonData := dd.jsonify(name, Points, tags)

	req, err := http.NewRequest("POST", dd.url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	if err != nil{
		log.Println(err)
		return
	}

	client := &http.Client{}
	resp, err := client.Do(req)
	if err != nil {
		log.Println(err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != 202{
		fmt.Println("response Status:", resp.Status)
		fmt.Println("response Headers:", resp.Header)
		body, _ := ioutil.ReadAll(resp.Body)
		fmt.Println("response Body:", string(body))
		fmt.Println("Tried to send ", len(Points), " to Datadog")
	}

}

func (dd *DatadogDriver) jsonify(name string, Points [][2]int64, tags map[string]string) []byte {
	tv := func() []string {
		output := make([]string, 0, len(tags))
		if len(tags) == 0{
			output = nil
			return output
		}
		for k, v := range tags {
			output = append(output,k+"_"+v)
		}
		return output
	}()
	dm := datadogMetric{
		Metric: name,
		Points: Points,
		Tags:   tv,
	}
	ds := datadogSeries{Series: []*datadogMetric{&dm}}
	b, _ := json.Marshal(ds)
	return b

}
