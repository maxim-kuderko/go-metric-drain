package metric_drivers

import (
	"bytes"
	"encoding/json"
	"net/http"
)

type DatadogDriver struct {
	url string
}

type datadogSeries struct {
	Series []*datadogMetric `json:"series"`
}
type datadogMetric struct {
	Metric string     `json:"metric"`
	Points [][2]float64 `json:"points"`
	Tags   []string   `json:"tags,omitempty"`
}

func NewDatadogDriver(apiKey string) *DatadogDriver {
	return &DatadogDriver{url: "https://app.datadoghq.com/api/v1/series?api_key=" + apiKey}
}

func (dd *DatadogDriver) Send(key string, name string, Points []PtDataer, tags *map[string]string) error{
	jsonData := dd.jsonify(name, Points, tags)

	req, err := http.NewRequest("POST", dd.url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")
	if err != nil{
		return err
	}

	client := &http.Client{}
	_, err = client.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (dd *DatadogDriver) jsonify(name string, Points []PtDataer, tags *map[string]string) []byte {
	tv := func() []string {
		t := *tags
		l :=  len(t)
		output := make([]string, 0, l)
		if l == 0{
			output = nil
			return output
		}
		for k, v := range t {
			output = append(output,k+"_"+v)
		}
		return output
	}()
	points := make([][2]float64,len(Points))
	for idx, p := range Points{
		points[idx] = [2]float64{float64(p.Time().UTC().Unix()),p.Data()}
	}
	dm := datadogMetric{
		Metric: name,
		Points: points,
		Tags:   tv,
	}
	ds := datadogSeries{Series: []*datadogMetric{&dm}}
	b, _ := json.Marshal(ds)
	return b

}
