package metric_drivers

import (
	"bytes"
	"encoding/json"
	"io"
	"net/http"
)

type MetricsLogger struct {
	driver, url, key string
}

func NewMetricsLogger(driver, url, key string) *MetricsLogger {
	return &MetricsLogger{driver: driver, url: url, key: key}
}

func (ml *MetricsLogger) Send(key string, name string, Points []PtDataer, tags *map[string]string) error {
	body := ml.prepare(name, Points, tags)

	req, _ := http.NewRequest("POST", ml.url, body)
	req.Header.Set("Content-Type", "application/json")
	req.Header.Set("x-api-token", ml.key)

	client := &http.Client{}
	_, err := client.Do(req)
	if err != nil {
		return err
	}
	return nil
}

func (ml *MetricsLogger) prepare(name string, Points []PtDataer, tags *map[string]string) io.Reader {
	output := bytes.NewBuffer(nil)
	for _, pt := range Points {
		if b, err := json.Marshal(&MetricsLoggerMetric{Driver: ml.driver, Name: name, Tags: *tags, Value: pt.Data()}); err == nil {
			output.Write(b)
		}
	}
	return output
}

type MetricsLoggerMetric struct {
	Driver string            `json:"driver"`
	Name   string            `json:"name"`
	Value  float64           `json:"value"`
	Tags   map[string]string `json:"tags"`
}
