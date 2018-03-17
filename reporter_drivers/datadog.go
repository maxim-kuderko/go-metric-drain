package reporter_drivers

import "log"

type DatadogDriver struct {
	url string
}

func NewDatadogDriver(apiKey string) *DatadogDriver {
	return &DatadogDriver{url: "https://app.datadoghq.com/api/v1/events?api_key=" + apiKey}
}

func (dd *DatadogDriver) Send(name string, Points [][2]int64, tags map[string]string) {
	log.Println("SENDING TO DATADOG: ", name, len(Points), tags)
}
