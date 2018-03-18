package reporter_drivers

import (
	"net/http"
	"bytes"
	"log"
	"fmt"
	"io/ioutil"
	"encoding/json"
)

type LibratoDriver struct {
	url string
	token string
}


func NewLibratoDriver(token string) *LibratoDriver {
	return &LibratoDriver{url:"https://api.appoptics.com/v1/measurements" , token: token}
}

func (ld *LibratoDriver) Send(name string, Points [][2]int64, tags map[string]string){
	jsonData := ld.jsonify(name, Points, tags)

	req, err := http.NewRequest("POST", ld.url, bytes.NewBuffer(jsonData))
	req.Header.Set("Content-Type", "application/json")

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
	}
}

func (ld *LibratoDriver) jsonify(name string, Points [][2]int64, tags map[string]string) []byte {
	tv := func() [] string {
		output := make([]string, 0, len(tags))
		for _, v := range tags {
			output = append(output, v)
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
