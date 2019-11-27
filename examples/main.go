package main

import (
	"github.com/maxim-kuderko/metric-reporter"
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"strconv"
	"time"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6660", nil))
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	ldriver := metric_drivers.NewInfluxDB("http://influxdb-214526056.us-east-1.elb.amazonaws.com:8086", "admin", "nv7jy2s9i4elcq5b", "realtime", `ms`, `default`, time.Minute, 2000)

	reporter, err := metric_reporter.NewMetricsReporter(
		[]metric_drivers.DriverInterface{ldriver},
		time.Second*2, time.Second*10, time.Second*30, "metric-logger-driver",
		map[string]string{"env": "test"},
	)

	go func() {
		for j := 0; j < 100000000; j++ {
			reporter.Send("mytest.metric1", 1, map[string]string{"test2": strconv.Itoa(j / 1000)})
			reporter.Send("mytest.metric2", 2, map[string]string{"test2": strconv.Itoa(j / 1000)})
		}

		log.Println("done")
	}()

	go func() {
		for e := range err {
			log.Println(e)
		}
	}()

	<-stop
	log.Println("sigint")
	log.Println("waiting")
	reporter.Wait() // wait for the reporter to exit
	log.Println("exit")
}
