package main

import (
	"github.com/maxim-kuderko/metric-reporter"
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"time"
)

func main() {
	go func() {
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	ldriver := metric_drivers.NewInfluxDB("https://telemetry-spot-im.aivencloud.com:13088", "avnadmin", "nv7jy2s9i4elcq5b", "defaultdb", "ns", "", time.Second)
	cdriver := metric_drivers.NewMysqlCounter("root:getalife@tcp(localhost:3306)/metrics", "counters", 130)

	reporter, err := metric_reporter.NewMetricsReporter([]metric_drivers.DriverInterface{ldriver}, []metric_drivers.DriverInterface{cdriver}, 10, 10000, "example_app", map[string]string{"env": "test"})

	go func() {
		for {
			reporter.Send("test.metric1", 1, map[string]string{"test": "test1"})
			reporter.Send("test.metric1", 1, map[string]string{"test": "test2"})
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
