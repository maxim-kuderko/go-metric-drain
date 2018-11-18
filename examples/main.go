package main

import (
	"fmt"
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
		log.Println(http.ListenAndServe("localhost:6060", nil))
	}()
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	ldriver := metric_drivers.NewInfluxDB(``, ``, ``, ``, ``, ``, time.Minute, 2000)

	reporter, err := metric_reporter.NewMetricsReporter(
		[]metric_drivers.DriverInterface{ldriver},
		time.Second*2, time.Second*10, time.Minute*10, "metric-logger-driver",
		map[string]string{"env": "test"},
	)

	go func() {
		for j := 0; j < 10000000; j++ {
			if j%1000 == 0 {
				fmt.Println(j)
			}
			for i := 0; i < 2000; i++ {
				reporter.Send("mytest.metric1", 1, map[string]string{"test": strconv.Itoa(i), "test2": strconv.Itoa(j / 1000)})
			}
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
