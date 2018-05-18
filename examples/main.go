package main

import (
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"github.com/maxim-kuderko/metric-reporter"
	"os/signal"
	"os"
	"log"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop)

	ldriver := metric_drivers.NewLibratoMetric("")
	cdriver := metric_drivers.NewMysqlCounter("root:1234@tcp(localhost:3306)/metrics", "counters", 130)

	reporter, err := metric_reporter.NewMetricsReporter([]metric_drivers.DriverInterface{ldriver}, []metric_drivers.DriverInterface{cdriver}, 10, 2000000000, "production.example_app")
	pool := 16
	sem := make(chan bool,pool)
	for i := 0; i< pool; i++{
		sem <- true
	}
	go func() {
		for i := 0.0; i< 999999.0; i++{
			<- sem
			go func(i float64) {
				reporter.Send("test.metric1", int64(i), map[string]string{"test": "test1",})
				reporter.Send("test.metric1", int64(i), map[string]string{"test": "test2", },)
				reporter.Count("test.metric1", i, map[string]string{"test": "test2", },)
				reporter.Count("test.metric1", -i, map[string]string{"test": "test2", },)
				reporter.Count("test.metric2", 1, map[string]string{"test": "test2",}, )
				reporter.Count("test.metric2", -1, map[string]string{"test": "test2", },)
				reporter.Count("test.metric3", 1, map[string]string{"test": "test2", },11)
				reporter.Count("test.metric4", 1, map[string]string{"test": "test2",})
				sem <- true
			}(i)
		}
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
