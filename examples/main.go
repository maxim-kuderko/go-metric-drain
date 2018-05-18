package main

import (
	"github.com/maxim-kuderko/metric-reporter/metric_drivers"
	"github.com/maxim-kuderko/metric-reporter"
	"os/signal"
	"os"
	"log"
	"math/rand"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, os.Kill)

	ldriver := metric_drivers.NewLibratoMetric("")
	cdriver := metric_drivers.NewMysqlCounter("root:1234@tcp(localhost:3306)/metrics", "counters", 130)

	reporter, err := metric_reporter.NewMetricsReporter([]metric_drivers.DriverInterface{ldriver}, []metric_drivers.DriverInterface{cdriver}, 60, 2000, "production.example_app")
	pool := 8
	sem := make(chan bool,pool)
	for i := 0; i< pool; i++{
		sem <- true
	}
	go func() {
		for {
			<- sem
			go func() {
				reporter.Send("test.metric1", rand.Int63n(1000), map[string]string{"test": "test1", "env": "example"})
				reporter.Send("test.metric2", rand.Int63n(1000), map[string]string{"test": "test1", "env": "example"})
				reporter.Count("test.metric1", rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric1", rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric2", rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric3", -rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric4", -rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric5", -rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				reporter.Count("test.metric6", -rand.Float64(), map[string]string{"test": "test2", "env": "example"})
				sem <- true
			}()
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
