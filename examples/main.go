package main

import (
	"github.com/maxim-kuderko/metric-reporter/reporter_drivers"
	"github.com/maxim-kuderko/metric-reporter"
	"os/signal"
	"os"
	"log"
)

func main() {
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, os.Kill)

	driver := reporter_drivers.NewDatadogDriver("")
	reporter := metric_reporter.NewMetricsReporter(driver, 5, 10000000)
	go func() {
		for {

			reporter.Send("metric1", 123, map[string]string{"test": "test1", "env": "example"})
			reporter.Send("metric2", 321, map[string]string{"test": "test2", "env": "example"})

		}
	}()

	<-stop
	log.Println("sigint")
	log.Println("waiting")
	reporter.Wait() // wait for the reporter to exit
	log.Println("exit")
}
