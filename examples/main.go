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

	driver := reporter_drivers.NewDatadogDriver("79b9f0937f4a26e23a9c6a17a7030884")
	reporter := metric_reporter.NewMetricsReporter(driver, 60, 150000)
	go func() {
		for {
			reporter.Send("test.metric1", 123, map[string]string{"test": "test1", "env": "example"})
			reporter.Send("test.metric1", 321, map[string]string{"test": "test2", "env": "example"})

		}
	}()

	<-stop
	log.Println("sigint")
	log.Println("waiting")
	reporter.Wait() // wait for the reporter to exit
	log.Println("exit")
}
