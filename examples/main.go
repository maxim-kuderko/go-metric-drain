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
	signal.Notify(stop, os.Interrupt)

	done := make(chan bool, 1) // create a chaneel that will signal the reporter to exit
	driver := reporter_drivers.NewDatadogDriver("")
	reporter := metric_reporter.NewMetricsReporter(driver, 60, 100000, done)
	go func() {
		for {
			go func(){reporter.Send("metric1", 123, map[string]string{"test": "test1", "env": "example"})}()
			go func(){reporter.Send("metric2", 321, map[string]string{"test": "test2", "env": "example"})}()
		}
	}()

	<-stop
	log.Println("sigint")
	done <- true    // init reporter exit, in this case the init happens after sys interrupt but in can be anything
	log.Println("waiting")
	reporter.Wait() // wait for the reporter to exit
}
