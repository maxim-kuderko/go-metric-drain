package metric_drivers

import "time"

type DriverInterface interface {
	Send(key uint64, name string, Point AggregatedPoint, tags map[string]string, time time.Time) error
}

type AggregatedPoint struct {
	Sum, Count, Last, Min, Max float64
}
