package metric_drivers

import (
	"time"
)

type DriverInterface interface {
	Send(key string, name string, Points []PtDataer, tags *map[string]string) error
}

type PtDataer interface {
	Time() time.Time
	Data() float64
}


type PtData struct{
	t time.Time
	d float64
}
func (pt *PtData) Time() time.Time{
	return pt.t
}

func (pt *PtData) Data() float64{
	return pt.d
}


func NewPoint(ts time.Time, d float64) * PtData{
	return &PtData{
		t:ts,
		d:d,
	}
}