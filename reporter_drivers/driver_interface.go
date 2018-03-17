package reporter_drivers

type DriverInterface interface {
	Send(name string, Points [][2]int64, tags map[string]string)
}