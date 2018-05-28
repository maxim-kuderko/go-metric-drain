package metric_drivers

type DriverInterface interface {
	Send(key string, name string, Points [][2]float64, tags *map[string]string) error
}
