package metric_reporter

import (
	"hash/fnv"
	"io"
	"sort"
	"strings"
)

func NewMetric(name string, value float64, tags map[string]string) Metric {
	m := Metric{
		name:  name,
		value: value,
		tags:  tags,
	}
	m.calcHash()
	return m
}

type Metric struct {
	name  string
	value float64
	tags  map[string]string
	hash  uint64
}

func (mc *Metric) calcHash() {
	hasher := fnv.New64()
	io.WriteString(hasher, mc.name)
	if mc.tags != nil {
		d := make([]string, 0, len(mc.tags))
		for _, v := range mc.tags {
			d = append(d, v)
		}
		sort.Strings(d)
		io.WriteString(hasher, strings.Join(d, ""))
	}
	mc.hash = hasher.Sum64()
}
