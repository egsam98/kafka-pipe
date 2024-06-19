package timex

import (
	"time"

	"gopkg.in/yaml.v3"
)

type DateTime struct {
	time.Time
}

func (d *DateTime) UnmarshalYAML(node *yaml.Node) error {
	var err error
	d.Time, err = time.Parse(time.DateTime, node.Value)
	return err
}
