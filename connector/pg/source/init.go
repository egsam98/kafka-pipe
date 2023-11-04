package source

import (
	"kafka-pipe/connector"
)

func init() {
	connector.Register("pg.Source", func(cfg connector.Config) (connector.Connector, error) {
		return NewSource(cfg)
	})
}
