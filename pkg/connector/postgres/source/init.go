package source

import (
	"kafka-pipe/pkg/connector"
)

func init() {
	connector.Register("postgres.Source", func(cfg connector.Config) (connector.Connector, error) {
		return NewSource(cfg)
	})
}
