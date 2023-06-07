package snapshot

import (
	"kafka-pipe/pkg/connector"
)

func init() {
	connector.Register("pg.SourceSnapshot", func(cfg connector.Config) (connector.Connector, error) {
		return NewSnapshot(cfg)
	})
}
