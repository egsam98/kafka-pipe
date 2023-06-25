package sink

import (
	"kafka-pipe/pkg/connector"
)

func init() {
	connector.Register("s3.Sink", func(cfg connector.Config) (connector.Connector, error) {
		return NewSink(cfg)
	})
}
