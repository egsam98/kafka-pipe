package sink

import (
	"kafka-pipe/pkg/connector"
)

func init() {
	connector.Register("s3.Sink", func(cfgRaw []byte) (connector.Connector, error) {
		return NewSink(cfgRaw)
	})
}
