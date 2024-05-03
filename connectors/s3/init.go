package s3

import (
	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/registry"
)

func init() {
	registry.Register("s3.Sink", func(cfg registry.Config) (kafkapipe.Connector, error) {
		return NewSink(cfg)
	})
	registry.Register("s3.Backup", func(cfg registry.Config) (kafkapipe.Connector, error) {
		return NewBackup(cfg)
	})
}
