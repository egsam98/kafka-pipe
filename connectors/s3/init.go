package s3

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/registry"
)

func init() {
	registry.Register("s3.Sink", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg SinkConfig
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse s3.Sink config")
		}
		return NewSink(cfg)
	})
	registry.Register("s3.Backup", func(cfg registry.Config) (kafkapipe.Connector, error) {
		return NewBackup(cfg)
	})
}
