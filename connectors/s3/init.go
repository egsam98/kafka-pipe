package s3

import (
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/registry"
	"github.com/egsam98/kafka-pipe/internal/timex"
)

func init() {
	registry.Register("s3.Sink", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg SinkConfig
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse s3.Sink config")
		}
		cfg.DB = config.Storage
		return NewSink(cfg), nil
	})
	registry.Register("s3.Backup", func(config registry.Config) (kafkapipe.Connector, error) {
		var cfg struct {
			BackupConfig `yaml:",inline"`
			DateSince    timex.DateTime `yaml:"date_since"`
			DateTo       timex.DateTime `yaml:"date_to"`
		}
		if err := yaml.Unmarshal(config.Raw, &cfg); err != nil {
			return nil, errors.Wrap(err, "parse s3.Backup config")
		}
		cfg.BackupConfig.DateSince = cfg.DateSince.Time
		cfg.BackupConfig.DateTo = cfg.DateTo.Time
		return NewBackup(cfg.BackupConfig)
	})
}
