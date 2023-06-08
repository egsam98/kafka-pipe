package snapshot

import (
	"net/url"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Pg struct {
		Url       string   `yaml:"url"`
		Tables    []string `yaml:"tables"`
		Condition string   `yaml:"condition"`
	} `yaml:"pg"`
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Topic   struct {
			Prefix            string `yaml:"prefix"`
			ReplicationFactor int16  `yaml:"replication.factor"`
			Partitions        int32  `yaml:"partitions"`
			CleanupPolicy     string `yaml:"cleanup.policy"`
			CompressionType   string `yaml:"compression.type"`
		} `yaml:"topic"`
		BatchSize int `yaml:"batch.size"`
	} `yaml:"kafka"`
}

func (c *Config) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "parse snapshot config")
	}
	if _, err := url.Parse(c.Pg.Url); err != nil {
		return errors.Wrap(err, `"pg.url" is invalid`)
	}
	if c.Kafka.BatchSize == 0 {
		c.Kafka.BatchSize = 10_000
	}
	if len(c.Pg.Tables) == 0 {
		return errors.New(`"pg.tables" list is required`)
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New(`"kafka.brokers" list is required`)
	}
	if c.Kafka.Topic.Prefix == "" {
		return errors.New(`"kafka.topic.prefix" is required`)
	}
	if c.Kafka.Topic.ReplicationFactor == 0 {
		c.Kafka.Topic.ReplicationFactor = 1
	}
	if c.Kafka.Topic.Partitions == 0 {
		c.Kafka.Topic.Partitions = 1
	}
	if c.Kafka.Topic.CompressionType == "" {
		c.Kafka.Topic.CompressionType = "producer"
	}
	if c.Kafka.Topic.CleanupPolicy == "" {
		c.Kafka.Topic.CleanupPolicy = "delete"
	}
	return nil
}
