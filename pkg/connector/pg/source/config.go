package source

import (
	"net/url"
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
	Pg struct {
		SkipDelete  bool     `yaml:"skip.delete"`
		Url         string   `yaml:"url"`
		Publication string   `yaml:"publication"`
		Slot        string   `yaml:"slot"`
		Tables      []string `yaml:"tables"`
		Health      struct {
			Table    string        `yaml:"table"`
			Interval time.Duration `yaml:"interval"`
		} `yaml:"health"`
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
		Batch struct {
			Size    int           `yaml:"size"`
			Timeout time.Duration `yaml:"timeout"`
		} `yaml:"batch"`
	} `yaml:"kafka"`
}

func (c *Config) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "parse source config")
	}

	uri, err := url.Parse(c.Pg.Url)
	if err != nil {
		return errors.Wrap(err, `"pg.url" is invalid`)
	}
	query := uri.Query()
	query.Set("replication", "database")
	uri.RawQuery = query.Encode()
	c.Pg.Url = uri.String()

	if c.Pg.Publication == "" {
		return errors.New(`"pg.publication" is required`)
	}
	if c.Pg.Slot == "" {
		return errors.New(`"pg.slot" is required`)
	}
	if len(c.Pg.Tables) == 0 {
		return errors.New(`"pg.tables" list is required`)
	}
	if c.Pg.Health.Table == "" {
		c.Pg.Health.Table = "public.pipe_health"
	}
	if c.Pg.Health.Interval == 0 {
		c.Pg.Health.Interval = 10 * time.Second
	}

	if len(c.Kafka.Brokers) == 0 {
		return errors.New(`"kafka.brokers" list is required`)
	}
	if c.Kafka.Topic.Prefix == "" {
		return errors.New(`"kafka.topic_prefix" is required`)
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
	if c.Kafka.Batch.Size == 0 {
		c.Kafka.Batch.Size = 10000
	}
	if c.Kafka.Batch.Timeout == 0 {
		c.Kafka.Batch.Timeout = 5 * time.Second
	}
	return nil
}
