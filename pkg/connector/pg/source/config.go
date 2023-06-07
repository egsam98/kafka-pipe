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
	} `yaml:"pg"`
	Kafka struct {
		Brokers     []string `yaml:"brokers"`
		TopicPrefix string   `yaml:"topic.prefix"`
	} `yaml:"kafka"`
	HealthInterval time.Duration `yaml:"health.interval"`
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
	if len(c.Kafka.Brokers) == 0 {
		return errors.New(`"kafka.brokers" list is required`)
	}
	if c.Kafka.TopicPrefix == "" {
		return errors.New(`"kafka.topic_prefix" is required`)
	}
	if c.HealthInterval == 0 {
		c.HealthInterval = 10 * time.Second
	}
	return nil
}
