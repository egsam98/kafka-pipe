package s3

import (
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type SinkConfig struct {
	Name              string                       `yaml:"name" validate:"required"`
	Kafka             kafkapipe.ConsumerPoolConfig `yaml:"kafka"`
	S3                ConnConfig                   `yaml:"s3"`
	GroupTimeInterval time.Duration                `yaml:"group_time_interval" validate:"default=1h"`
	DB                *badger.DB                   `yaml:"-" validate:"required"`
}

type ConnConfig struct {
	SSL    bool   `yaml:"ssl"`
	URL    string `yaml:"url" validate:"url"`
	Bucket string `yaml:"bucket" validate:"required"`
	ID     string `yaml:"id" validate:"required"`
	Secret string `yaml:"secret" validate:"required"`
}

type BackupConfig struct {
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Flush   struct {
			Size    int           `yaml:"size"`
			Timeout time.Duration `yaml:"timeout"`
		} `yaml:"flush"`
	} `yaml:"kafka"`
	S3 struct {
		SSL             bool     `yaml:"ssl"`
		URL             string   `yaml:"url"`
		Bucket          string   `yaml:"bucket"`
		AccessKeyID     string   `yaml:"access_key_id"`
		SecretKeyAccess string   `yaml:"secret_key_access"`
		Topics          []string `yaml:"topics"`
		Since           Time     `yaml:"since"`
		To              *Time    `yaml:"to"`
	} `yaml:"s3"`
}

func (c *BackupConfig) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "parse backup config")
	}

	if len(c.Kafka.Brokers) == 0 {
		return errors.New(`"kafka.brokers" list is required`)
	}
	if c.Kafka.Flush.Timeout == 0 {
		c.Kafka.Flush.Timeout = 5 * time.Second
	}
	if c.Kafka.Flush.Size == 0 {
		c.Kafka.Flush.Size = 10000
	}

	if c.S3.URL == "" {
		return errors.New(`"s3.url" is required`)
	}
	if c.S3.AccessKeyID == "" {
		return errors.New(`"s3.access_key_id" is required`)
	}
	if c.S3.SecretKeyAccess == "" {
		return errors.New(`"s3.secret_key_access" is required`)
	}
	if c.S3.Bucket == "" {
		return errors.New(`"s3.bucket" is required`)
	}
	if len(c.S3.Topics) == 0 {
		return errors.New(`"s3.topics" list is required`)
	}
	if c.S3.Since == (Time{}) {
		return errors.New(`"s3.since" is required`)
	}
	return nil
}

type Time struct {
	time.Time
}

func (t *Time) UnmarshalYAML(node *yaml.Node) (err error) {
	if string(node.Value) == "null" {
		return nil
	}
	t.Time, err = time.Parse("2006-01-02 15:04:05", node.Value)
	return
}
