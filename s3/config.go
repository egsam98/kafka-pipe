package s3

import (
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type SinkConfig struct {
	Name  string `yaml:"name"`
	Kafka struct {
		Brokers []string `yaml:"brokers"`
		Topics  []string `yaml:"topics"`
	} `yaml:"kafka"`
	S3 struct {
		SSL             bool   `yaml:"ssl"`
		URL             string `yaml:"url"`
		Bucket          string `yaml:"bucket"`
		AccessKeyID     string `yaml:"access_key_id"`
		SecretKeyAccess string `yaml:"secret_key_access"`
		Flush           struct {
			Size    int           `yaml:"size"`
			Timeout time.Duration `yaml:"timeout"`
		} `yaml:"flush"`
	} `yaml:"s3"`
}

func (c *SinkConfig) Parse(src []byte) error {
	if err := yaml.Unmarshal(src, c); err != nil {
		return errors.Wrap(err, "parse sink config")
	}
	if len(c.Kafka.Brokers) == 0 {
		return errors.New(`"kafka.brokers" list is required`)
	}
	if len(c.Kafka.Topics) == 0 {
		return errors.New(`"kafka.topics" list is required`)
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
	if c.S3.Flush.Size == 0 {
		return errors.New(`"s3.flush.size" is required`)
	}
	if c.S3.Flush.Timeout == 0 {
		return errors.New(`"s3.flush.timeout" is required`)
	}
	return nil
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
