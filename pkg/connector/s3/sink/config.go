package sink

import (
	"time"

	"github.com/pkg/errors"
	"gopkg.in/yaml.v3"
)

type Config struct {
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
	} `yaml:"s3"`
	Flush struct {
		Size    int           `yaml:"size"`
		Timeout time.Duration `yaml:"timeout"`
	} `yaml:"flush"`
}

func (c *Config) Parse(src []byte) error {
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
	if c.S3.Bucket == "" {
		return errors.New(`"s3.bucket" is required`)
	}
	if c.Flush.Size == 0 {
		return errors.New(`"flush.size" is required`)
	}
	if c.Flush.Timeout == 0 {
		return errors.New(`"flush.timeout" is required`)
	}
	return nil
}
