package kafkapipe

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/twmb/franz-go/pkg/sasl"
	"github.com/twmb/franz-go/pkg/sasl/aws"
	"github.com/twmb/franz-go/pkg/sasl/oauth"
	"github.com/twmb/franz-go/pkg/sasl/plain"
	"github.com/twmb/franz-go/pkg/sasl/scram"
	"gopkg.in/yaml.v3"
)

var Version = "dev"

type Connector interface {
	Run(ctx context.Context) error
}

type ProducerConfig struct {
	Brokers []string `yaml:"brokers" validate:"min=1,dive,url"`
	Topic   struct {
		Prefix            string            `yaml:"prefix"`
		ReplicationFactor uint16            `yaml:"replication.factor" validate:"default=1"`
		Partitions        uint32            `yaml:"partitions" validate:"default=1"`
		CleanupPolicy     string            `yaml:"cleanup.policy" validate:"default=delete"`
		CompressionType   string            `yaml:"compression.type" validate:"default=producer"`
		Routes            map[string]string `yaml:"routes"`
	} `yaml:"topic"`
	Batch BatchConfig `yaml:"batch"`
}

type ConsumerPoolConfig struct {
	Group                  string         `yaml:"group" validate:"required"`
	Brokers                []string       `yaml:"brokers" validate:"min=1,dive,url"`
	Topics                 []string       `yaml:"topics" validate:"min=1"`
	RebalanceTimeout       time.Duration  `yaml:"rebalance_timeout" validate:"default=1m"`
	WorkersPerTopic        uint           `yaml:"workers_per_topic" validate:"default=1"`
	FetchMaxBytes          uint           `yaml:"fetch_max_bytes"`
	FetchMaxPartitionBytes uint           `yaml:"fetch_max_partition_bytes"`
	Batch                  BatchConfig    `yaml:"batch"`
	SASL                   sasl.Mechanism `yaml:"-"`
}

type BatchConfig struct {
	Size    uint          `yaml:"size" validate:"default=10000"`
	Timeout time.Duration `yaml:"timeout" validate:"default=5s"`
}

func (c *ConsumerPoolConfig) UnmarshalYAML(node *yaml.Node) error {
	type inline ConsumerPoolConfig // Avoid stack overflow
	var cfg struct {
		inline `yaml:",inline"`
		SASL   yaml.Node `yaml:"sasl"`
	}
	if err := node.Decode(&cfg); err != nil {
		return errors.Wrap(err, "parse Kafka consumer pool config")
	}

	*c = ConsumerPoolConfig(cfg.inline)
	var err error
	if !cfg.SASL.IsZero() {
		c.SASL, err = newSASLFromYAML(cfg.SASL)
	}
	return err
}

func newSASLFromYAML(node yaml.Node) (sasl.Mechanism, error) {
	var protocol struct {
		Value string `yaml:"protocol"`
	}
	if err := node.Decode(&protocol); err != nil {
		return nil, errors.Wrap(err, "decode SASL protocol")
	}

	switch protocol.Value {
	case "plain":
		var cfg struct {
			Zid  string `yaml:"zid"`
			User string `yaml:"user"`
			Pass string `yaml:"pass"`
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, err
		}
		return plain.Auth{
			Zid:  cfg.Zid,
			User: cfg.User,
			Pass: cfg.Pass,
		}.AsMechanism(), nil
	case "scram-256", "scram-512":
		var cfg struct {
			IsToken bool   `yaml:"is_token"`
			Nonce   string `yaml:"nonce"`
			Zid     string `yaml:"zid"`
			User    string `yaml:"user"`
			Pass    string `yaml:"pass"`
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, err
		}
		auth := scram.Auth{
			Zid:     cfg.Zid,
			User:    cfg.User,
			Pass:    cfg.Pass,
			Nonce:   []byte(cfg.Nonce),
			IsToken: cfg.IsToken,
		}
		if protocol.Value == "scram-256" {
			return auth.AsSha256Mechanism(), nil
		} else {
			return auth.AsSha512Mechanism(), nil
		}
	case "oauth":
		var cfg struct {
			Zid   string `yaml:"zid"`
			Token string `yaml:"token"`
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, err
		}
		return oauth.Auth{
			Zid:   cfg.Zid,
			Token: cfg.Token,
		}.AsMechanism(), nil
	case "aws":
		var cfg struct {
			AccessKey    string `yaml:"access_key"`
			SecretKey    string `yaml:"secret_key"`
			SessionToken string `yaml:"session_token"`
			UserAgent    string `yaml:"user_agent"`
		}
		if err := node.Decode(&cfg); err != nil {
			return nil, err
		}
		return aws.Auth{
			AccessKey:    cfg.AccessKey,
			SecretKey:    cfg.SecretKey,
			SessionToken: cfg.SessionToken,
			UserAgent:    cfg.UserAgent,
		}.AsManagedStreamingIAMMechanism(), nil
	default:
		return nil, errors.Errorf("unexpected SASL protocol: %s. "+
			"Expected one of: [plain,scram-256,scram-512,oauth,aws]", protocol.Value)
	}
}
