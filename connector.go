package kafkapipe

import (
	"context"
	"strconv"
	"time"

	"code.cloudfoundry.org/bytefmt"
	"github.com/egsam98/ecto"
	ectosl "github.com/egsam98/ecto/slices"
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
	Brokers []string    `yaml:"brokers"`
	Topic   TopicConfig `yaml:"topic"`
	Batch   BatchConfig `yaml:"batch"`
}

type TopicConfig struct {
	Prefix            string            `yaml:"prefix"`
	ReplicationFactor uint16            `yaml:"replication.factor"`
	Partitions        uint32            `yaml:"partitions"`
	CleanupPolicy     string            `yaml:"cleanup.policy"`
	CompressionType   string            `yaml:"compression.type"`
	Retention         time.Duration     `yaml:"retention"`
	PartRetentionSize string            `yaml:"part_retention_size"`
	Routes            map[string]string `yaml:"routes"`
}

var ProducerCfgSchema = ecto.Struct[ProducerConfig](ecto.M{
	"Brokers": ecto.Slice[[]string](
		ecto.String().Required(),
	).Test(ectosl.Min[[]string](1)),
	"Topic": ecto.Struct[TopicConfig](ecto.M{
		"ReplicationFactor": ecto.Atomic[uint16]().Default(1),
		"Partitions":        ecto.Atomic[uint32]().Default(1),
		"CleanupPolicy":     ecto.String().Default("delete"),
		"CompressionType":   ecto.String().Default("producer"),
		"Retention":         ecto.Atomic[time.Duration]().Default(168 * time.Hour),
		"PartRetentionSize": ecto.String().Default("10GB"),
	}),
	"Batch": BatchCfgSchema,
})

func (c *ProducerConfig) TopicMapConfig() (map[string]*string, error) {
	retentionBytes, err := bytefmt.ToBytes(c.Topic.PartRetentionSize)
	if err != nil {
		return nil, errors.Wrap(err, "parse part_retention_size: "+c.Topic.PartRetentionSize)
	}
	return map[string]*string{
		"compression.type": &c.Topic.CompressionType,
		"cleanup.policy":   &c.Topic.CleanupPolicy,
		"retention.ms":     new(strconv.FormatInt(c.Topic.Retention.Milliseconds(), 10)),
		"retention.bytes":  new(strconv.FormatUint(retentionBytes, 10)),
	}, nil
}

type ConsumerPoolConfig struct {
	Group                  string         `yaml:"group"`
	Brokers                []string       `yaml:"brokers"`
	Topics                 []string       `yaml:"topics"`
	RebalanceTimeout       time.Duration  `yaml:"rebalance_timeout"`
	WorkersPerTopic        uint           `yaml:"workers_per_topic"`
	FetchMaxBytes          uint           `yaml:"fetch_max_bytes"`
	FetchMaxPartitionBytes uint           `yaml:"fetch_max_partition_bytes"`
	Batch                  BatchConfig    `yaml:"batch"`
	SASL                   sasl.Mechanism `yaml:"-"`
}

type BatchConfig struct {
	Size    uint          `yaml:"size"`
	Timeout time.Duration `yaml:"timeout"`
}

var ConsumerPoolCfgSchema = ecto.Struct[ConsumerPoolConfig](ecto.M{
	"Group": ecto.String().Required(),
	"Brokers": ecto.Slice[[]string](
		ecto.String().Required(),
	).Test(ectosl.Min[[]string](1)),
	"Topics":           ecto.Slice[[]string](ecto.String()).Test(ectosl.Min[[]string](1)),
	"RebalanceTimeout": ecto.Atomic[time.Duration]().Default(time.Minute),
	"WorkersPerTopic":  ecto.Atomic[uint]().Default(1),
	"Batch":            BatchCfgSchema,
})

var BatchCfgSchema = ecto.Struct[BatchConfig](ecto.M{
	"Size":    ecto.Atomic[uint]().Default(10000),
	"Timeout": ecto.Atomic[time.Duration]().Default(5 * time.Second),
})

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
		}
		return auth.AsSha512Mechanism(), nil
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
