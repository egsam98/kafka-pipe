package sink

import (
	"context"
	"fmt"
	"os"
	"time"

	"github.com/pkg/errors"
	"github.com/segmentio/kafka-go"
	"gopkg.in/yaml.v3"
)

const (
	KafkaBatchSize    = 50000
	KafkaBatchTimeout = 5 * time.Second
)

type Sink struct {
	name string
	cfg  Config
}

func NewSink(name string, cfgRaw []byte) (*Sink, error) {
	var cfg Config
	if err := cfg.Parse(cfgRaw); err != nil {
		return nil, errors.Wrap(err, "parse sink config")
	}
	return &Sink{
		name: name,
		cfg:  cfg,
	}, nil
}

func (s *Sink) Run() error {
	r := kafka.NewReader(kafka.ReaderConfig{
		Brokers:     s.cfg.Kafka.Brokers,
		GroupID:     s.name,
		GroupTopics: s.cfg.Kafka.Topics,
	})

	for {
		msgs := make([]kafka.Message, 0, KafkaBatchSize)
		ctx, cancel := context.WithTimeout(context.Background(), KafkaBatchTimeout)
		for i := 0; i < KafkaBatchSize; i++ {
			msg, err := r.FetchMessage(ctx)
			if err != nil {
				if errors.Is(err, context.DeadlineExceeded) {
					break // Use collected messages next
				}
				cancel()
				return errors.Wrap(err, "fetch messages")
			}
			msgs = append(msgs, msg)
		}
		cancel()

		if err := s.storeIntoS3(msgs); err != nil {
			return err
		}
	}
}

func (s *Sink) storeIntoS3(msgs []kafka.Message) error {
	// Files by topic name
	files := make(map[string]*os.File)

	for _, msg := range msgs {
		f, ok := files[msg.Topic]
		if !ok {
			var err error
			if f, err = os.OpenFile(
				fmt.Sprintf("%s_%d", msg.Topic, msg.Offset),
				os.O_CREATE|os.O_WRONLY|os.O_APPEND,
				0755,
			); err != nil {
				return errors.Wrapf(err, "open file for topic %q", msg.Topic)
			}
			files[msg.Topic] = f
		}

		if _, err := fmt.Fprintf(f, "%s\t%s\t%v\n", string(msg.Key), string(msg.Value), msg.Headers); err != nil {
			return err
		}
	}

	return nil
}

type Config struct {
	ScheduleIntervalMin int `yaml:"schedule.interval.min"`
	FlushSize           int `yaml:"flush.size"`
	Kafka               struct {
		Brokers []string `yaml:"brokers"`
		Topics  []string `yaml:"topics"`
	} `yaml:"kafka"`
	S3 struct {
		URL    string `yaml:"url"`
		Bucket string `yaml:"bucket"`
	} `yaml:"s3"`
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
	if c.FlushSize == 0 {
		return errors.New(`"flush.size" is required`)
	}
	if c.ScheduleIntervalMin < 1 {
		return errors.New(`set "schedule.interval.min" = 1 at least`)
	}
	return nil
}
