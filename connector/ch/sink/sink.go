package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Sink struct {
	cfg    Config
	kafka  map[string]*kgo.Client // Kafka client per topic
	chConn driver.Conn
}

func NewSink(cfg Config) *Sink {
	return &Sink{
		cfg:   cfg,
		kafka: make(map[string]*kgo.Client),
	}
}

func (s *Sink) Run(ctx context.Context) error {
	var err error
	// Init consumer group
	for _, topic := range s.cfg.Kafka.Topics {
		if s.kafka[topic], err = kgo.NewClient(
			kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
			kgo.ConsumeTopics(topic),
			kgo.ConsumerGroup(s.cfg.Kafka.GroupID),
			kgo.AutoCommitMarks(),
		); err != nil {
			return errors.Wrap(err, "Kafka: Init consumer group")
		}
		if err := s.kafka[topic].Ping(ctx); err != nil {
			return errors.Wrap(err, "Kafka: Ping brokers")
		}
	}

	if s.chConn, err = clickhouse.Open(&clickhouse.Options{
		Protocol: clickhouse.Native,
		Addr:     s.cfg.ClickHouse.Addrs,
		Auth: clickhouse.Auth{
			Database: s.cfg.ClickHouse.Database,
			Username: s.cfg.ClickHouse.User,
			Password: s.cfg.ClickHouse.Password,
		},
		MaxOpenConns: 10,
	}); err != nil {
		return errors.Wrap(err, "ClickHouse: Connect")
	}
	if err := s.chConn.Ping(ctx); err != nil {
		return errors.Wrap(err, "ClickHouse: Ping")
	}

	log.Info().Msg("Kafka: Listening to topics...")
	var wg sync.WaitGroup
	for topic, kafka := range s.kafka {
		wg.Add(1)
		go func(topic string, kafka *kgo.Client) {
			defer wg.Done()
			for {
				if err := s.poll(ctx, topic, kafka); err != nil {
					log.Error().Stack().Err(err).Msgf("Kafka: Poll topic %q", topic)
				}
			}
		}(topic, kafka)
	}

	wg.Wait()
	log.Info().Msg("Kafka: Disconnect")
	for _, kafka := range s.kafka {
		kafka.Close()
	}
	log.Info().Msg("ClickHouse: Disconnect")
	return s.chConn.Close()
}

func (s *Sink) poll(ctx context.Context, topic string, kafka *kgo.Client) error {
	// TODO fix
	pollCtx, cancel := context.WithTimeout(ctx, s.cfg.Kafka.Batch.Timeout)
	defer cancel()
	fetches := kafka.PollRecords(pollCtx, s.cfg.Kafka.Batch.Size)
	if err := fetches.Err(); err != nil {
		if errors.Is(err, context.DeadlineExceeded) {
			return nil
		}
		return errors.Wrap(err, "Kafka: Fetch error")
	}

	_, table, ok := strings.Cut(topic, ".")
	if !ok {
		return errors.Errorf("expected topic format {schema}.{table}, got %q", topic)
	}
	batch, err := s.chConn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%s`, s.cfg.ClickHouse.Database, table))
	if err != nil {
		return errors.Wrap(err, "ClickHouse: Prepare batch")
	}

	records := fetches.Records()
	for _, rec := range records {
		var value map[string]any
		if err := json.Unmarshal(rec.Value, &value); err != nil {
			return errors.Wrapf(err, "Kafka: Unmarshal %q into map", string(rec.Value))
		}
		if err := batch.AppendStruct(value); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}

	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "ClickHouse: Send batch")
	}
	kafka.MarkCommitRecords(records...)
	log.Info().Msgf("ClickHouse: batch (%d) is successfully sent", len(records))
	return nil
}
