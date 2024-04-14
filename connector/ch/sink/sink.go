package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"strings"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-pipe/internal/kgox"
	"kafka-pipe/internal/validate"
)

type Sink struct {
	i         int // TODO temp
	cfg       Config
	consumers []consumer
	chConn    driver.Conn
}

func NewSink(cfg Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}

	// Init consumer group
	for _, topic := range s.cfg.Kafka.Topics {
		for range s.cfg.Kafka.WorkersPerTopic {
			kafka, err := kgo.NewClient(
				kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
				kgo.ConsumeTopics(topic),
				kgo.ConsumerGroup(s.cfg.Kafka.GroupID+"-"+topic),
				kgo.BlockRebalanceOnPoll(),
				kgo.RebalanceTimeout(s.cfg.Kafka.RebalanceTimeout),
				kgo.DisableAutoCommit(),
				kgo.OnPartitionsAssigned(func(ctx context.Context, client *kgo.Client, m map[string][]int32) {
					log.Info().Msgf("PARTITION ASSIGNED %v", m)
				}),
				kgo.OnPartitionsRevoked(func(ctx context.Context, client *kgo.Client, m map[string][]int32) {
					log.Info().Msgf("PARTITION REVOKED %v", m)
				}),
				kgo.WithLogger(&kgox.Logger{Logger: log.Logger}),
			)
			if err != nil {
				return errors.Wrap(err, "Kafka: Init consumer")
			}
			if err := kafka.Ping(ctx); err != nil {
				return errors.Wrap(err, "Kafka: Ping brokers")
			}

			s.consumers = append(s.consumers, consumer{
				Client: kafka,
				Topic:  topic,
			})
		}
	}

	var err error
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
	for _, consum := range s.consumers {
		wg.Add(1)
		go func(consum consumer) {
			defer wg.Done()
			for {
				if err := s.poll(ctx, &consum); err != nil {
					if errors.Is(err, context.Canceled) {
						return
					}
					log.Error().Stack().Err(err).Msgf("Kafka: Poll topic %q", consum.Topic)
				}
			}
		}(consum)
	}

	wg.Wait()
	log.Info().Msg("Kafka: Disconnect")
	for _, consum := range s.consumers {
		consum.Close()
	}
	log.Info().Msg("ClickHouse: Disconnect")
	return s.chConn.Close()
}

func (s *Sink) poll(ctx context.Context, consum *consumer) error {
	// TODO rework
	_, table, ok := strings.Cut(consum.Topic, ".")
	if !ok {
		return errors.Errorf("expected topic format {schema}.{table}, got %q", consum.Topic)
	}

	t := time.Now()

	defer consum.AllowRebalance()

	var batch []*kgo.Record
	pollCtx, cancel := context.WithTimeout(ctx, s.cfg.Kafka.Batch.Timeout)
	defer cancel()
	for len(batch) < s.cfg.Kafka.Batch.Size {
		fetches := consum.PollRecords(pollCtx, s.cfg.Kafka.Batch.Size-len(batch))
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return errors.Wrap(err, "Kafka: Fetch error")
		}
		batch = append(batch, fetches.Records()...)
	}
	log.Info().Msgf("TIME %s BATCH %d", time.Since(t), len(batch)) // TODO

	if len(batch) == 0 {
		return nil
	}

	for {
		err := s.writeToCH(ctx, table, batch)
		if err == nil {
			break
		}

		if !errors.Is(err, context.Canceled) {
			log.Error().Stack().Err(err).Send()
		}
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(5 * time.Second):
		}
	}

	if err := consum.CommitRecords(context.Background(), batch...); err != nil {
		return errors.Wrapf(err, "Kafka: commit records")
	}
	log.Info().Int("size", len(batch)).Msg("ClickHouse: batch is successfully sent")
	return nil
}

func (s *Sink) writeToCH(ctx context.Context, table string, records []*kgo.Record) error {
	// TODO
	log.Info().Msg("PROCESS BATCH")
	if s.i <= 500 {
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(20 * time.Second):
		}
	}
	s.i++
	return nil

	batch, err := s.chConn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%s`, s.cfg.ClickHouse.Database, table))
	if err != nil {
		return errors.Wrap(err, "ClickHouse: Prepare batch")
	}

	for _, rec := range records {
		var value map[string]any
		if err := json.Unmarshal(rec.Value, &value); err != nil {
			return errors.Wrapf(err, "Kafka: Unmarshal %q into map", string(rec.Value))
		}
		if err := batch.AppendStruct(value); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}

	return errors.Wrap(batch.Send(), "ClickHouse: Send batch")
}

type consumer struct {
	*kgo.Client
	Topic string
}
