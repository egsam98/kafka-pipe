package sink

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/google/uuid"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-pipe/internal/kgox"
	"kafka-pipe/internal/validate"
)

type Sink struct {
	i         int // TODO temp
	cfg       Config
	consumers []*kgox.Consumer
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
			consum, err := kgox.NewConsumer(kgox.ConsumerConfig{
				Brokers:          s.cfg.Kafka.Brokers,
				Topic:            topic,
				Group:            s.cfg.Kafka.GroupID + "-" + topic,
				BatchSize:        s.cfg.Kafka.Batch.Size,
				BatchTimeout:     s.cfg.Kafka.Batch.Timeout,
				RebalanceTimeout: s.cfg.Kafka.RebalanceTimeout,
				Handler:          s.writeToCH,
			})
			if err != nil {
				return err
			}
			s.consumers = append(s.consumers, consum)
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
		go func(consum *kgox.Consumer) {
			defer wg.Done()
			consum.Listen(ctx)
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

func (s *Sink) writeToCH(ctx context.Context, records []*kgo.Record) error {
	table := records[0].Topic

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
		//var value map[string]any
		//if err := json.Unmarshal(rec.Value, &value); err != nil {
		//	return errors.Wrapf(err, "Kafka: Unmarshal %q into map", string(rec.Value))
		//}

		//var id struct {
		//	Value string `json:"id"`
		//}
		//if err := json.Unmarshal(rec.Key, &id); err != nil {
		//	return errors.Wrapf(err, "Kafka: Unmarshal %q into %T", string(rec.Key), id)
		//}
		var key kgox.Key[uuid.UUID]
		if err := key.Parse(rec.Key); err != nil {
			return err
		}

		if err := batch.Append(key.Value, rec.Value); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}

	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "ClickHouse: Send batch")
	}
	log.Info().Int("size", len(records)).Msg("ClickHouse: batch is successfully sent")
	return nil
}
