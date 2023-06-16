package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"

	"kafka-pipe/pkg/connector"
	"kafka-pipe/pkg/connector/pg"
)

type Snapshot struct {
	cfg      Config
	db       *pgxpool.Pool
	prod     sarama.SyncProducer
	sarAdmin sarama.ClusterAdmin
}

func NewSnapshot(config connector.Config) (*Snapshot, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}
	return &Snapshot{cfg: cfg}, nil
}

func (s *Snapshot) Run(ctx context.Context) error {
	var err error
	if s.sarAdmin, err = sarama.NewClusterAdmin(s.cfg.Kafka.Brokers, nil); err != nil {
		return errors.Wrap(err, "init Kafka admin")
	}
	defer s.sarAdmin.Close()

	// Init Kafka producer
	sarCfg := sarama.NewConfig()
	sarCfg.Net.MaxOpenRequests = 1
	sarCfg.Producer.Idempotent = true
	sarCfg.Producer.RequiredAcks = sarama.WaitForAll
	sarCfg.Producer.Return.Successes = true
	sarCfg.Producer.Retry.Max = 10
	sarCfg.Producer.Retry.Backoff = 100 * time.Millisecond
	sarCfg.Metadata.AllowAutoTopicCreation = false
	if s.prod, err = sarama.NewSyncProducer(s.cfg.Kafka.Brokers, sarCfg); err != nil {
		return errors.Wrap(err, "init Kafka producer")
	}
	defer s.prod.Close()

	poolCfg, err := pgxpool.ParseConfig(s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "parse PostgreSQL connection URL")
	}
	poolCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pg.RegisterTypes(conn.TypeMap())
		return nil
	}
	if s.db, err = pgxpool.NewWithConfig(ctx, poolCfg); err != nil {
		return errors.Wrap(err, "connect to PostgreSQL")
	}
	defer s.db.Close()

	for _, table := range s.cfg.Pg.Tables {
		if err := s.run(ctx, table); err != nil {
			return err
		}
	}
	return nil
}

func (s *Snapshot) run(ctx context.Context, table string) error {
	select {
	case <-ctx.Done():
		return nil
	default:
	}

	// Create topic if not exists
	topic := s.cfg.Kafka.Topic.Prefix + "." + table
	if err := s.sarAdmin.CreateTopic(topic, &sarama.TopicDetail{
		NumPartitions:     s.cfg.Kafka.Topic.Partitions,
		ReplicationFactor: s.cfg.Kafka.Topic.ReplicationFactor,
		ConfigEntries: map[string]*string{
			"compression.type": &s.cfg.Kafka.Topic.CompressionType,
			"cleanup.policy":   &s.cfg.Kafka.Topic.CleanupPolicy,
		},
	}, false); err != nil {
		if !errors.Is(err, sarama.ErrTopicAlreadyExists) {
			return errors.Wrapf(err, "create topic %q", topic)
		}
	}

	res, err := s.query(ctx, table)
	if err != nil {
		return err
	}
	return s.handleQuery(ctx, table, res)
}

type queryResult struct {
	Count  int64
	Data   chan *sarama.ProducerMessage
	Errors chan error
}

func (s *Snapshot) query(ctx context.Context, table string) (*queryResult, error) {
	sql := fmt.Sprintf(`SELECT COUNT(data) FROM (SELECT * FROM %s %s) data`, table, s.cfg.Pg.Condition)
	log.Info().Msg("PostgreSQL: " + sql)
	rows, err := s.db.Query(ctx, sql)
	if err != nil {
		return nil, errors.Wrap(err, "query rows count")
	}
	count, err := pgx.CollectOneRow(rows, pgx.RowTo[int64])
	if err != nil {
		return nil, errors.Wrap(err, "query snapshot rows count")
	}

	sql = fmt.Sprintf(`SELECT data.*, count(data.*) OVER() AS total FROM (SELECT * FROM %s %s) data`, table, s.cfg.Pg.Condition)
	log.Info().Msg("PostgreSQL: " + sql)
	if rows, err = s.db.Query(ctx, sql); err != nil {
		return nil, errors.Wrap(err, "query snapshot")
	}

	res := &queryResult{
		Count:  count,
		Data:   make(chan *sarama.ProducerMessage, s.cfg.Kafka.BatchSize),
		Errors: make(chan error),
	}

	go func() {
		defer rows.Close()
		defer close(res.Data)

		for rows.Next() {
			data, err := pgx.RowToMap(rows)
			if err != nil {
				res.Errors <- errors.Wrap(err, "scan into map")
				return
			}
			value, err := json.Marshal(data)
			if err != nil {
				res.Errors <- errors.Wrapf(err, "marshal %+v", data)
				return
			}

			res.Data <- &sarama.ProducerMessage{
				Topic: s.cfg.Kafka.Topic.Prefix + "." + table,
				Key:   sarama.StringEncoder(fmt.Sprintf(`{"id": %q}`, data["id"])),
				Value: sarama.ByteEncoder(value),
			}
		}

		if err := rows.Err(); err != nil {
			res.Errors <- errors.Wrap(err, "rows error")
		}
	}()

	return res, nil
}

func (s *Snapshot) handleQuery(ctx context.Context, table string, res *queryResult) error {
	bar := progressbar.Default(res.Count, "Snapshot of "+table)
	batch := make([]*sarama.ProducerMessage, 0, s.cfg.Kafka.BatchSize)
MainLoop:
	for {
		select {
		case data, ok := <-res.Data:
			if ok {
				batch = append(batch, data)
				if len(batch) < s.cfg.Kafka.BatchSize {
					continue
				}
			}

			for {
				err := s.prod.SendMessages(batch)
				if err == nil {
					break
				}
				log.Err(err).Msg("Send to Kafka")

				select {
				case <-ctx.Done():
					return nil
				case <-time.After(5 * time.Second):
				}
			}

			_ = bar.Add(len(batch))

			if !ok {
				break MainLoop
			}
			batch = make([]*sarama.ProducerMessage, 0, s.cfg.Kafka.BatchSize)
		case err := <-res.Errors:
			_ = bar.Clear()
			if errors.Is(err, context.Canceled) {
				log.Info().Msg("Interrupt")
				return nil
			}
			return err
		}
	}

	return bar.Close()
}
