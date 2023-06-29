package snapshot

import (
	"context"
	"encoding/json"
	"fmt"

	"github.com/cheggaaa/pb/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-pipe/pkg/connector"
	"kafka-pipe/pkg/connector/pg"
)

type Snapshot struct {
	cfg   Config
	db    *pgxpool.Pool
	kafka *kgo.Client
	errs  chan error
}

func NewSnapshot(config connector.Config) (*Snapshot, error) {
	var cfg Config
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}
	return &Snapshot{
		cfg:  cfg,
		errs: make(chan error),
	}, nil
}

func (s *Snapshot) Run(ctx context.Context) error {
	// Init Kafka client
	var err error
	if s.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
		kgo.MaxBufferedRecords(s.cfg.Kafka.Batch.Size),
		kgo.ProducerLinger(s.cfg.Kafka.Batch.Timeout),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	); err != nil {
		return errors.Wrap(err, "init Kafka client")
	}
	if err := s.kafka.Ping(ctx); err != nil {
		return errors.Wrap(err, "ping Kafka client")
	}

	kafkaAdmin := kadm.NewClient(s.kafka)
	// Create topics if not exist
	for _, table := range s.cfg.Pg.Tables {
		topic := s.cfg.Kafka.Topic.Prefix + "." + table
		res, err := kafkaAdmin.CreateTopic(ctx, s.cfg.Kafka.Topic.Partitions, s.cfg.Kafka.Topic.ReplicationFactor, map[string]*string{
			"compression.type": &s.cfg.Kafka.Topic.CompressionType,
			"cleanup.policy":   &s.cfg.Kafka.Topic.CleanupPolicy,
		}, topic)
		if err != nil && !errors.Is(res.Err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(err, "create topic %q", topic)
		}
	}

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

	for _, table := range s.cfg.Pg.Tables {
		if err := s.query(ctx, table); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
	}

	log.Info().Msg("Kafka: Close producer")
	s.kafka.Close()
	log.Info().Msg("PostgreSQL: Close")
	s.db.Close()
	return nil
}

func (s *Snapshot) query(ctx context.Context, table string) error {
	sql := fmt.Sprintf(`SELECT COUNT(data) FROM (SELECT 1 FROM %s %s) data`, table, s.cfg.Pg.Condition)
	log.Info().Msg("PostgreSQL: " + sql)
	rows, err := s.db.Query(ctx, sql)
	if err != nil {
		return errors.Wrap(err, "query rows count")
	}
	count, err := pgx.CollectOneRow(rows, pgx.RowTo[int])
	if err != nil {
		return errors.Wrap(err, "query snapshot rows count")
	}

	sql = fmt.Sprintf(`SELECT data.*, count(data.*) OVER() AS total FROM (SELECT * FROM %s %s) data`, table, s.cfg.Pg.Condition)
	log.Info().Msg("PostgreSQL: " + sql)
	if rows, err = s.db.Query(ctx, sql); err != nil {
		return errors.Wrap(err, "query snapshot")
	}
	defer rows.Close()

	bar := pb.StartNew(count)
	produceErr := make(chan error, 1)

	for rows.Next() {
		select {
		case err := <-produceErr:
			if err := s.kafka.AbortBufferedRecords(context.Background()); err != nil {
				log.Err(err).Msgf("Kafka: Abort buffered records")
			}
			return err
		default:
		}

		data, err := pgx.RowToMap(rows)
		if err != nil {
			return errors.Wrap(err, "scan into map")
		}
		key, err := pg.KafkaKey(data)
		if err != nil {
			return err
		}
		value, err := json.Marshal(data)
		if err != nil {
			return errors.Wrapf(err, "marshal %+v", data)
		}

		s.kafka.Produce(ctx, &kgo.Record{
			Key:   key,
			Value: value,
			Topic: s.cfg.Kafka.Topic.Prefix + "." + table,
		}, func(_ *kgo.Record, err error) {
			if err != nil {
				select {
				case produceErr <- err:
				default:
				}
				return
			}
			bar.Increment()
		})
	}

	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows error")
	}
	if err := s.kafka.Flush(context.Background()); err != nil {
		return errors.Wrap(err, "flush Kafka records")
	}
	bar.Finish()
	return nil
}
