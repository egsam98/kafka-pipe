package snapshot

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"sync"
	"sync/atomic"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	"kafka-pipe/connector/pg"
)

type Snapshot struct {
	cfg   Config
	db    *pgxpool.Pool
	pgCfg pgxpool.Config
	kafka *kgo.Client
}

func NewSnapshot(cfg Config) (*Snapshot, error) {
	pgCfg, err := pgxpool.ParseConfig(cfg.Pg.Url)
	if err != nil {
		return nil, errors.Wrap(err, "parse PostgreSQL connection URL")
	}
	pgCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		pg.RegisterTypes(conn.TypeMap())
		return nil
	}
	return &Snapshot{cfg: cfg, pgCfg: *pgCfg}, nil
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

	if s.db, err = pgxpool.NewWithConfig(ctx, &s.pgCfg); err != nil {
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

	var (
		bar        = pb.StartNew(count)
		produceErr error
		once       atomic.Bool
		wg         sync.WaitGroup
	)

	for rows.Next() {
		data, err := pgx.RowToMap(rows)
		if err != nil {
			produceErr = errors.Wrap(err, "scan into map")
			break
		}
		key, err := pg.KafkaKey(data)
		if err != nil {
			produceErr = errors.Wrap(err, "scan into map")
			break
		}
		value, err := json.Marshal(data)
		if err != nil {
			produceErr = errors.Wrapf(err, "marshal %+v", data)
			break
		}
		if produceErr != nil {
			break
		}

		wg.Add(1)
		s.kafka.Produce(ctx, &kgo.Record{
			Key:   key,
			Value: value,
			Topic: s.cfg.Kafka.Topic.Prefix + "." + table,
			Headers: []kgo.RecordHeader{
				{
					Key:   "ts_ms",
					Value: []byte(strconv.FormatInt(time.Now().UnixMilli(), 10)),
				},
				{
					Key:   "host",
					Value: []byte(s.pgCfg.ConnConfig.Host),
				},
				{
					Key:   "port",
					Value: []byte(strconv.FormatUint(uint64(s.pgCfg.ConnConfig.Port), 10)),
				},
				{
					Key:   "database",
					Value: []byte(s.pgCfg.ConnConfig.Database),
				},
				{
					Key:   "table",
					Value: []byte(table),
				},
			},
		}, func(_ *kgo.Record, err error) {
			defer wg.Done()
			if err == nil {
				bar.Increment()
				return
			}
			if !once.Swap(true) {
				produceErr = err
				wg.Add(1)
				go func() {
					defer wg.Done()
					_ = s.kafka.AbortBufferedRecords(context.Background())
				}()
			}
		})
	}

	wg.Wait()
	if produceErr != nil {
		return produceErr
	}
	if err := rows.Err(); err != nil {
		return errors.Wrap(err, "rows error")
	}
	bar.Finish()
	return nil
}
