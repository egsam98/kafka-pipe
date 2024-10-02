package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
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

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Snapshot struct {
	cfg           SnapshotConfig
	db            *pgxpool.Pool
	pgCfg         pgxpool.Config
	kafka         *kgo.Client
	topicResolver *topicResolver
}

func NewSnapshot(cfg SnapshotConfig) *Snapshot {
	return &Snapshot{cfg: cfg}
}

func (s *Snapshot) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}

	var err error
	if s.topicResolver, err = newTopicResolver(&s.cfg.Kafka); err != nil {
		return err
	}

	pgCfg, err := pgxpool.ParseConfig(s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "parse PostgreSQL connection URL")
	}
	pgCfg.AfterConnect = func(ctx context.Context, conn *pgx.Conn) error {
		registerTypes(conn.TypeMap())
		return nil
	}
	s.pgCfg = *pgCfg

	// Init Kafka client
	if s.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
		kgo.MaxBufferedRecords(int(s.cfg.Kafka.Batch.Size)),
		kgo.ProducerLinger(s.cfg.Kafka.Batch.Timeout),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
	); err != nil {
		return errors.Wrap(err, "init Kafka client")
	}
	if err := s.kafka.Ping(ctx); err != nil {
		return errors.Wrap(err, "ping Kafka client")
	}

	topicMapCfg, err := s.cfg.Kafka.TopicMapConfig()
	if err != nil {
		return err
	}
	kafkaAdmin := kadm.NewClient(s.kafka)
	// Create topics if not exist
	for _, table := range s.cfg.Pg.Tables {
		topic := s.topicResolver.resolve(table)
		if _, err := kafkaAdmin.CreateTopic(
			ctx,
			int32(s.cfg.Kafka.Topic.Partitions),
			int16(s.cfg.Kafka.Topic.ReplicationFactor),
			topicMapCfg,
			topic,
		); err != nil && !errors.Is(err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(err, "create topic %q", topic)
		}
	}

	if s.db, err = pgxpool.NewWithConfig(ctx, pgCfg); err != nil {
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
	// Count rows
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

	// Find primary key's column names
	schema, tableNoSchema, ok := strings.Cut(table, ".")
	if !ok {
		return errors.Errorf("invalid table name: %s. Expected template {schema}.{table}", table)
	}
	sql = `SELECT kcu.column_name FROM information_schema.table_constraints tc
		JOIN information_schema.key_column_usage kcu ON kcu.constraint_name = tc.constraint_name
		WHERE tc.constraint_type = 'PRIMARY KEY' AND tc.table_schema = $1 AND tc.table_name = $2`
	if rows, err = s.db.Query(ctx, sql, schema, tableNoSchema); err != nil {
		return errors.Wrap(err, "Postgres: find primary keys")
	}
	primaryKeys, err := pgx.CollectRows(rows, pgx.RowTo[string])
	if err != nil {
		return errors.Wrap(err, "Postgres: find primary keys")
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
		value, err := pgx.RowToMap(rows)
		if err != nil {
			produceErr = errors.Wrap(err, "scan into map")
			break
		}

		key := make(map[string]any)
		for _, pk := range primaryKeys {
			key[pk] = value[pk]
		}
		keyBytes, err := json.Marshal(key)
		if err != nil {
			produceErr = errors.Wrapf(err, "marshal Kafka key: %+v", key)
			break
		}
		valueBytes, err := json.Marshal(value)
		if err != nil {
			produceErr = errors.Wrapf(err, "marshal Kafka value: %+v", value)
			break
		}
		if produceErr != nil {
			break
		}

		wg.Add(1)
		s.kafka.Produce(ctx, &kgo.Record{
			Key:   keyBytes,
			Value: valueBytes,
			Topic: s.topicResolver.resolve(table),
			Headers: []kgo.RecordHeader{
				{
					Key:   "version",
					Value: []byte(kafkapipe.Version),
				},
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
