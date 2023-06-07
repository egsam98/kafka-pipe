package snapshot

import (
	"context"
	"fmt"
	"time"

	"github.com/Shopify/sarama"
	"github.com/georgysavva/scany/v2/pgxscan"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/schollz/progressbar/v3"

	"kafka-pipe/pkg/connector"
	"kafka-pipe/pkg/saramax"
)

type Snapshot struct {
	cfg      Config
	db       *pgxpool.Pool
	prod     sarama.SyncProducer
	sarAdmin sarama.ClusterAdmin
	bar      *progressbar.ProgressBar
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

	if s.db, err = pgxpool.New(ctx, s.cfg.Pg.Url); err != nil {
		return errors.Wrap(err, "connect to PostgreSQL")
	}
	defer s.db.Close()

	for _, table := range s.cfg.Pg.Tables {
		select {
		case <-ctx.Done():
			break
		default:
			if err := s.run(ctx, table); err != nil {
				return err
			}
		}
	}
	return nil
}

func (s *Snapshot) run(ctx context.Context, table string) error {
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

	resultCh, err := s.query(ctx, table)
	if err != nil {
		return err
	}

	batch := make([]*sarama.ProducerMessage, 0, s.cfg.Kafka.Batch.Size)
	for {
		res, ok := <-resultCh
		if ok {
			batch = append(batch, &sarama.ProducerMessage{
				Topic: topic,
				Key:   sarama.StringEncoder(fmt.Sprintf(`{"id": %q}`, res.Data["id"])),
				Value: saramax.JsonEncoder(res.Data),
			})
			if len(batch) < s.cfg.Kafka.Batch.Size {
				continue
			}
		}

		for {
			err := s.prod.SendMessages(batch)
			if err == nil {
				break
			}
			log.Err(err).Msg("Send to Kafka")
			time.Sleep(time.Second)
		}

		if err := s.bar.Add(len(batch)); err != nil {
			return err
		}

		if !ok {
			break
		}
		batch = make([]*sarama.ProducerMessage, 0, s.cfg.Kafka.Batch.Size)
	}

	select {
	case <-ctx.Done():
		s.bar.Exit()
	default:
		s.bar.Close()
	}
	log.Info().Msg("Snapshot finished")
	return nil
}

type scanResult struct {
	Data map[string]any
	Err  error
}

func (s *Snapshot) query(ctx context.Context, table string) (<-chan scanResult, error) {
	sql := fmt.Sprintf(`SELECT data.*, count(data.*) OVER() AS total FROM (SELECT * FROM %s %s) data`, table, s.cfg.Pg.Condition)
	log.Info().Msg(sql)
	rows, err := s.db.Query(ctx, sql)
	if err != nil {
		return nil, errors.Wrap(err, "query snapshot")
	}

	msgs := make(chan scanResult)
	go func() {
		defer rows.Close()
		defer close(msgs)

		for rows.Next() {
			var m map[string]any
			if err := pgxscan.ScanRow(&m, rows); err != nil {
				msgs <- scanResult{Err: errors.Wrap(err, "scan")}
				return
			}
			if s.bar == nil {
				s.bar = progressbar.Default(m["total"].(int64))
			}
			msgs <- scanResult{Data: m}
		}

		if err := rows.Err(); err != nil {
			msgs <- scanResult{Err: errors.Wrap(err, "rows error")}
		}
	}()
	return msgs, nil
}
