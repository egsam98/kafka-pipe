package kgox

import (
	"context"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
)

type Consumer struct {
	*kgo.Client
	cfg ConsumerConfig
	log Logger
}

type ConsumerConfig struct {
	Brokers, Topics        []string
	Group                  string
	BatchSize              uint
	FetchMaxBytes          uint // default 50MB
	FetchMaxPartitionBytes uint // default 1MB
	BatchTimeout           time.Duration
	RebalanceTimeout       time.Duration // default 1m
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	c := &Consumer{
		cfg: cfg,
		log: NewLogger(&log.Logger),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topics...),
		kgo.ConsumerGroup(cfg.Group),
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(&c.log),
	}
	if cfg.RebalanceTimeout > 0 {
		opts = append(opts, kgo.RebalanceTimeout(cfg.RebalanceTimeout))
	}
	if cfg.FetchMaxBytes > 0 {
		opts = append(opts, kgo.FetchMaxBytes(int32(cfg.FetchMaxBytes)))
	}
	if cfg.FetchMaxPartitionBytes > 0 {
		opts = append(opts, kgo.FetchMaxPartitionBytes(int32(cfg.FetchMaxPartitionBytes)))
	}

	var err error
	if c.Client, err = kgo.NewClient(opts...); err != nil {
		return nil, errors.Wrap(err, "Kafka: Init consumer")
	}
	if err := c.Ping(context.Background()); err != nil {
		return nil, errors.Wrap(err, "Kafka: Ping brokers")
	}
	return c, nil
}

type Handler func(ctx context.Context, fetches kgo.Fetches) error

func (c *Consumer) Listen(ctx context.Context, handler Handler) {
	for {
		if err := c.poll(ctx, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error().Stack().Err(err).Msgf("Kafka: Poll topics %v", c.cfg.Topics)
		}
	}
}

func (c *Consumer) poll(ctx context.Context, handler Handler) error {
	defer c.AllowRebalance()

	timer := time.NewTimer(c.cfg.BatchTimeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	batch := c.PollRecords(nil, int(c.cfg.BatchSize)) //nolint:staticcheck
	if len(batch) == 0 {
		return nil
	}
	if err := batch.Err(); err != nil {
		return errors.Wrap(err, "Kafka: Fetch error")
	}

	handleCtx, handleCancel := context.WithCancelCause(ctx)
	defer handleCancel(nil)

	go func() {
		select {
		case <-handleCtx.Done():
		case err := <-c.log.Errors():
			if errors.Is(err, kerr.RebalanceInProgress) {
				handleCancel(err)
			}
		}
	}()

	for {
		err := handler(handleCtx, batch)
		if err == nil {
			break
		}

		if !errors.Is(err, context.Canceled) {
			log.Error().Stack().Err(err).Send()
		}

		timer := time.NewTimer(5 * time.Second)
		select {
		case <-handleCtx.Done():
			timer.Stop()
			return context.Cause(handleCtx)
		case <-timer.C:
		}
	}

	err := c.CommitUncommittedOffsets(context.Background())
	return errors.Wrapf(err, "Kafka: commit offsets")
}
