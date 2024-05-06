package kgox

import (
	"context"
	"sync"
	"time"

	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

type ConsumerPool []consumer

func NewConsumerPool(cfg kafkapipe.ConsumerPoolConfig) (ConsumerPool, error) {
	pool := make(ConsumerPool, 0, len(cfg.Topics)*int(cfg.WorkersPerTopic))
	for _, topic := range cfg.Topics {
		for range cfg.WorkersPerTopic {
			consum, err := newConsumer(consumerConfig{
				Brokers:                cfg.Brokers,
				Topic:                  topic,
				Group:                  cfg.GroupPrefix + "-" + topic,
				FetchMaxBytes:          cfg.FetchMaxBytes,
				FetchMaxPartitionBytes: cfg.FetchMaxPartitionBytes,
				RebalanceTimeout:       cfg.RebalanceTimeout,
				Batch:                  cfg.Batch,
			})
			if err != nil {
				return nil, err
			}
			pool = append(pool, *consum)
		}
	}
	return pool, nil
}

func (c ConsumerPool) Listen(ctx context.Context, h Handler) {
	var wg sync.WaitGroup
	for _, consum := range c {
		wg.Add(1)
		go func(consum *consumer) {
			defer wg.Done()
			consum.Listen(ctx, h)
		}(&consum)
	}
	wg.Wait()
}

func (c ConsumerPool) Close() {
	for _, consum := range c {
		consum.Close()
	}
}

type consumer struct {
	*kgo.Client
	topic    string
	batchCfg kafkapipe.BatchConfig
	log      *logger
}

type consumerConfig struct {
	Brokers                []string
	Group, Topic           string
	FetchMaxBytes          uint          // default 50MB
	FetchMaxPartitionBytes uint          // default 1MB
	RebalanceTimeout       time.Duration // default 1m
	Batch                  kafkapipe.BatchConfig
}

func newConsumer(cfg consumerConfig) (*consumer, error) {
	c := &consumer{
		topic:    cfg.Topic,
		batchCfg: cfg.Batch,
		log:      newLogger(&log.Logger),
	}

	opts := []kgo.Opt{
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.BlockRebalanceOnPoll(),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(c.log),
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

func (c *consumer) Listen(ctx context.Context, handler Handler) {
	for {
		if err := c.poll(ctx, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error().Stack().Err(err).Msgf("Kafka: Poll topic %q", c.topic)
		}
	}
}

func (c *consumer) poll(ctx context.Context, handler Handler) error {
	defer c.AllowRebalance()

	timer := time.NewTimer(c.batchCfg.Timeout)
	defer timer.Stop()
	select {
	case <-ctx.Done():
		return ctx.Err()
	case <-timer.C:
	}

	batch := c.PollRecords(nil, int(c.batchCfg.Size)) //nolint:staticcheck
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
		case err := <-c.log.errors():
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
