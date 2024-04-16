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
	Brokers                        []string
	Topic, Group                   string
	BatchSize                      int
	BatchTimeout, RebalanceTimeout time.Duration
}

func NewConsumer(cfg ConsumerConfig) (*Consumer, error) {
	c := &Consumer{
		cfg: cfg,
		log: NewLogger(&log.Logger),
	}

	var err error
	if c.Client, err = kgo.NewClient(
		kgo.SeedBrokers(cfg.Brokers...),
		kgo.ConsumeTopics(cfg.Topic),
		kgo.ConsumerGroup(cfg.Group),
		kgo.BlockRebalanceOnPoll(),
		kgo.RebalanceTimeout(cfg.RebalanceTimeout),
		kgo.DisableAutoCommit(),
		kgo.WithLogger(&c.log),
	); err != nil {
		return nil, errors.Wrap(err, "Kafka: Init consumer")
	}
	if err := c.Ping(context.Background()); err != nil {
		return nil, errors.Wrap(err, "Kafka: Ping brokers")
	}
	return c, nil
}

type Handler func(ctx context.Context, batch []*kgo.Record) error

func (c *Consumer) Listen(ctx context.Context, handler Handler) {
	for {
		if err := c.poll(ctx, handler); err != nil {
			if errors.Is(err, context.Canceled) {
				return
			}
			log.Error().Stack().Err(err).Msgf("Kafka: Poll topic %q", c.cfg.Topic)
		}
	}
}

func (c *Consumer) poll(ctx context.Context, handler Handler) error {
	defer c.AllowRebalance()

	var batch []*kgo.Record
	pollCtx, pollCancel := context.WithTimeout(ctx, c.cfg.BatchTimeout)
	defer pollCancel()
	for len(batch) < c.cfg.BatchSize {
		fetches := c.PollRecords(pollCtx, c.cfg.BatchSize-len(batch))
		if err := fetches.Err(); err != nil {
			if errors.Is(err, context.DeadlineExceeded) {
				break
			}
			return errors.Wrap(err, "Kafka: Fetch error")
		}
		batch = append(batch, fetches.Records()...)
	}

	if len(batch) == 0 {
		return nil
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

	err := c.CommitRecords(context.Background(), batch...) // TODO
	return errors.Wrapf(err, "Kafka: commit records")
}
