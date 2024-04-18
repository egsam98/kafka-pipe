package sink

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"kafka-pipe/internal/kgox"
	"kafka-pipe/internal/timex"
	"kafka-pipe/internal/validate"
)

type Sink struct {
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
			consum.Listen(ctx, s.writeToCH)
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
	// TODO
	//log.Info().Msg("PROCESS BATCH")
	//if s.i <= 500 {
	//	select {
	//	case <-ctx.Done():
	//		return ctx.Err()
	//	case <-time.After(20 * time.Second):
	//	}
	//}
	//s.i++
	//return nil

	table := records[0].Topic
	if s.cfg.OnProcess != nil {
		if err := s.cfg.OnProcess(ctx, records); err != nil {
			return err
		}
	}

	batch, err := s.chConn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%q`, s.cfg.ClickHouse.Database, table))
	if err != nil {
		return errors.Wrap(err, "ClickHouse: Prepare batch")
	}
	structType, err := s.tableSchema(batch.Block())
	if err != nil {
		return err
	}

	for _, rec := range records {
		data := reflect.New(structType).Interface()
		if err := json.Unmarshal(rec.Value, data); err != nil {
			return errors.Wrapf(err, "Kafka: Unmarshal %q into %T", string(rec.Value), data)
		}
		if err := batch.AppendStruct(data); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}

	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "ClickHouse: Send batch")
	}
	log.Info().Int("size", len(records)).Msg("ClickHouse: batch is successfully sent")
	return nil
}

func (s *Sink) tableSchema(block proto.Block) (reflect.Type, error) {
	fields := make([]reflect.StructField, len(block.Columns))
	for i, col := range block.Columns {
		var fieldType reflect.Type
		switch col.(type) {
		case *column.DateTime:
			fieldType = reflect.TypeFor[timex.UnixMicro]()
		default:
			fieldType = col.ScanType()
		}

		name := col.Name()
		fields[i] = reflect.StructField{
			Name: cases.Title(language.Und).String(name),
			Type: fieldType,
			Tag:  reflect.StructTag(fmt.Sprintf("json:%q ch:%q", name, name)),
		}
	}
	return reflect.StructOf(fields), nil
}
