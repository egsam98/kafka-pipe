package sink

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/egsam98/kafka-pipe/internal/kgox"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Sink struct {
	cfg        Config
	consumers  []*kgox.Consumer
	chConn     driver.Conn
	batchState *batchState
}

func NewSink(cfg Config) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}
	var err error
	if s.batchState, err = newBatchState(s.cfg.Name, s.cfg.DB); err != nil {
		return err
	}

	// Init consumer group
	for _, topic := range s.cfg.Kafka.Topics {
		for range s.cfg.Kafka.WorkersPerTopic {
			consum, err := kgox.NewConsumer(kgox.ConsumerConfig{
				Brokers:                s.cfg.Kafka.Brokers,
				Topics:                 []string{topic},
				Group:                  s.cfg.Name + "-" + topic,
				FetchMaxBytes:          s.cfg.Kafka.FetchMaxBytes,
				FetchMaxPartitionBytes: s.cfg.Kafka.FetchMaxPartitionBytes,
				BatchSize:              s.cfg.Kafka.Batch.Size,
				BatchTimeout:           s.cfg.Kafka.Batch.Timeout,
				RebalanceTimeout:       s.cfg.Kafka.RebalanceTimeout,
			})
			if err != nil {
				return err
			}
			s.consumers = append(s.consumers, consum)
		}
	}

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

func (s *Sink) writeToCH(ctx context.Context, fetches kgo.Fetches) error {
	var records []*kgo.Record
	s.batchState.Lock()
	fetches.EachPartition(func(fs kgo.FetchTopicPartition) {
		if len(fs.Records) == 0 {
			return
		}

		idx := -1
		if part, ok := s.batchState.offsets[fs.Topic]; ok {
			diff := int(cmp.Or(part[fs.Partition], -1) - fs.Records[0].Offset)
			if diff >= len(fs.Records)-1 {
				return
			}
			if diff >= 0 {
				idx = diff
			}
		} else {
			s.batchState.offsets[fs.Topic] = make(map[int32]int64)
		}

		records = append(records, fs.Records[idx+1:]...)
	})
	s.batchState.Unlock()
	if len(records) == 0 {
		return nil
	}

	if s.cfg.BeforeInsert != nil {
		if err := s.cfg.BeforeInsert(ctx, records); err != nil {
			return errors.Wrap(err, "BeforeInsert")
		}
	}

	batch, err := s.chConn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%q`, s.cfg.ClickHouse.Database, records[0].Topic))
	if err != nil {
		return errors.Wrap(err, "ClickHouse: Prepare batch")
	}
	defer batch.Abort() //nolint:errcheck

	structType, err := tableSchema(batch.Block())
	if err != nil {
		return err
	}

	for _, rec := range records {
		if rec == nil {
			continue
		}
		data := reflect.New(structType).Interface()
		if err := s.cfg.Deserializer.Deserialize(data, rec.Value); err != nil {
			return errors.Wrapf(err, "Deserialize message %q using %T", string(rec.Value), s.cfg.Deserializer)
		}
		if err := batch.AppendStruct(data); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}
	if err := batch.Send(); err != nil {
		return errors.Wrap(err, "ClickHouse: Send batch")
	}

	s.batchState.Lock()
	fetches.EachPartition(func(fs kgo.FetchTopicPartition) {
		if len(fs.Records) == 0 {
			return
		}
		s.batchState.offsets[fs.Topic][fs.Partition] = fs.Records[len(fs.Records)-1].Offset
	})
	s.batchState.Unlock()

	log.Info().Int("size", len(records)).Msg("ClickHouse: batch is successfully sent")
	return nil
}

func tableSchema(block proto.Block) (reflect.Type, error) {
	fields := make([]reflect.StructField, len(block.Columns))
	for i, col := range block.Columns {
		name := col.Name()
		fields[i] = reflect.StructField{
			Name: cases.Title(language.Und).String(name),
			Type: col.ScanType(),
			Tag:  reflect.StructTag(fmt.Sprintf("json:%q ch:%q", name, name)),
		}
	}
	return reflect.StructOf(fields), nil
}

type batchState struct {
	sync.Mutex
	dbKey   []byte
	db      *badger.DB
	offsets map[string]map[int32]int64
}

func newBatchState(name string, db *badger.DB) (*batchState, error) {
	state := batchState{
		dbKey:   fmt.Appendf(nil, "%s/batch_state", name),
		db:      db,
		offsets: make(map[string]map[int32]int64),
	}

	if err := db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(state.dbKey)
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return json.Unmarshal(val, &state.offsets)
		})
	}); err != nil && !errors.Is(err, badger.ErrKeyNotFound) {
		return nil, errors.Wrap(err, "Badger: get batch state")
	}
	return &state, nil
}

func (b *batchState) Unlock() {
	defer b.Mutex.Unlock()
	if err := b.db.Update(func(tx *badger.Txn) error {
		value, err := json.Marshal(b.offsets)
		if err != nil {
			return err
		}
		return tx.Set(b.dbKey, value)
	}); err != nil {
		log.Warn().Err(err).Msgf("Badger: Failed to save batch state")
	}
}
