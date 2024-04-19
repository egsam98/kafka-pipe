package sink

import (
	"cmp"
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/ClickHouse/clickhouse-go/v2/lib/proto"
	"github.com/dgraph-io/badger/v4"
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
				Brokers:          s.cfg.Kafka.Brokers,
				Topics:           []string{topic},
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

type batchState struct {
	sync.Mutex
	name    string
	offsets map[string]map[int32]int64
	db      *badger.DB
}

func newBatchState(name string, db *badger.DB) (*batchState, error) {
	state := batchState{
		name:    name,
		offsets: make(map[string]map[int32]int64),
		db:      db,
	}
	if err := db.View(func(tx *badger.Txn) error {
		item, err := tx.Get(state.key())
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
		return tx.Set(b.key(), value)
	}); err != nil {
		log.Warn().Err(err).Msgf("Badger: Failed to save batch state")
	}
}

func (b *batchState) key() []byte {
	return fmt.Appendf(nil, "%s/batch_state", b.name)
}
