package ch

import (
	"context"
	"encoding/json"
	"fmt"
	"reflect"
	"sync"

	"github.com/ClickHouse/clickhouse-go/v2"
	"github.com/ClickHouse/clickhouse-go/v2/lib/column"
	"github.com/ClickHouse/clickhouse-go/v2/lib/driver"
	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/text/cases"
	"golang.org/x/text/language"

	"github.com/egsam98/kafka-pipe/internal/kgox"
	"github.com/egsam98/kafka-pipe/internal/router"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Sink struct {
	cfg        SinkConfig
	consumPool kgox.ConsumerPool
	chConn     driver.Conn
	router     *router.Router
	batchState *batchState
}

func NewSink(cfg SinkConfig) *Sink {
	return &Sink{cfg: cfg}
}

func (s *Sink) Run(ctx context.Context) error {
	if err := validate.Struct(&s.cfg); err != nil {
		return err
	}
	var err error
	if s.router, err = router.NewRouter(s.cfg.Routes); err != nil {
		return err
	}
	if s.batchState, err = newBatchState(s.cfg.Name, s.cfg.DB); err != nil {
		return err
	}

	// Init consumer pool
	if s.consumPool, err = kgox.NewConsumerPool(s.cfg.Kafka); err != nil {
		return err
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
	s.consumPool.Listen(ctx, s.chWrite)
	log.Info().Msg("Kafka: Disconnect")
	s.consumPool.Close()
	log.Info().Msg("ClickHouse: Disconnect")
	return s.chConn.Close()
}

func (s *Sink) chWrite(ctx context.Context, fetches kgo.Fetches) error {
	var records []*kgo.Record
	s.batchState.Lock()
	fetches.EachPartition(func(fs kgo.FetchTopicPartition) {
		if len(fs.Records) == 0 {
			return
		}

		idx := -1
		if part, ok := s.batchState.offsets[fs.Topic]; ok {
			offset, ok := part[fs.Partition]
			if !ok {
				offset = -1
			}
			diff := int(offset - fs.Records[0].Offset)
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

	table := s.router.Route(records[0].Topic)
	batch, err := s.chConn.PrepareBatch(ctx, fmt.Sprintf(`INSERT INTO %s.%q`, s.cfg.ClickHouse.Database, table))
	if err != nil {
		return errors.Wrap(err, "ClickHouse: Prepare batch")
	}
	defer batch.Abort() //nolint:errcheck

	// Parse record values and append to ClickHouse batch
	if s.cfg.BeforeInsert != nil {
		err = s.chBatchStatic(ctx, batch, records)
	} else {
		err = s.chBatchReflect(batch, records)
	}
	if err != nil {
		return err
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

func (s *Sink) chBatchStatic(ctx context.Context, batch driver.Batch, records []*kgo.Record) error {
	values, err := s.cfg.BeforeInsert(ctx, s.cfg.Serde, records)
	if err != nil {
		return errors.Wrap(err, "BeforeInsert")
	}
	for _, value := range values {
		if err := batch.AppendStruct(value); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}
	return nil
}

func (s *Sink) chBatchReflect(batch driver.Batch, records []*kgo.Record) error {
	structType, err := tableSchema(batch.Columns(), s.cfg.Serde.Tag())
	if err != nil {
		return err
	}

	for _, rec := range records {
		data := reflect.New(structType).Interface()
		if err := s.cfg.Serde.Deserialize(data, rec.Topic, rec.Value); err != nil {
			return errors.Wrapf(err, "Deserialize message %q using %T", string(rec.Value), s.cfg.Serde)
		}
		if err := batch.AppendStruct(data); err != nil {
			return errors.Wrap(err, "ClickHouse")
		}
	}
	return nil
}

func tableSchema(columns []column.Interface, tag string) (reflect.Type, error) {
	fields := make([]reflect.StructField, len(columns))
	for i, col := range columns {
		name := col.Name()
		fields[i] = reflect.StructField{
			Name: cases.Title(language.Und).String(name),
			Type: col.ScanType(),
			Tag:  reflect.StructTag(fmt.Sprintf("%s:%q ch:%q", tag, name, name)),
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
