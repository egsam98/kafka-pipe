package pg

import (
	"context"
	"encoding/json"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/dgraph-io/badger/v4"
	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/jackc/pgx/v5/pgxpool"
	"github.com/pkg/errors"
	"github.com/rs/zerolog"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/twmb/franz-go/plugin/kzerolog"

	kafkapipe "github.com/egsam98/kafka-pipe"
	"github.com/egsam98/kafka-pipe/internal/set"
)

const Plugin = "pgoutput"
const KafkaProduceBatchTimeout = time.Minute

type Source struct {
	cfg           SourceConfig
	pgCfg         pgxpool.Config
	kafka         *kgo.Client
	replConn      *pgconn.PgConn
	db            *pgxpool.Pool
	relations     map[uint32]*pglogrepl.RelationMessage
	typeMap       *pgtype.Map
	lsn           pglogrepl.LSN
	log           zerolog.Logger
	topicResolver *topicResolver
}

type walMessage struct {
	Data       *walData
	Start, End pglogrepl.LSN
}

type walData struct {
	Relation string
	Key      map[string]any
	Value    map[string]any
}

func NewSource(cfg SourceConfig) *Source {
	s := &Source{
		cfg:       cfg,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   pgtype.NewMap(),
	}
	registerTypes(s.typeMap)
	s.log = log.Logger.
		With().
		Logger().
		Hook(s.lsnHook())
	return s
}

func (s *Source) Run(ctx context.Context) error {
	if err := s.cfg.Validate(); err != nil {
		return err
	}
	// Add replication=database query param
	pgUrl, _ := url.Parse(s.cfg.Pg.Url)
	pgQuery := pgUrl.Query()
	if !pgQuery.Has("replication") {
		pgQuery.Set("replication", "database")
		pgUrl.RawQuery = pgQuery.Encode()
		s.cfg.Pg.Url = pgUrl.String()
	}

	var err error
	if s.topicResolver, err = newTopicResolver(&s.cfg.Kafka); err != nil {
		return err
	}

	pgCfg, err := pgxpool.ParseConfig(s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "parse PostgreSQL connection URL")
	}
	s.pgCfg = *pgCfg

	// Init Kafka client
	if s.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
		kgo.WithLogger(kzerolog.New(&log.Logger)),
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
	// Create topics if not exists
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

	if err := s.startReplication(ctx); err != nil {
		return err
	}

	msgs := make(chan walMessage)
	var wg sync.WaitGroup

	wg.Add(2)
	go s.healthPg(ctx, &wg)
	go s.listenPgRepl(ctx, msgs, &wg)
	if err := s.produceMessages(msgs); err != nil {
		return err
	}

	// Shutdown
	wg.Wait()

	s.log.Info().Msg("PostgreSQL: Close")
	s.db.Close()
	s.log.Info().Msg("Kafka: Close producer")
	s.kafka.Close()
	return nil
}

func (s *Source) startReplication(ctx context.Context) error {
	// Connect to PostgreSQL
	s.log.Info().Msg("PostgreSQL: Connect")
	db, err := pgconn.Connect(ctx, s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "connect to PostgreSQL server")
	}
	s.replConn = db
	if err := s.replConn.CheckConn(); err != nil {
		return errors.Wrap(err, "check connection")
	}

	// Create health check table
	sql := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (id int primary key, timestamp timestamp)`, s.cfg.Pg.HealthTable)
	if _, err := s.db.Exec(ctx, sql); err != nil {
		return errors.Wrap(err, sql)
	}
	s.log.Info().Msg("PostgreSQL: " + sql)

	// Create/edit publication
	tables := strings.Join(append(s.cfg.Pg.Tables, s.cfg.Pg.HealthTable), ", ")
	sql = fmt.Sprintf("CREATE PUBLICATION %s FOR TABLE %s", s.cfg.Pg.Publication, tables)
	switch _, err := s.db.Exec(ctx, sql); {
	case err == nil:
		s.log.Info().Msg("PostgreSQL: " + sql)
	case Is(err, DuplicateObject):
		sql := fmt.Sprintf("ALTER PUBLICATION %s SET TABLE %s", s.cfg.Pg.Publication, tables)
		s.log.Info().Msg("PostgreSQL: " + sql)
		if _, err := s.db.Exec(ctx, sql); err != nil {
			return errors.Wrap(err, sql)
		}
	default:
		return errors.Wrap(err, sql)
	}

	// Create replication slot if not exists
	switch _, err := pglogrepl.CreateReplicationSlot(
		ctx,
		s.replConn,
		s.cfg.Pg.Slot,
		Plugin,
		pglogrepl.CreateReplicationSlotOptions{Mode: pglogrepl.LogicalReplication},
	); {
	case err == nil:
		s.log.Info().Msgf("PostgreSQL: Create replication slot %q", s.cfg.Pg.Slot)
	case Is(err, DuplicateObject): // Ignore
	default:
		return errors.Wrapf(err, "create replication slot %q", s.cfg.Pg.Slot)
	}

	// Get last committed LSN
	if err := s.cfg.DB.View(func(tx *badger.Txn) error {
		item, err := tx.Get(s.lsnKey())
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error {
			return s.lsn.Scan(val)
		})
	}); err != nil {
		if !errors.Is(err, badger.ErrKeyNotFound) {
			return err
		}
		s.log.Warn().Msgf("PostgreSQL: LSN position is not found, using first available one from replication slot")
	}

	// Start replication
	s.log.Info().Msgf("PostgreSQL: Start logical replication")
	err = pglogrepl.StartReplication(ctx, s.replConn, s.cfg.Pg.Slot, s.lsn, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{
			"proto_version '1'",
			fmt.Sprintf("publication_names '%s'", s.cfg.Pg.Publication),
			"messages 'true'",
		},
		Mode: pglogrepl.LogicalReplication,
	})
	return errors.Wrap(err, "start logical replication")
}

// healthPg sends health check status to postgres table and replication slot with the latest WAL position
func (s *Source) healthPg(ctx context.Context, wg *sync.WaitGroup) {
	defer wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		case <-time.After(10 * time.Second):
			if s.replConn.IsClosed() {
				continue
			}

			err := pglogrepl.SendStandbyStatusUpdate(ctx, s.replConn, pglogrepl.StandbyStatusUpdate{WALWritePosition: s.lsn})
			if err != nil {
				s.log.Err(err).Msg("PostgreSQL: SendStandbyStatusUpdate")
				continue
			}

			sql := fmt.Sprintf("INSERT INTO %s (id, timestamp) VALUES (0, now()) ON CONFLICT (id) DO UPDATE SET "+
				"timestamp = now()", s.cfg.Pg.HealthTable)
			if _, err := s.db.Exec(ctx, sql); err != nil {
				s.log.Err(err).Msgf("PostgreSQL: Health check to %q table", s.cfg.Pg.HealthTable)
			}
		}
	}
}

// listenPgRepl listens publication messages from pg replication slot and writes to channel
func (s *Source) listenPgRepl(ctx context.Context, msgs chan<- walMessage, wg *sync.WaitGroup) {
	defer wg.Done()
	defer close(msgs)

	for {
		err := s.recvMessage(ctx, msgs)
		if err == nil {
			continue
		}

		_ = s.replConn.Close(ctx)
		if errors.Is(err, context.Canceled) {
			return
		}
		s.log.Error().Stack().Err(err).Msgf("PostgreSQL: Handle message")

		for {
			err := s.startReplication(ctx)
			if err == nil {
				break
			}
			s.log.Error().Stack().Err(err).Msg("PostgreSQL: Start replication")
			select {
			case <-time.After(5 * time.Second):
			case <-ctx.Done():
				return
			}
		}
	}
}

// recvMessage receives message from pg slot and writes to channel
func (s *Source) recvMessage(ctx context.Context, msgs chan<- walMessage) error {
	msg, err := s.replConn.ReceiveMessage(ctx)
	if err != nil {
		if pgconn.Timeout(err) {
			return nil
		}
		return errors.Wrap(err, "receive message")
	}

	if errMsg, ok := msg.(*pgproto3.ErrorResponse); ok {
		return errors.Errorf("Postgres WAL error: %+v", errMsg)
	}

	cd, ok := msg.(*pgproto3.CopyData)
	if !ok || cd.Data[0] != pglogrepl.XLogDataByteID {
		return nil
	}

	xld, err := pglogrepl.ParseXLogData(cd.Data[1:])
	if err != nil {
		return errors.Wrap(err, "parse XLogData")
	}

	xldData, err := pglogrepl.Parse(xld.WALData)
	if err != nil {
		return errors.Wrap(err, "parse WAL data")
	}

	s.log.Debug().
		Time("server_time", xld.ServerTime).
		Stringer("wal_start", xld.WALStart).
		Stringer("wal_end", xld.ServerWALEnd).
		Msgf("PostgreSQL: Received message %T", xldData)

	var data *walData
	switch xldData := xldData.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[xldData.RelationID] = xldData
	case *pglogrepl.InsertMessage:
		data, err = s.messageData(xldData.RelationID, xldData.Tuple.Columns)
	case *pglogrepl.UpdateMessage:
		data, err = s.messageData(xldData.RelationID, xldData.NewTuple.Columns)
	case *pglogrepl.DeleteMessage:
		if !s.cfg.Pg.SkipDelete {
			data, err = s.messageData(xldData.RelationID, xldData.OldTuple.Columns)
		}
	}
	if err != nil {
		return err
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case msgs <- walMessage{
		Start: xld.WALStart,
		End:   xld.WALStart + pglogrepl.LSN(len(xld.WALData)),
		Data:  data,
	}:
		return nil
	}
}

// messageData returns relation name (schema/namespace is included) and row data
func (s *Source) messageData(relationID uint32, cols []*pglogrepl.TupleDataColumn) (*walData, error) {
	rel, ok := s.relations[relationID]
	if !ok {
		return nil, errors.Errorf("unknown relation ID=%d", relationID)
	}

	table := rel.Namespace + "." + rel.RelationName
	// Skip health check table
	if strings.HasSuffix(table, s.cfg.Pg.HealthTable) {
		return nil, nil
	}

	msg := &walData{
		Relation: table,
		Key:      make(map[string]any),
		Value:    make(map[string]any),
	}

	// Decode columns tuple into Go map
	for i, col := range cols {
		colMeta := rel.Columns[i]

		var value any
		switch col.DataType {
		case 'n': // null
		case 't', 'b':
			var format int16
			switch col.DataType {
			case 't': // text
				format = pgtype.TextFormatCode
			case 'b': // binary
				format = pgtype.BinaryFormatCode
			}

			oid := colMeta.DataType
			typ, ok := s.typeMap.TypeForOID(oid)
			if !ok {
				value = string(col.Data)
				break
			}
			var err error
			if value, err = typ.Codec.DecodeValue(s.typeMap, oid, format, col.Data); err != nil {
				return nil, errors.Wrapf(err, "decode %q (OID=%d, format=%d)", string(col.Data), oid, format)
			}
		}

		if colMeta.Flags == 1 {
			msg.Key[colMeta.Name] = value
		}
		msg.Value[colMeta.Name] = value
	}
	return msg, nil
}

// produceMessages collects WAL messages into batch and produces to Kafka
func (s *Source) produceMessages(msgs <-chan walMessage) error {
	batch := make([]*kgo.Record, 0, s.cfg.Kafka.Batch.Size)
	var latestLSN pglogrepl.LSN
	ticker := time.NewTicker(s.cfg.Kafka.Batch.Timeout)
	defer ticker.Stop()

	for {
		// Collect messages into batch
		select {
		case <-ticker.C:
		case msg, ok := <-msgs:
			if !ok {
				return nil
			}
			latestLSN = msg.End
			if msg.Data == nil {
				continue
			}

			key, err := json.Marshal(msg.Data.Key)
			if err != nil {
				return errors.Wrapf(err, "marshal WAL message's key: %+v", msg.Data.Key)
			}
			value, err := json.Marshal(msg.Data.Value)
			if err != nil {
				return errors.Wrapf(err, "marshal WAL message's value: %+v", msg.Data.Value)
			}

			batch = append(batch, &kgo.Record{
				Topic: s.topicResolver.resolve(msg.Data.Relation),
				Key:   key,
				Value: value,
				Headers: []kgo.RecordHeader{
					{
						Key:   "version",
						Value: []byte(kafkapipe.Version),
					},
					{
						Key:   "lsn",
						Value: []byte(msg.Start.String()),
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
						Key:   "relation",
						Value: []byte(msg.Data.Relation),
					},
				},
			})

			if uint(len(batch)) < s.cfg.Kafka.Batch.Size {
				continue
			}
		}

		// Produce batch to Kafka
		if len(batch) > 0 {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), KafkaProduceBatchTimeout)
				err := s.kafka.ProduceSync(ctx, batch...).FirstErr()
				cancel()

				if err == nil {
					break
				}
				if errors.Is(err, context.DeadlineExceeded) {
					s.log.Err(err).
						Int("batch_size", len(batch)).
						Dur("batch_timeout", KafkaProduceBatchTimeout).
						Msgf("Kafka: Failed to produce a batch")
					continue
				}
				if errors.Is(err, kerr.UnknownTopicOrPartition) {
					topics := set.NewSet[string]()
					for _, rec := range batch {
						topics.Add(rec.Topic)
					}
					return errors.Wrapf(err, "topics=%v", topics.Slice())
				}
				return errors.Wrap(err, "produce to Kafka")
			}

			s.log.Info().Int("count", len(batch)).Msg("Kafka: Messages have been published")
			clear(batch)
			batch = batch[:0]
		}

		// Update LSN to Badger storage
		if latestLSN != 0 {
			if err := s.cfg.DB.Update(func(tx *badger.Txn) error {
				return tx.Set(s.lsnKey(), []byte(latestLSN.String()))
			}); err != nil {
				return err
			}
			s.lsn = latestLSN
			latestLSN = 0
			s.log.Info().Msg("Commit LSN")
		}
	}
}

// lsnHook appends current WAL LSN position to log messages
func (s *Source) lsnHook() zerolog.HookFunc {
	return func(e *zerolog.Event, _ zerolog.Level, _ string) {
		if s.lsn == 0 {
			return
		}
		e.Stringer("lsn", s.lsn)
	}
}

// lsnKey for badger storage
func (s *Source) lsnKey() []byte {
	return []byte(s.cfg.Name + "/lsn")
}
