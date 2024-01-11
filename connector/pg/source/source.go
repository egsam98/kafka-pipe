package source

import (
	"context"
	"encoding/json"
	"fmt"
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

	"kafka-pipe/connector/pg"
)

const Plugin = "pgoutput"
const KafkaProduceBatchTimeout = time.Minute

type Source struct {
	cfg       Config
	pgCfg     pgxpool.Config
	wg        sync.WaitGroup
	kafka     *kgo.Client
	replConn  *pgconn.PgConn
	db        *pgxpool.Pool
	relations map[uint32]*pglogrepl.RelationMessage
	typeMap   *pgtype.Map
	lsn       pglogrepl.LSN
	events    chan Event
	log       zerolog.Logger
}

type Event struct {
	Start, End pglogrepl.LSN
	Table      string
	Data       map[string]any
}

func NewSource(cfg Config) *Source {
	s := &Source{
		cfg:       cfg,
		relations: make(map[uint32]*pglogrepl.RelationMessage),
		typeMap:   pgtype.NewMap(),
		events:    make(chan Event),
	}
	pg.RegisterTypes(s.typeMap)
	s.log = log.Logger.
		With().
		Logger().
		Hook(s.lsnHook())
	return s
}

func (s *Source) Run(ctx context.Context) error {
	pgCfg, err := pgxpool.ParseConfig(s.cfg.Pg.Url)
	if err != nil {
		return errors.Wrap(err, "parse PostgreSQL connection URL")
	}
	s.pgCfg = *pgCfg

	// Init Kafka client
	if s.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(s.cfg.Kafka.Brokers...),
	); err != nil {
		return errors.Wrap(err, "init Kafka client")
	}
	if err := s.kafka.Ping(ctx); err != nil {
		return errors.Wrap(err, "ping Kafka client")
	}

	kafkaAdmin := kadm.NewClient(s.kafka)
	// Create topics if not exists
	for _, table := range s.cfg.Pg.Tables {
		topic := s.cfg.Kafka.Topic.Prefix + "." + table
		res, err := kafkaAdmin.CreateTopic(ctx, s.cfg.Kafka.Topic.Partitions, s.cfg.Kafka.Topic.ReplicationFactor, map[string]*string{
			"compression.type": &s.cfg.Kafka.Topic.CompressionType,
			"cleanup.policy":   &s.cfg.Kafka.Topic.CleanupPolicy,
		}, topic)
		if err != nil && !errors.Is(res.Err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(err, "create topic %q", topic)
		}
	}

	if s.db, err = pgxpool.NewWithConfig(ctx, pgCfg); err != nil {
		return errors.Wrap(err, "connect to PostgreSQL")
	}

	if err := s.startReplication(ctx); err != nil {
		return err
	}

	s.wg.Add(3)
	go s.healthPg(ctx)
	go s.listenPgRepl(ctx)

	if err := s.produceEvents(); err != nil {
		return err
	}

	// Shutdown
	s.wg.Wait()

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
	case pg.Is(err, pg.DuplicateObject):
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
	case pg.Is(err, pg.DuplicateObject): // Ignore
	default:
		return errors.Wrapf(err, "create replication slot %q", s.cfg.Pg.Slot)
	}

	// Get last committed LSN
	if err := s.cfg.Storage.View(func(tx *badger.Txn) error {
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

func (s *Source) healthPg(ctx context.Context) {
	defer s.wg.Done()

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

func (s *Source) listenPgRepl(ctx context.Context) {
	defer s.wg.Done()
	defer close(s.events)

	for {
		err := s.recvEvent(ctx)
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

func (s *Source) recvEvent(ctx context.Context) error {
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

	var (
		data  map[string]any
		table string
	)
	switch xldData := xldData.(type) {
	case *pglogrepl.RelationMessage:
		s.relations[xldData.RelationID] = xldData
	case *pglogrepl.InsertMessage:
		table, data, err = s.newEventData(xldData.RelationID, xldData.Tuple.Columns)
	case *pglogrepl.UpdateMessage:
		table, data, err = s.newEventData(xldData.RelationID, xldData.NewTuple.Columns)
	case *pglogrepl.DeleteMessage:
		if !s.cfg.Pg.SkipDelete {
			table, data, err = s.newEventData(xldData.RelationID, xldData.OldTuple.Columns)
		}
	}
	if err != nil {
		return err
	}

	event := Event{
		Start: xld.WALStart,
		End:   xld.WALStart + pglogrepl.LSN(len(xld.WALData)),
		Table: table,
		Data:  data,
	}

	select {
	case <-ctx.Done():
		return ctx.Err()
	case s.events <- event:
		return nil
	}
}

func (s *Source) newEventData(relationID uint32, cols []*pglogrepl.TupleDataColumn) (string, map[string]any, error) {
	rel, ok := s.relations[relationID]
	if !ok {
		return "", nil, errors.Errorf("unknown relation ID=%d", relationID)
	}

	table := rel.Namespace + "." + rel.RelationName
	// Skip health check table
	if table == s.cfg.Pg.HealthTable {
		return "", nil, nil
	}

	// Decode columns tuple into Go map
	data := make(map[string]any)
	for i, col := range cols {
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

			oid := rel.Columns[i].DataType
			typ, ok := s.typeMap.TypeForOID(oid)
			if !ok {
				value = string(col.Data)
				break
			}
			var err error
			if value, err = typ.Codec.DecodeValue(s.typeMap, oid, format, col.Data); err != nil {
				return "", nil, errors.Wrapf(err, "decode %q (OID=%d, format=%d)", string(col.Data), oid, format)
			}
		}

		data[rel.Columns[i].Name] = value
	}
	return table, data, nil
}

func (s *Source) produceEvents() error {
	batch := make([]*kgo.Record, 0, s.cfg.Kafka.Batch.Size)
	var latestLSN pglogrepl.LSN
	ticker := time.NewTicker(s.cfg.Kafka.Batch.Timeout)
	defer ticker.Stop()

	for {
		select {
		case <-ticker.C:
		case event, ok := <-s.events:
			if !ok {
				return nil
			}
			latestLSN = event.End
			if event.Data == nil {
				continue
			}

			value, err := json.Marshal(event.Data)
			if err != nil {
				return errors.Wrapf(err, "marshal event data: %+v", event.Data)
			}

			key, err := pg.KafkaKey(event.Data)
			if err != nil {
				return err
			}

			batch = append(batch, &kgo.Record{
				Topic: s.cfg.Kafka.Topic.Prefix + "." + event.Table,
				Key:   key,
				Value: value,
				Headers: []kgo.RecordHeader{
					{
						Key:   "lsn",
						Value: []byte(event.Start.String()),
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
						Value: []byte(event.Table),
					},
				},
			})

			if len(batch) < s.cfg.Kafka.Batch.Size {
				continue
			}
		}

		if len(batch) > 0 {
			for {
				ctx, cancel := context.WithTimeout(context.Background(), KafkaProduceBatchTimeout)
				err := s.kafka.ProduceSync(ctx, batch...).FirstErr()
				cancel()

				if err == nil {
					break
				}
				if !errors.Is(err, context.DeadlineExceeded) {
					return errors.Wrap(err, "produce to Kafka")
				}
				s.log.Err(err).
					Int("batch_size", len(batch)).
					Dur("batch_timeout", KafkaProduceBatchTimeout).
					Msgf("Kafka: Failed to produce a batch")
			}

			s.log.Info().Int("count", len(batch)).Msg("Kafka: Events have been published")
			clear(batch)
			batch = batch[:0]
		}

		if latestLSN != 0 {
			if err := s.cfg.Storage.Update(func(tx *badger.Txn) error {
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

func (s *Source) lsnHook() zerolog.HookFunc {
	return func(e *zerolog.Event, _ zerolog.Level, _ string) {
		if s.lsn == 0 {
			return
		}
		e.Stringer("lsn", s.lsn)
	}
}

func (s *Source) lsnKey() []byte {
	return []byte(s.cfg.Name + "/lsn")
}
