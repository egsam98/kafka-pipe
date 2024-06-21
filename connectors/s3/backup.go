package s3

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/dgraph-io/badger/v4"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

const barTmpl = `{{ cycle . "↖" "↗" "↘" "↙" }} {{ string . "title" }} (total: {{ counters . }}, skipped: {{ string . "skipped" }})`

type Backup struct {
	cfg          BackupConfig
	s3           *minio.Client
	producers    map[string]*kgo.Client // Topic is the key
	offsets      map[string]map[int32]int64
	muOffsets    sync.RWMutex
	progress     *pb.Pool
	dbOffsetsKey []byte
}

func NewBackup(cfg BackupConfig) (*Backup, error) {
	return &Backup{
		cfg:          cfg,
		producers:    make(map[string]*kgo.Client),
		offsets:      make(map[string]map[int32]int64),
		dbOffsetsKey: []byte(cfg.Name + "/offsets"),
	}, nil
}

func (b *Backup) Run(ctx context.Context) error {
	if err := validate.Struct(&b.cfg); err != nil {
		return err
	}

	// Init MinIO client - S3 compatible storage
	var err error
	if b.s3, err = minio.New(b.cfg.S3.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(b.cfg.S3.ID, b.cfg.S3.Secret, ""),
		Secure: b.cfg.S3.SSL,
	}); err != nil {
		return errors.Wrap(err, "S3: init client")
	}
	if _, err := b.s3.ListBuckets(ctx); err != nil {
		return errors.Wrap(err, "S3: ping")
	}

	// Init Kafka producers
	for _, topic := range b.cfg.Topics {
		if b.producers[topic], err = kgo.NewClient(
			kgo.SeedBrokers(b.cfg.Kafka.Brokers...),
			kgo.RecordPartitioner(kgo.ManualPartitioner()),
			kgo.ProducerBatchCompression(kgo.Lz4Compression()),
			kgo.MaxBufferedRecords(int(b.cfg.Kafka.Batch.Size)),
			kgo.ProducerLinger(b.cfg.Kafka.Batch.Timeout),
		); err != nil {
			return errors.Wrap(err, "Kafka: init producer")
		}
		if err := b.producers[topic].Ping(ctx); err != nil {
			return errors.Wrap(err, "Kafka: ping")
		}
	}
	defer func() {
		log.Info().Msgf("Kafka: Close producers")
		for _, producer := range b.producers {
			producer.Close()
		}
	}()

	// Restore offsets from Badger
	if err := b.cfg.DB.View(func(tx *badger.Txn) error {
		item, err := tx.Get(b.dbOffsetsKey)
		if err != nil {
			if err == badger.ErrKeyNotFound {
				return nil
			}
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, &b.offsets) })
	}); err != nil {
		return err
	}

	// Start progress bars pool
	b.progress, err = pb.StartPool()
	if err != nil {
		return errors.Wrap(err, "start progress bars")
	}
	defer b.progress.Stop() //nolint:errcheck

	var g errgroup.Group
	for _, topic := range b.cfg.Topics {
		g.Go(func() error {
			return b.backup(ctx, topic)
		})
	}
	err = g.Wait()
	if errors.Is(err, context.Canceled) {
		return nil
	}
	return err
}

func (b *Backup) backup(ctx context.Context, topic string) error {
	ctx, cancel := context.WithCancel(ctx) // To prevent memory leak after breaking channel consumption
	defer cancel()
	objInfos := b.s3.ListObjects(ctx, b.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    topic + "/" + b.cfg.DateSince.Format(time.DateTime),
		Recursive: true,
	})

	bar := pb.ProgressBarTemplate(barTmpl).
		New(-1).
		Set("title", topic).
		Set("skipped", 0)
	b.progress.Add(bar)

	for info := range objInfos {
		if err := b.backupObject(ctx, &info, bar); err != nil {
			return err
		}
	}
	return nil
}

func (b *Backup) backupObject(ctx context.Context, info *minio.ObjectInfo, bar *pb.ProgressBar) error {
	if info.Err != nil {
		return errors.WithStack(info.Err)
	}

	keySeg, err := newKeySegments(info.Key)
	if err != nil {
		return err
	}
	dateTime, err := keySeg.dateTime()
	if err != nil {
		return err
	}
	if dateTime.Before(b.cfg.DateSince) || dateTime.After(b.cfg.DateTo) {
		return nil
	}
	partition, err := keySeg.partition()
	if err != nil {
		return err
	}
	topic := keySeg.topic()

	obj, err := b.s3.GetObject(ctx, b.cfg.S3.Bucket, info.Key, minio.GetObjectOptions{})
	if err != nil {
		return errors.Wrapf(err, "S3: Get %s", info.Key)
	}
	defer obj.Close()

	r, err := gzip.NewReader(obj)
	if err != nil {
		return errors.Wrapf(err, "S3: Init gxip-reader of %s", info.Key)
	}
	defer r.Close()

	dec := json.NewDecoder(r)
	var records []*kgo.Record
	var lastOffset int64 // Last offset in S3 object

	for i := 0; ; i++ {
		var row jsonRow
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				break
			}
			return errors.Wrapf(err, "S3: Decode json row from %s (line: %d)", info.Key, i)
		}
		if row.Offset > lastOffset {
			lastOffset = row.Offset
		}

		b.muOffsets.RLock()
		if partitions, ok := b.offsets[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				if row.Offset <= offset {
					bar.Set("skipped", bar.Get("skipped").(int)+1)
					b.muOffsets.RUnlock()
					continue
				}
			}
		}
		b.muOffsets.RUnlock()

		record := row.kafkaRecord(topic, partition)
		records = append(records, &record)
	}
	if len(records) == 0 {
		return nil
	}

	producer := b.producers[topic]
	promise := kgo.AbortingFirstErrPromise(producer)
	for _, record := range records {
		producer.Produce(ctx, record, promise.Promise())
	}
	if err := promise.Err(); err != nil {
		return errors.Wrapf(err, "Kafka: Produce rows from %s", info.Key)
	}

	// Update offsets and save to Badger
	b.muOffsets.Lock()
	defer b.muOffsets.Unlock()
	partitions, ok := b.offsets[topic]
	if !ok {
		partitions = make(map[int32]int64)
		b.offsets[topic] = partitions
	}
	partitions[partition] = lastOffset
	if err := b.cfg.DB.Update(func(tx *badger.Txn) error {
		bytes, err := json.Marshal(b.offsets)
		if err != nil {
			return err
		}
		return tx.Set(b.dbOffsetsKey, bytes)
	}); err != nil {
		return errors.Wrap(err, "Badger: update offsets")
	}

	bar.Add(len(records))
	return nil
}
