package s3

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"io"
	"sync"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/syncx"
	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Backup struct {
	cfg       BackupConfig
	s3        *minio.Client
	producers map[string]*kgo.Client // Topic is the key
	progress  *mpb.Progress
	offsets   syncx.Map[string, map[int32]int64]
	muOffsets sync.RWMutex
}

func NewBackup(cfg BackupConfig) (*Backup, error) {
	return &Backup{
		cfg:       cfg,
		producers: make(map[string]*kgo.Client),
		progress:  mpb.New(mpb.WithWidth(3)),
		offsets:   syncx.NewMap[string, map[int32]int64](),
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
	defer b.progress.Wait()

	var g errgroup.Group
	for _, topic := range b.cfg.Topics {
		g.Go(func() error {
			return b.backup(ctx, topic)
		})
	}
	return g.Wait()
}

func (b *Backup) backup(ctx context.Context, topic string) error {
	ctx, cancel := context.WithCancel(ctx) // To prevent memory leak after breaking channel consumption
	defer cancel()
	objInfos := b.s3.ListObjects(ctx, b.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    topic + "/" + b.cfg.DateSince.Format(time.DateTime),
		Recursive: true,
	})

	producer := newAbortProducer(b.producers[topic], topic, b.progress, &b.offsets)
	for info := range objInfos {
		if err := b.backupObject(ctx, &info, &producer); err != nil {
			_ = producer.wait()
			return err
		}
	}
	return producer.wait()
}

func (b *Backup) backupObject(ctx context.Context, info *minio.ObjectInfo, producer *abortProducer) error {
	if info.Err != nil {
		return errors.WithStack(info.Err)
	}

	segments, err := newKeySegments(info.Key)
	if err != nil {
		return err
	}
	dateTime, err := time.Parse(time.DateTime, segments.dateTime())
	if err != nil {
		return errors.Wrapf(err, "S3: Parse time from %s", info.Key)
	}

	if dateTime.Before(b.cfg.DateSince) || dateTime.After(b.cfg.DateTo) {
		return nil
	}

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
	for i := 0; ; i++ {
		var row jsonRow
		if err := dec.Decode(&row); err != nil {
			if err == io.EOF {
				return nil
			}
			return errors.Wrapf(err, "S3: Decode json row from %s (line: %d)", info.Key, i)
		}

		topic := segments.topic()
		partition, err := segments.partition()
		if err != nil {
			return err
		}

		b.offsets.RLock()
		if partitions, ok := b.offsets.Raw[topic]; ok {
			if offset, ok := partitions[partition]; ok {
				if offset >= row.Offset {
					b.offsets.RUnlock()
					continue
				}
			}
		}
		b.offsets.RUnlock()

		record := row.kafkaRecord(topic, partition)
		if err := producer.produce(ctx, &record, info.Key, row.Offset); err != nil {
			return err
		}
	}
}

type abortProducer struct {
	client  *kgo.Client
	wg      sync.WaitGroup
	err     error
	muErr   sync.Mutex
	offsets *syncx.Map[string, map[int32]int64]
	bar     *mpb.Bar
}

func newAbortProducer(client *kgo.Client, topic string, progress *mpb.Progress, offsets *syncx.Map[string, map[int32]int64]) abortProducer {
	bar := progress.AddSpinner(-1, mpb.AppendDecorators(
		decor.Name(topic, decor.WCSyncSpace),
		decor.CurrentNoUnit("(count: %d,", decor.WCSyncSpace),
		decor.AverageSpeed(0, "speed: %.0f/s)", decor.WCSyncSpace),
	))
	return abortProducer{
		client:  client,
		offsets: offsets,
		bar:     bar,
	}
}

func (p *abortProducer) produce(ctx context.Context, r *kgo.Record, s3Key string, s3Offset int64) error {
	p.muErr.Lock()
	if p.err != nil {
		p.muErr.Unlock()
		return p.err
	}
	p.muErr.Unlock()

	ctx = context.WithValue(ctx, "s3_key", s3Key)
	ctx = context.WithValue(ctx, "s3_offset", s3Offset)
	r.Context = ctx
	p.wg.Add(1)

	p.client.Produce(ctx, r, func(rec *kgo.Record, err error) {
		defer p.wg.Done()
		p.bar.Increment()

		if err == nil {
			p.offsets.Lock()
			defer p.offsets.Unlock()
			partitions, ok := p.offsets.Raw[r.Topic]
			if !ok {
				partitions = make(map[int32]int64)
				p.offsets.Raw[r.Topic] = partitions
			}
			offset, ok := partitions[r.Partition]
			if !ok {
				offset = -1
				partitions[r.Partition] = offset
			}
			s3Offset := r.Context.Value("s3_offset").(int64)
			if s3Offset > offset {
				partitions[r.Partition] = s3Offset
			}
			return
		}

		p.muErr.Lock()
		defer p.muErr.Unlock()
		if p.err != nil {
			return
		}
		p.err = errors.Wrapf(err, "Kafka: Produce from S3 key %s", rec.Context.Value("s3_key"))
		p.wg.Add(1)
		go func() {
			defer p.wg.Done()
			_ = p.client.AbortBufferedRecords(context.Background())
		}()
	})
	return nil
}

func (p *abortProducer) wait() error {
	p.wg.Wait()
	p.bar.SetTotal(-1, true)
	return p.err
}
