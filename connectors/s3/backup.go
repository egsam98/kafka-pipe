package s3

import (
	"compress/gzip"
	"context"
	"fmt"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Backup struct {
	cfg       BackupConfig
	s3        *minio.Client
	producers map[string]*kgo.Client // Topic is the key
}

func NewBackup(cfg BackupConfig) (*Backup, error) {
	return &Backup{cfg: cfg, producers: make(map[string]*kgo.Client)}, nil
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

	var g errgroup.Group
	for _, topic := range b.cfg.Topics {
		g.Go(func() error {
			return b.backup(ctx, topic)
		})
	}
	if err := g.Wait(); err != nil {
		return err
	}

	log.Info().Msgf("Kafka: Close producers")
	for _, producer := range b.producers {
		producer.Close()
	}
	return nil
}

func (b *Backup) backup(ctx context.Context, topic string) error {
	objInfos := b.s3.ListObjects(ctx, b.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    topic + "/" + b.cfg.DateSince.Format(time.DateTime),
		Recursive: true,
	})

	producer := b.producers[topic]
	promise := kgo.AbortingFirstErrPromise(producer)
	for info := range objInfos {
		if info.Err != nil {
			return info.Err
		}

		t, err := time.Parse(fmt.Sprintf("%s/%s", topic, time.DateTime), info.Key)
		if err != nil {
			return err
		}

		if t.Before(b.cfg.DateSince) || t.After(b.cfg.DateTo) {
			continue
		}

		if err := func() error {
			obj, err := b.s3.GetObject(ctx, b.cfg.S3.Bucket, info.Key, minio.GetObjectOptions{})
			if err != nil {
				return errors.Wrapf(err, "S3: get %s", info.Key)
			}
			defer obj.Close()

			r, err := gzip.NewReader(obj)
			if err != nil {
				return errors.Wrap(err, "gzip reader")
			}
			defer r.Close()

			it := jsoniter.ConfigDefault.BorrowIterator(nil)
			defer jsoniter.ConfigDefault.ReturnIterator(it)
			it.Reset(r)

			var row jsonRow
			if it.ReadVal(&row); it.Error != nil {
				return errors.Wrap(it.Error, "decode json row")
			}

			record := row.kafkaRecord(topic)
			producer.Produce(ctx, &record, promise.Promise())
			return nil
		}(); err != nil {
			return err
		}
	}

	return promise.Err()
}
