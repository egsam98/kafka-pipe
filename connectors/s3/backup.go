package s3

import (
	"compress/gzip"
	"context"
	"encoding/json"
	"sync"
	"sync/atomic"
	"time"

	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kgo"

	"github.com/egsam98/kafka-pipe/internal/registry"
)

type Backup struct {
	cfg   BackupConfig
	s3    *minio.Client
	kafka *kgo.Client
}

func NewBackup(config registry.Config) (*Backup, error) {
	var cfg BackupConfig
	if err := cfg.Parse(config.Raw); err != nil {
		return nil, err
	}
	return &Backup{cfg: cfg}, nil
}

func (b *Backup) Run(ctx context.Context) error {
	// Init MinIO client - S3 compatible storage
	var err error
	if b.s3, err = minio.New(b.cfg.S3.URL, &minio.Options{
		Creds:  credentials.NewStaticV4(b.cfg.S3.AccessKeyID, b.cfg.S3.SecretKeyAccess, ""),
		Secure: b.cfg.S3.SSL,
	}); err != nil {
		return errors.Wrap(err, "init MinIO client")
	}
	if _, err := b.s3.ListBuckets(ctx); err != nil {
		return errors.Wrap(err, "ping S3")
	}

	// Init Kafka client
	if b.kafka, err = kgo.NewClient(
		kgo.SeedBrokers(b.cfg.Kafka.Brokers...),
		kgo.ProducerBatchCompression(kgo.Lz4Compression()),
		kgo.MaxBufferedRecords(b.cfg.Kafka.Flush.Size),
		kgo.ProducerLinger(b.cfg.Kafka.Flush.Timeout),
	); err != nil {
		return errors.Wrap(err, "init Kafka client")
	}
	if err := b.kafka.Ping(ctx); err != nil {
		return errors.Wrap(err, "ping Kafka client")
	}

	for _, topic := range b.cfg.S3.Topics {
		if err := b.handleObjects(ctx, topic); err != nil {
			if errors.Is(err, context.Canceled) {
				break
			}
			return err
		}
	}

	log.Info().Msgf("Kafka: Close producer")
	b.kafka.Close()
	return nil
}

func (b *Backup) handleObjects(ctx context.Context, topic string) error {
	objects := b.s3.ListObjects(ctx, b.cfg.S3.Bucket, minio.ListObjectsOptions{
		Prefix:    topic,
		Recursive: true,
	})

	var (
		produceErr error
		wg         sync.WaitGroup
		once       atomic.Bool
	)

	for objInfo := range objects {
		if objInfo.Err != nil {
			produceErr = errors.Wrapf(objInfo.Err, "object %s", objInfo.Key)
			break
		}
		t, err := time.Parse(topic+"/"+TimeFmt+".json.gz", objInfo.Key)
		if err != nil {
			produceErr = errors.Wrapf(err, "object %s", objInfo.Key)
			break
		}

		if t.Before(b.cfg.S3.Since.Time) || (b.cfg.S3.To != nil && t.After(b.cfg.S3.To.Time)) {
			continue
		}

		obj, err := b.s3.GetObject(ctx, b.cfg.S3.Bucket, objInfo.Key, minio.GetObjectOptions{})
		if err != nil {
			produceErr = errors.Wrap(err, "get object")
			break
		}
		r, err := gzip.NewReader(obj)
		if err != nil {
			produceErr = errors.Wrap(err, "gzip reader")
			break
		}
		var records []record
		if err := json.NewDecoder(r).Decode(&records); err != nil {
			produceErr = errors.Wrap(err, "decode record")
			break
		}

		r.Close()
		obj.Close()
		if produceErr != nil {
			break
		}

		wg.Add(len(records))
		for _, rec := range records {
			b.kafka.Produce(ctx, &kgo.Record{
				Topic:   topic,
				Key:     rec.Key,
				Value:   rec.Value,
				Headers: rec.Headers,
			}, func(record *kgo.Record, err error) {
				defer wg.Done()
				if err == nil {
					return
				}
				if !once.Swap(true) {
					produceErr = err
					wg.Add(1)
					go func() {
						defer wg.Done()
						_ = b.kafka.AbortBufferedRecords(context.Background())
					}()
				}
			})
		}
	}

	wg.Wait()
	return produceErr
}
