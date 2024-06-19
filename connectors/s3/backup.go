package s3

import (
	"compress/gzip"
	"context"
	"io"
	"sync"
	"time"

	"github.com/egsam98/json-iterator"
	"github.com/minio/minio-go/v7"
	"github.com/minio/minio-go/v7/pkg/credentials"
	"github.com/pkg/errors"
	"github.com/rs/zerolog/log"
	"github.com/twmb/franz-go/pkg/kadm"
	"github.com/twmb/franz-go/pkg/kerr"
	"github.com/twmb/franz-go/pkg/kgo"
	"github.com/vbauerster/mpb/v8"
	"github.com/vbauerster/mpb/v8/decor"
	"golang.org/x/sync/errgroup"

	"github.com/egsam98/kafka-pipe/internal/validate"
)

type Backup struct {
	cfg       BackupConfig
	s3        *minio.Client
	producers map[string]*kgo.Client // Topic is the key
	progress  *mpb.Progress
}

func NewBackup(cfg BackupConfig) (*Backup, error) {
	return &Backup{
		cfg:       cfg,
		producers: make(map[string]*kgo.Client),
		progress:  mpb.New(mpb.WithWidth(3)),
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
			kgo.DefaultProduceTopic(topic),
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

	// Create Kafka topics
	kafkaAdmin := kadm.NewClient(b.producers[b.cfg.Topics[0]])
	for _, topic := range b.cfg.Topics {
		res, err := kafkaAdmin.CreateTopic(ctx, int32(b.cfg.Kafka.Topic.Partitions), int16(b.cfg.Kafka.Topic.ReplicationFactor), map[string]*string{
			"compression.type": &b.cfg.Kafka.Topic.CompressionType,
			"cleanup.policy":   &b.cfg.Kafka.Topic.CleanupPolicy,
		}, topic)
		if err != nil && !errors.Is(res.Err, kerr.TopicAlreadyExists) {
			return errors.Wrapf(err, "create topic %q", topic)
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

	producer := newAbortProducer(b.producers[topic], b.progress)
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
		return info.Err
	}

	parts := regexKeySuffix.FindStringSubmatch(info.Key)
	if len(parts) < 2 {
		return errors.Errorf("S3: Invalid key: %q", info.Key)
	}
	t, err := time.Parse(time.DateTime, parts[1])
	if err != nil {
		return errors.Wrapf(err, "S3: Parse time from %s", info.Key)
	}

	if t.Before(b.cfg.DateSince) || t.After(b.cfg.DateTo) {
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

	it := jsoniter.ConfigDefault.BorrowIterator(make([]byte, 1024))
	defer jsoniter.ConfigDefault.ReturnIterator(it)
	it.Reset(r)

	for i := 0; ; i++ {
		var row jsonRow
		it.ReadVal(&row)
		if it.Error != nil {
			if it.Error == io.EOF {
				return nil
			}
			return errors.Wrapf(it.Error, "S3: Decode json row from %s (line: %d)", info.Key, i)
		}

		record := row.kafkaRecord()
		if err := producer.produce(ctx, &record, info.Key); err != nil {
			return err
		}
	}
}

type abortProducer struct {
	client *kgo.Client
	wg     sync.WaitGroup
	err    error
	muErr  sync.Mutex
	topic  string
	bar    *mpb.Bar
}

func newAbortProducer(client *kgo.Client, progress *mpb.Progress) abortProducer {
	topic := client.OptValue(kgo.DefaultProduceTopic).(string)
	bar := progress.AddSpinner(-1, mpb.AppendDecorators(
		decor.Name(topic, decor.WCSyncSpace),
		decor.CurrentNoUnit("(count: %d,", decor.WCSyncSpace),
		decor.AverageSpeed(0, "speed: %.0f/s)", decor.WCSyncSpace),
	))
	return abortProducer{
		client: client,
		topic:  topic,
		bar:    bar,
	}
}

func (p *abortProducer) produce(ctx context.Context, r *kgo.Record, s3Key string) error {
	p.muErr.Lock()
	if p.err != nil {
		p.muErr.Unlock()
		return p.err
	}
	p.muErr.Unlock()

	r.Topic = p.topic
	r.Context = context.WithValue(ctx, "s3_key", s3Key)
	p.wg.Add(1)

	p.client.Produce(ctx, r, func(rec *kgo.Record, err error) {
		defer p.wg.Done()
		p.bar.Increment()

		//if p.bar.Current() > 100 {
		//	err = errors.New("Something")
		//}

		if err == nil {
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
