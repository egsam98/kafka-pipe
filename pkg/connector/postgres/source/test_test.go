package source

import (
	"context"
	"testing"
	"time"

	"github.com/Shopify/sarama"
	"github.com/segmentio/kafka-go"
	"github.com/stretchr/testify/assert"
)

const n = 10

func BenchmarkTestName(b *testing.B) {
	return
	//uri, _ := url.Parse("postgres://postgres:postgres@localhost:5436/postgres")
	//lis := NewSource(Config{
	//	ConnectionURL:  *uri,
	//	Slot:           "test",
	//	Tables:         []string{"vendor"},
	//	StandbyTimeout: 10 * time.Second,
	//})
	//log.SetDefault(log.NewStd(log.WithPretty(true)))
	//
	//out, err := lis.Listen(context.Background())
	//assert.NoError(t, err)
	//
	//for b := range out {
	//	t.Log(string(b))
	//}

	ctx := context.Background()

	w := kafka.Writer{
		Addr:                   kafka.TCP("localhost:29092"),
		MaxAttempts:            5,
		RequiredAcks:           kafka.RequireAll,
		Compression:            kafka.Lz4,
		AllowAutoTopicCreation: true,
		BatchSize:              1,
	}
	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			err := w.WriteMessages(ctx, kafka.Message{
				Topic: "test",
				Key:   []byte("test"),
				Value: []byte("test"),
			})
			assert.NoError(b, err)
		}
	}
}

func BenchmarkSarama(b *testing.B) {
	//ctx := context.Background()

	cfg := sarama.NewConfig()
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	p, err := sarama.NewSyncProducer([]string{"localhost:29092"}, cfg)
	assert.NoError(b, err)

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			_, _, err := p.SendMessage(&sarama.ProducerMessage{
				Topic: "test",
				Key:   sarama.StringEncoder("test"),
				Value: sarama.StringEncoder("test"),
			})
			assert.NoError(b, err)
		}
	}
}

func BenchmarkSaramaAsync(b *testing.B) {
	//ctx := context.Background()

	cfg := sarama.NewConfig()
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	p, err := sarama.NewAsyncProducer([]string{"localhost:29092"}, cfg)
	assert.NoError(b, err)

	go func() {
		for err := range p.Errors() {
			b.Log(err)
		}
	}()
	//go func() {
	//	for range p.Successes() {
	//		//b.Log(err)
	//	}
	//}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			p.Input() <- &sarama.ProducerMessage{
				Topic: "test",
				Key:   sarama.StringEncoder("test"),
				Value: sarama.StringEncoder("test"),
			}
			_ = <-p.Successes()
		}
	}
}

func BenchmarkSaramaAsyncBatch(b *testing.B) {
	//ctx := context.Background()

	cfg := sarama.NewConfig()
	cfg.Net.MaxOpenRequests = 1
	cfg.Producer.Idempotent = true
	cfg.Producer.RequiredAcks = sarama.WaitForAll
	cfg.Producer.Compression = sarama.CompressionLZ4
	cfg.Producer.Return.Successes = true
	cfg.Producer.Retry.Max = 5
	cfg.Producer.Flush.Frequency = 1 * time.Second
	p, err := sarama.NewAsyncProducer([]string{"localhost:29092"}, cfg)
	assert.NoError(b, err)

	go func() {
		for err := range p.Errors() {
			b.Log(err)
		}
	}()
	go func() {
		for range p.Successes() {
			//b.Log(err)
		}
	}()

	b.ResetTimer()

	for i := 0; i < b.N; i++ {
		for j := 0; j < n; j++ {
			p.Input() <- &sarama.ProducerMessage{
				Topic: "test",
				Key:   sarama.StringEncoder("test"),
				Value: sarama.StringEncoder("test"),
			}
			//_ = <-p.Successes()
		}
	}

	time.Sleep(time.Hour)
}
