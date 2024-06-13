package s3

import (
	"unsafe"

	"github.com/twmb/franz-go/pkg/kgo"
)

// jsonRow represents a line of encoded data in S3
type jsonRow struct {
	Offset  int64    `json:"offset"`
	Key     string   `json:"key"`
	Value   string   `json:"value"`
	Headers []header `json:"headers"`
}

type header struct {
	Key   string `json:"key"`
	Value string `json:"value"`
}

func newJsonRow(rec *kgo.Record) jsonRow {
	r := jsonRow{
		Offset:  rec.Offset,
		Headers: make([]header, len(rec.Headers)),
	}
	if n := len(rec.Key); n > 0 {
		r.Key = unsafe.String(&rec.Key[0], n)
	}
	if n := len(rec.Value); n > 0 {
		r.Value = unsafe.String(&rec.Value[0], n)
	}
	for i, h := range rec.Headers {
		hr := header{Key: h.Key}
		if n := len(h.Value); n > 0 {
			hr.Value = unsafe.String(&h.Value[0], n)
		}
		r.Headers[i] = hr
	}
	return r
}

func (j *jsonRow) kafkaRecord(topic string) kgo.Record {
	rec := kgo.Record{
		Key:     unsafe.Slice(unsafe.StringData(j.Key), len(j.Key)),
		Value:   unsafe.Slice(unsafe.StringData(j.Value), len(j.Value)),
		Headers: make([]kgo.RecordHeader, len(j.Headers)),
		Topic:   topic,
	}
	for i, h := range j.Headers {
		rec.Headers[i] = kgo.RecordHeader{
			Key:   h.Key,
			Value: unsafe.Slice(unsafe.StringData(h.Value), len(h.Value)),
		}
	}
	return rec
}
