package s3

import (
	"github.com/twmb/franz-go/pkg/kgo"
)

const TimeFmt = "2006/01/02/15:04:05"

type Record struct {
	Key     []byte
	Value   []byte
	Headers []kgo.RecordHeader
}

func NewRecord(rec *kgo.Record) Record {
	return Record{
		Key:     rec.Key,
		Value:   rec.Value,
		Headers: rec.Headers,
	}
}
