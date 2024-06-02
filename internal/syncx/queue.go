package syncx

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/rs/zerolog/log"
)

type Queue[T any] struct {
	db      *badger.DB
	seq     *badger.Sequence
	cond    *sync.Cond
	size    int
	dataKey []byte
}

func NewQueue[T any](id string, db *badger.DB) (*Queue[T], error) {
	seq, err := db.GetSequence(fmt.Appendf(nil, "queue/%s/seq", id), 1)
	if err != nil {
		return nil, err
	}
	return &Queue[T]{
		db:      db,
		seq:     seq,
		cond:    sync.NewCond(new(sync.Mutex)),
		dataKey: fmt.Appendf(nil, "queue/%s/data", id),
	}, nil
}

func (q *Queue[T]) Push(value T) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return err
	}

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	i64, err := q.seq.Next()
	if err != nil {
		return err
	}
	if err := q.db.Update(func(tx *badger.Txn) error {
		return tx.Set(fmt.Appendf(q.dataKey, "/%d", i64), buf.Bytes())
	}); err != nil {
		return err
	}

	q.size++
	q.cond.Signal()
	return nil
}

func (q *Queue[T]) Listen(ctx context.Context, f func(value T) error) {
	for {
		select {
		case <-ctx.Done():
			return
		default:
			if err := q.handle(f); err != nil {
				log.Error().Stack().Err(err).Msg("Queue")
			}
		}
	}
}

func (q *Queue[T]) Close() error {
	q.cond.L.Lock() // Make sure `handle` has finished
	defer q.cond.L.Unlock()
	return q.seq.Release()
}

func (q *Queue[T]) handle(f func(value T) error) error {
	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	for q.size == 0 {
		q.cond.Wait()
	}

	if err := q.db.Update(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   1,
			Prefix:         q.dataKey,
		})
		defer it.Close()
		if it.Rewind(); !it.Valid() {
			return nil
		}
		item := it.Item()

		var value T
		if err := item.Value(func(b []byte) error {
			reader := bytes.NewReader(b)
			return gob.NewDecoder(reader).Decode(&value)
		}); err != nil {
			return err
		}
		if err := f(value); err != nil {
			return err
		}
		return tx.Delete(item.Key())
	}); err != nil {
		return err
	}
	q.size--
	return nil
}
