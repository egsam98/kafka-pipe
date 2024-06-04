package badgerx

import (
	"bytes"
	"context"
	"encoding/gob"
	"fmt"
	"sync"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"
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
	q := &Queue[T]{
		db:      db,
		cond:    sync.NewCond(new(sync.Mutex)),
		dataKey: fmt.Appendf(nil, "queue/%s/data", id),
	}

	var err error
	if q.seq, err = db.GetSequence(fmt.Appendf(nil, "queue/%s/seq", id), 1); err != nil {
		return nil, errors.Wrap(err, "badgerx.Queue")
	}

	// Init size
	err = db.View(func(tx *badger.Txn) error {
		it := tx.NewIterator(badger.IteratorOptions{
			PrefetchValues: true,
			PrefetchSize:   100,
			Prefix:         q.dataKey,
		})
		defer it.Close()
		for it.Rewind(); it.Valid(); it.Next() {
			q.size++
		}
		return nil
	})
	return q, errors.Wrap(err, "badgerx.Queue: init size")
}

func (q *Queue[T]) Push(value T) error {
	var buf bytes.Buffer
	if err := gob.NewEncoder(&buf).Encode(value); err != nil {
		return errors.Wrapf(err, "badgerx.Queue: encode %+v (%T)", value, value)
	}

	q.cond.L.Lock()
	defer q.cond.L.Unlock()

	i64, err := q.seq.Next()
	if err != nil {
		return errors.Wrap(err, "badgerx.Queue")
	}
	key := fmt.Appendf(q.dataKey, "/%d", i64)
	if err := q.db.Update(func(tx *badger.Txn) error {
		return tx.Set(key, buf.Bytes())
	}); err != nil {
		return errors.Wrapf(err, "badgerx.Queue: set key %q", string(key))
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
				log.Error().Stack().Err(err).Msg("badgerx.Queue: listen")
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
			if err := gob.NewDecoder(reader).Decode(&value); err != nil {
				return errors.Wrapf(err, "decode %q into %T (key=%q)", string(b), value, string(item.Key()))
			}
			return nil
		}); err != nil {
			return err
		}
		if err := f(value); err != nil {
			return errors.Wrap(err, "handle value")
		}
		err := tx.Delete(item.Key())
		return errors.Wrapf(err, "delete key %q", string(item.Key()))
	}); err != nil {
		return err
	}
	q.size--
	return nil
}
