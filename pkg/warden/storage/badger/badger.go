package badger

import (
	"encoding/json"

	"github.com/dgraph-io/badger/v4"
	"github.com/pkg/errors"

	"kafka-pipe/pkg/warden"
)

const Folder = "data"

type Badger struct {
	namespace string
	db        *badger.DB
}

func NewBadger(namespace string) *Badger {
	return &Badger{namespace: namespace}
}

func (b *Badger) Open() (err error) {
	b.db, err = badger.Open(badger.
		DefaultOptions(Folder).
		WithLogger(new(loggerAdapter)))
	return
}

func (b *Badger) Get(key string, dst any) error {
	err := b.db.View(func(tx *badger.Txn) error {
		item, err := tx.Get([]byte(key))
		if err != nil {
			return err
		}
		return item.Value(func(val []byte) error { return json.Unmarshal(val, dst) })
	})
	if errors.Is(err, badger.ErrKeyNotFound) {
		return warden.ErrNotFound
	}
	return err
}

func (b *Badger) Set(key string, value any) error {
	return b.db.Update(func(tx *badger.Txn) error {
		b, err := json.Marshal(value)
		if err != nil {
			return err
		}
		return tx.Set([]byte(key), b)
	})
}

func (b *Badger) Close() error {
	return b.db.Close()
}
