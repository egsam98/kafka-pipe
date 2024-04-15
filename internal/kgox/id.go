package kgox

import (
	"encoding/json"
	"fmt"

	"github.com/pkg/errors"
)

// TODO draft
type Key[T any] struct {
	Value T `json:"id"`
}

func NewKey[T any](data map[string]any) (*Key[T], error) {
	dataID, ok := data["id"]
	if !ok {
		return nil, errors.Errorf(`default ID key "id" is not specified for data: %v`, data)
	}
	return &Key[T]{Value: dataID.(T)}, nil
}

func (k *Key[T]) ToBytes() []byte {
	if _, ok := any(k.Value).(string); ok {
		return fmt.Appendf(nil, `{"id": %q}`, k.Value)
	}
	return fmt.Appendf(nil, `{"id": %v}`, k.Value)
}

func (k *Key[T]) Parse(src []byte) error {
	return json.Unmarshal(src, k)
}
