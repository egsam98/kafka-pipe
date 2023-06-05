package saramax

import (
	"encoding/json"

	"github.com/pkg/errors"
)

type jsonEncoder struct {
	value any
}

func JsonEncoder(value any) *jsonEncoder {
	return &jsonEncoder{value: value}
}

func (j *jsonEncoder) Encode() ([]byte, error) {
	b, err := json.Marshal(j.value)
	return b, errors.Wrapf(err, "marshal %+v (%T)", j.value, j.value)
}

func (j *jsonEncoder) Length() int {
	return 0
}
