package timex

import (
	"database/sql/driver"
	"strconv"
	"time"
)

type UnixMicro struct {
	time.Time
}

func (u UnixMicro) MarshalJSON() ([]byte, error) {
	return []byte(strconv.FormatInt(u.UnixMicro(), 10)), nil
}

func (u *UnixMicro) UnmarshalJSON(src []byte) error {
	num, err := strconv.ParseInt(string(src), 10, 64)
	if err != nil {
		return err
	}
	u.Time = time.UnixMicro(num)
	return nil
}

func (u UnixMicro) Value() (driver.Value, error) {
	return u.Time, nil
}
