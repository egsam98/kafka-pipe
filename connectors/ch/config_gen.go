// Code generated by Warden. DO NOT EDIT.

package ch

import (
	warden "github.com/egsam98/warden"
	"net/url"
	"strconv"
)

func (self *SinkConfig) Validate() error {
	var errs warden.Errors
	if self.Name == "" {
		errs.Add("name", warden.Error("required"))
	}
	errs.Add("kafka", self.Kafka.Validate())
	errs.Add("click_house", self.ClickHouse.Validate())
	if self.Serde == nil {
		errs.Add("Serde", warden.Error("required"))
	}
	if self.DB == nil {
		errs.Add("DB", warden.Error("required"))
	}
	return errs.AsError()
}

func (self *ClickHouseConfig) Validate() error {
	var errs warden.Errors
	if self.Database == "" {
		errs.Add("database", warden.Error("required"))
	}
	if self.User == "" {
		errs.Add("user", warden.Error("required"))
	}
	if len(self.Addrs) == 0 {
		errs.Add("addrs", warden.Error("must be non empty"))
	}
	errs.Add("addrs", func() error {
		var errs warden.Errors
		for i, elem := range self.Addrs {
			if _, err := url.Parse(elem); err != nil {
				errs.Add(strconv.Itoa(i), warden.Error("must be URL"))
			}
		}
		return errs.AsError()
	}())
	return errs.AsError()
}
