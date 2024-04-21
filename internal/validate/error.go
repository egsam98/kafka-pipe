package validate

import (
	"strings"
)

type Errors []string

func (e Errors) Error() string {
	var buf strings.Builder
	for i, msg := range e {
		if i > 0 {
			buf.WriteString("; ")
		}
		buf.WriteString(msg)
	}
	return buf.String()
}

func (e Errors) WithNamespace(namespace ...string) Errors {
	errs := make(Errors, len(e))
	for i, msg := range e {
		errs[i] = strings.Join(append(namespace, msg), ":")
	}
	return errs
}
