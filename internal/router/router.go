package router

import (
	"regexp"

	"github.com/pkg/errors"
)

// Router defines mapping from regular expression to target string.
// Ex. Database table with partitions => Kafka topic: transaction_\d+ => transactions
type Router struct {
	routes []route
}

type route struct {
	from regexp.Regexp
	to   string
}

func NewRouter(routes map[string]string) (*Router, error) {
	r := Router{routes: make([]route, 0, len(routes))}
	for from, to := range routes {
		re, err := regexp.Compile(from)
		if err != nil {
			return nil, errors.Wrap(err, "compile route's regular expression")
		}
		r.routes = append(r.routes, route{
			from: *re,
			to:   to,
		})
	}
	return &r, nil
}

func (r *Router) Route(from string) string {
	for _, r := range r.routes {
		if r.from.MatchString(from) {
			return r.to
		}
	}
	return from
}
