package pg

import (
	"regexp"
	"strings"
	"sync"

	"github.com/pkg/errors"

	kafkapipe "github.com/egsam98/kafka-pipe"
)

// topicResolver resolves Kafka topic name by Postgres relation regular expressions
type topicResolver struct {
	cfg     *kafkapipe.ProducerConfig
	routes  []relationTopic
	cache   map[string]string // map[pg relation]kafka topic
	muCache sync.RWMutex
}

type relationTopic struct {
	pgRelation regexp.Regexp
	kafkaTopic string
}

func newTopicResolver(cfg *kafkapipe.ProducerConfig) (*topicResolver, error) {
	res := topicResolver{
		cfg:    cfg,
		routes: make([]relationTopic, 0, len(cfg.Topic.Routes)),
		cache:  make(map[string]string),
	}
	for pgRel, topic := range cfg.Topic.Routes {
		reg, err := regexp.Compile(pgRel)
		if err != nil {
			return nil, errors.Wrap(err, "parse Postgres relation regex")
		}
		res.routes = append(res.routes, relationTopic{
			pgRelation: *reg,
			kafkaTopic: topic,
		})
	}
	return &res, nil
}

// resolve Kafka topic by Postgres relation name
func (t *topicResolver) resolve(pgRelation string) (topic string) {
	t.muCache.RLock()
	topic, ok := t.cache[pgRelation]
	t.muCache.RUnlock()
	if ok {
		return topic
	}

	defer func() {
		t.muCache.Lock()
		t.cache[pgRelation] = topic
		t.muCache.Unlock()
	}()

	prefix := t.cfg.Topic.Prefix
	if prefix != "" {
		prefix += "."
	}
	for _, reg := range t.routes {
		if reg.pgRelation.MatchString(pgRelation) {
			return prefix + strings.TrimPrefix(reg.kafkaTopic, prefix)
		}
	}
	return prefix + pgRelation
}
