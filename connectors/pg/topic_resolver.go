package pg

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

// topicResolver resolves Kafka topic name by Postgres relation regular expressions
type topicResolver struct {
	cfg    *SourceConfig
	routes []relationTopic
	cache  map[string]string // map[pg relation]kafka topic
}

type relationTopic struct {
	pgRelation regexp.Regexp
	kafkaTopic string
}

func newTopicResolver(cfg *SourceConfig) (topicResolver, error) {
	res := topicResolver{
		cfg:    cfg,
		routes: make([]relationTopic, 0, len(cfg.Kafka.Topic.Routes)),
		cache:  make(map[string]string),
	}
	for pgRel, topic := range cfg.Kafka.Topic.Routes {
		reg, err := regexp.Compile(pgRel)
		if err != nil {
			return topicResolver{}, errors.Wrap(err, "parse Postgres relation regex")
		}
		res.routes = append(res.routes, relationTopic{
			pgRelation: *reg,
			kafkaTopic: topic,
		})
	}
	return res, nil
}

// resolve Kafka topic by Postgres relation name
func (t *topicResolver) resolve(pgRelation string) (topic string) {
	if topic, ok := t.cache[pgRelation]; ok {
		return topic
	}

	defer func() { t.cache[pgRelation] = topic }()

	prefix := t.cfg.Kafka.Topic.Prefix
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
