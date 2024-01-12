package source

import (
	"regexp"
	"strings"

	"github.com/pkg/errors"
)

type topicResolver struct {
	cfg   *Config
	regs  []regex
	cache map[string]string
}

type regex struct {
	regexp.Regexp
	topic string
}

func newTopicResolver(cfg *Config) (topicResolver, error) {
	res := topicResolver{
		cfg:   cfg,
		regs:  make([]regex, 0, len(cfg.Kafka.Topic.Routes)),
		cache: make(map[string]string),
	}
	for expr, topic := range cfg.Kafka.Topic.Routes {
		reg, err := regexp.Compile(expr)
		if err != nil {
			return topicResolver{}, errors.WithStack(err)
		}
		res.regs = append(res.regs, regex{
			Regexp: *reg,
			topic:  topic,
		})
	}
	return res, nil
}

func (t *topicResolver) resolve(rel string) (topic string) {
	if topic, ok := t.cache[rel]; ok {
		return topic
	}

	defer func() { t.cache[rel] = topic }()

	prefix := t.cfg.Kafka.Topic.Prefix
	if prefix != "" {
		prefix += "."
	}
	for _, reg := range t.regs {
		if reg.MatchString(rel) {
			return prefix + strings.TrimPrefix(reg.topic, prefix)
		}
	}
	return prefix + rel
}
