package sink

type Config struct {
	Kafka struct {
		GroupID string   `yaml:"group_id"`
		Brokers []string `yaml:"brokers"`
		Topics  []string `yaml:"topics"`
	} `yaml:"kafka"`
	ClickHouse struct {
		Database string   `yaml:"database"`
		User     string   `yaml:"user"`
		Password string   `yaml:"password"`
		Addrs    []string `yaml:"addrs"`
	} `yaml:"click_house"`
}

func (c *Config) Parse(src []byte) error {
	return nil
}
