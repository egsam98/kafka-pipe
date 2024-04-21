package backup

import (
	"github.com/egsam98/kafka-pipe/connector"
)

func init() {
	connector.Register("s3.Backup", func(cfg connector.Config) (connector.Connector, error) {
		return NewBackup(cfg)
	})
}
