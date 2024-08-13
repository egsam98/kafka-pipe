package progress

import (
	"sync"
	"time"

	"github.com/cheggaaa/pb/v3"
	"github.com/rs/zerolog/log"
)

type Pool interface {
	Add(bar ...*pb.ProgressBar)
	Stop() error
}

type PeriodicPool struct {
	bars   []*pb.ProgressBar
	barsMu sync.RWMutex
	stop   chan struct{}
}

func RunPeriodicPool() *PeriodicPool {
	p := &PeriodicPool{stop: make(chan struct{})}
	go p.run()
	return p
}

func (p *PeriodicPool) Add(bar ...*pb.ProgressBar) {
	p.barsMu.Lock()
	p.bars = append(p.bars, bar...)
	p.barsMu.Unlock()
}

func (p *PeriodicPool) Stop() error {
	close(p.stop)
	p.print()
	return nil
}

func (p *PeriodicPool) run() {
	for {
		select {
		case <-p.stop:
			return
		case <-time.After(5 * time.Second):
			p.print()
		}
	}
}

func (p *PeriodicPool) print() {
	p.barsMu.RLock()
	for _, bar := range p.bars {
		log.Info().Stringer("bar", bar).Msg("Progress")
	}
	p.barsMu.RUnlock()
}
