package task

import (
	"context"
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Name     string
	Interval time.Duration
	Execute  func(context.Context) error
	ReloadCh chan struct{}

	mu      sync.Mutex
	running bool
	stop    chan struct{}
	cancel  context.CancelFunc
}

func (t *Task) Start(first bool) error {
	t.mu.Lock()
	if t.running {
		t.mu.Unlock()
		return nil
	}
	t.running = true
	t.stop = make(chan struct{})
	t.mu.Unlock()

	go func() {
		timer := time.NewTimer(t.Interval)
		defer timer.Stop()

		if first {
			t.executeTask()
		}

		for {
			timer.Reset(t.Interval)
			select {
			case <-timer.C:
				// continue
			case <-t.stop:
				return
			}

			t.executeTask()
		}
	}()

	return nil
}

func (t *Task) executeTask() {
	// Create a context that is only cancelled when Close() is called.
	// No timeout — HTTP-level timeouts (resty 15s + 1 retry) handle
	// slow connections. This eliminates leaked goroutines entirely.
	ctx, cancel := context.WithCancel(context.Background())

	t.mu.Lock()
	t.cancel = cancel
	t.mu.Unlock()

	err := t.Execute(ctx)
	cancel()

	if err != nil {
		log.Warnf("Task %s execution error: %v", t.Name, err)
	}
}

func (t *Task) Close() {
	t.mu.Lock()
	if t.running {
		t.running = false
		close(t.stop)
		if t.cancel != nil {
			t.cancel()
		}
	}
	t.mu.Unlock()
	log.Warningf("Task %s stopped", t.Name)
}
