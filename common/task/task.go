package task

import (
	"sync"
	"time"

	log "github.com/sirupsen/logrus"
)

type Task struct {
	Name     string
	Interval time.Duration
	Execute  func() error
	Access   sync.RWMutex
	Running  bool
	Stop     chan struct{}
	Done     chan struct{} // closed when goroutine actually exits
}

// Start launches the periodic task. Simple ticker loop — each API call
// uses its own resty timeout, never leaks goroutines, never corrupts state.
func (t *Task) Start(first bool) error {
	t.Access.Lock()
	if t.Running {
		t.Access.Unlock()
		return nil
	}
	t.Running = true
	t.Stop = make(chan struct{})
	t.Done = make(chan struct{})
	t.Access.Unlock()
	go func() {
		defer close(t.Done) // signal that goroutine has exited
		timer := time.NewTimer(t.Interval)
		defer timer.Stop()
		if first {
			if err := t.Execute(); err != nil {
				return
			}
		}

		for {
			timer.Reset(t.Interval)
			select {
			case <-timer.C:
				// continue
			case <-t.Stop:
				return
			}

			if err := t.Execute(); err != nil {
				log.Errorf("Task %s execution error: %v", t.Name, err)
				return
			}
		}
	}()

	return nil
}

func (t *Task) safeStop() {
	t.Access.Lock()
	if t.Running {
		t.Running = false
		close(t.Stop)
	}
	t.Access.Unlock()
}

// Close signals the task to stop and WAITS for the goroutine to fully
// exit (up to 30s). This prevents race conditions during reload where
// the old goroutine accesses a nil dispatcher after V2Core.Close().
func (t *Task) Close() {
	t.safeStop()
	if t.Done != nil {
		select {
		case <-t.Done:
			// goroutine exited cleanly
		case <-time.After(30 * time.Second):
			log.Warnf("Task %s did not stop within 30s, proceeding", t.Name)
		}
	}
	log.Warningf("Task %s stopped", t.Name)
}
