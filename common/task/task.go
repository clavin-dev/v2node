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
			t.runOnce()
		}

		for {
			timer.Reset(t.Interval)
			select {
			case <-timer.C:
				// continue
			case <-t.Stop:
				return
			}

			t.runOnce()
		}
	}()

	return nil
}

// runOnce executes the task body exactly once. A failed cycle — an error
// return OR a panic (e.g. a panel returning empty/500, a transient network
// blip, a nil deref during a rebuild) — is logged and swallowed so the
// periodic loop ALWAYS continues. This is critical: previously a single
// Execute error permanently killed the goroutine, so e.g. one bad heartbeat
// cycle would stop nodeInfoMonitor forever and the node would show offline
// indefinitely while its (separate) reporting task kept running. Now a bad
// cycle is just skipped and the next interval retries.
func (t *Task) runOnce() {
	defer func() {
		if r := recover(); r != nil {
			log.Errorf("Task %s panicked (recovered, task continues): %v", t.Name, r)
		}
	}()
	if err := t.Execute(); err != nil {
		log.Errorf("Task %s execution error (task continues): %v", t.Name, err)
	}
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
