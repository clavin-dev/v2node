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
	// Per-cycle watchdog. Bound how long one execution may run, then ALWAYS
	// return to the loop. Execute runs in a child goroutine with a deadline
	// context that is threaded into every network call, so if a call hangs
	// (e.g. an HTTP read stuck despite the resty client timeout — the exact
	// failure that froze nodeInfoMonitor inside GetNodeInfo and made the node
	// show offline forever), the deadline fires, cancels the in-flight
	// request, the stuck goroutine unblocks and exits (no leak), and the
	// periodic loop proceeds to the next tick. A stuck cycle is skipped, never
	// fatal — self-healing without a full reload.
	timeout := 5 * t.Interval
	if timeout > 5*time.Minute {
		timeout = 5 * time.Minute
	}
	if timeout <= 0 {
		timeout = time.Minute
	}
	ctx, cancel := context.WithTimeout(context.Background(), timeout)
	defer cancel()

	done := make(chan struct{})
	go func() {
		defer close(done)
		defer func() {
			if r := recover(); r != nil {
				log.Errorf("Task %s panicked (recovered, task continues): %v", t.Name, r)
			}
		}()
		if err := t.Execute(ctx); err != nil {
			log.Errorf("Task %s execution error (task continues): %v", t.Name, err)
		}
	}()

	select {
	case <-done:
	case <-ctx.Done():
		// Deadline hit. The deferred cancel() cancels ctx, which unblocks any
		// network call the Execute goroutine is stuck in (the request context
		// is wired through), so it exits on its own. We return immediately so
		// the loop continues — the next tick starts a fresh cycle.
		log.Errorf("Task %s timed out after %s, cancelling stuck cycle and continuing", t.Name, timeout)
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
