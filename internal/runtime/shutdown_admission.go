package runtime

import (
	"context"
	"sync"
)

type shutdownAdmission struct {
	mu      sync.Mutex
	closed  bool
	nextID  uint64
	active  map[uint64]context.CancelFunc
	drained chan struct{}
}

func (a *shutdownAdmission) Close() {
	if a == nil {
		return
	}
	a.mu.Lock()
	a.closed = true
	a.mu.Unlock()
}

func (a *shutdownAdmission) Closed() bool {
	if a == nil {
		return false
	}
	a.mu.Lock()
	defer a.mu.Unlock()
	return a.closed
}

func (a *shutdownAdmission) Begin() (func(), bool) {
	_, release, admitted := a.BeginContext(context.Background())
	return release, admitted
}

func (a *shutdownAdmission) BeginContext(parent context.Context) (context.Context, func(), bool) {
	if a == nil {
		return parent, func() {}, true
	}
	if parent == nil {
		parent = context.Background()
	}
	a.mu.Lock()
	if a.closed {
		a.mu.Unlock()
		return parent, nil, false
	}
	if len(a.active) == 0 {
		a.active = make(map[uint64]context.CancelFunc)
		a.drained = make(chan struct{})
	}
	a.nextID++
	id := a.nextID
	ctx, cancel := context.WithCancel(parent)
	a.active[id] = cancel
	a.mu.Unlock()

	var once sync.Once
	release := func() {
		once.Do(func() {
			a.mu.Lock()
			cancel, exists := a.active[id]
			if exists {
				delete(a.active, id)
				if len(a.active) == 0 {
					close(a.drained)
				}
			}
			a.mu.Unlock()
			if exists {
				cancel()
			}
		})
	}
	return ctx, release, true
}

func (a *shutdownAdmission) Wait(ctx context.Context) error {
	if a == nil {
		return nil
	}
	a.mu.Lock()
	if len(a.active) == 0 {
		a.mu.Unlock()
		return nil
	}
	drained := a.drained
	a.mu.Unlock()
	select {
	case <-drained:
		return nil
	case <-ctx.Done():
		a.CancelActive()
		select {
		case <-drained:
			return nil
		default:
			return ctx.Err()
		}
	}
}

func (a *shutdownAdmission) CancelActive() {
	if a == nil {
		return
	}
	a.mu.Lock()
	cancels := make([]context.CancelFunc, 0, len(a.active))
	for _, cancel := range a.active {
		cancels = append(cancels, cancel)
	}
	a.mu.Unlock()
	for _, cancel := range cancels {
		cancel()
	}
}
