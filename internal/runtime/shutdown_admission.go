package runtime

import "sync"

type shutdownAdmission struct {
	mu     sync.RWMutex
	closed bool
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
	a.mu.RLock()
	defer a.mu.RUnlock()
	return a.closed
}

func (a *shutdownAdmission) Begin() (func(), bool) {
	if a == nil {
		return func() {}, true
	}
	a.mu.RLock()
	if a.closed {
		a.mu.RUnlock()
		return nil, false
	}
	return a.mu.RUnlock, true
}
