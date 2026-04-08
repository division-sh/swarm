package runtime

import "sync/atomic"

type shutdownAdmission struct {
	closed atomic.Bool
}

func (a *shutdownAdmission) Close() {
	if a == nil {
		return
	}
	a.closed.Store(true)
}

func (a *shutdownAdmission) Closed() bool {
	if a == nil {
		return false
	}
	return a.closed.Load()
}
