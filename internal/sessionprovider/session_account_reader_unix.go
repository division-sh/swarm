//go:build linux || darwin

package sessionprovider

import (
	"context"
	"fmt"

	"github.com/division-sh/swarm/internal/operatorchannel"
)

func (s *sessionState) sessionAccountScope() (string, string, context.Context, error) {
	if s == nil || s.directory == nil {
		return "", "", nil, fmt.Errorf("session authority requires its original declared operation and private state")
	}
	occurrence := s.currentOccurrence()
	if occurrence == nil {
		return "whatsapp", s.directory.connectionID, nil, errClientOccurrenceFenced
	}
	return "whatsapp", s.directory.connectionID, occurrence.ctx, nil
}

// The concrete adapter verifies the SDK device under the original occurrence
// lease. It returns no authority token; the common owner still issues that.
func (s *sessionState) acquireSessionAccount(ctx context.Context, expected operatorchannel.SessionAccountAdmission) (context.Context, func() bool, func(), error) {
	occurrence := s.currentOccurrence()
	if occurrence == nil {
		return nil, nil, nil, errClientOccurrenceFenced
	}
	workCtx, release, err := occurrence.acquire(ctx)
	if err != nil {
		return nil, nil, nil, err
	}
	retained := false
	defer func() {
		if !retained {
			release()
		}
	}()
	device, err := s.device(workCtx)
	if err != nil {
		return nil, nil, nil, err
	}
	if device.ID == nil || device.ID.ToNonAD().String() != expected.AccountRef || workCtx.Err() != nil {
		return nil, nil, nil, errSessionAccount
	}
	current := func() bool { return s.ownsConnectedOccurrence(workCtx, occurrence) }
	retained = true
	return workCtx, current, release, nil
}

func (s *sessionState) failSessionAuthority(err error) {
	if occurrence := s.currentOccurrence(); occurrence != nil {
		occurrence.recordFailure("runtime release failed", false)
		occurrence.mu.Lock()
		callbacks := occurrence.callbacks
		occurrence.mu.Unlock()
		if callbacks != nil {
			callbacks.fail("crash", err)
			return
		}
		occurrence.fence()
	}
}
