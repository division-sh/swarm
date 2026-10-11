//go:build !linux && !darwin

package sessionprovider

import (
	"errors"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func (s *InvocationState) Retire(receipt *worklifetime.ProcessJoinReceipt) error {
	if s == nil {
		return errors.New("native invocation state requires its process owner")
	}
	if err := s.process.ValidateJoinReceipt(receipt); err != nil {
		return err
	}
	s.mu.Lock()
	s.retired = true
	s.mu.Unlock()
	return nil
}
