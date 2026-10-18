package sessionprovider

import (
	"errors"
	"os"
	"sync"

	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

// InvocationState owns dev-only native files, not provider execution authority.
// It creates nothing until an admitted connection holds counted work.
type InvocationState struct {
	mu          sync.Mutex
	process     *worklifetime.Process
	parent      *os.File
	base        *os.File
	name        string
	path        string
	retired     bool
	retireFiles func(*InvocationState) error
}

func NewInvocationState(process *worklifetime.Process) (*InvocationState, error) {
	if process == nil {
		return nil, errors.New("native invocation state requires its process owner")
	}
	return &InvocationState{process: process}, nil
}

func (s *InvocationState) Retire(receipt *worklifetime.ProcessJoinReceipt) error {
	if s == nil || s.process == nil {
		return errSessionProtection
	}
	if err := s.process.ValidateJoinReceipt(receipt); err != nil {
		return err
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	s.retired = true
	if s.retireFiles == nil {
		return nil
	}
	return s.retireFiles(s)
}
