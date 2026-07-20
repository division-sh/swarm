package serveapp

import "github.com/division-sh/swarm/internal/runtime"

func (s *runtimeProjectSupervisor) CurrentRuntime() *runtime.Runtime {
	if s == nil {
		return nil
	}
	s.mu.RLock()
	defer s.mu.RUnlock()
	return s.currentRT
}
