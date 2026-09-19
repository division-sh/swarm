package serveapp

import (
	"errors"

	"github.com/division-sh/swarm/internal/config"
)

// Snapshot the declaration for this runtime occurrence. Only the selected
// process capacity owner may resolve defaults or impose backend limits.
func configuredFanOutWorkers(cfg *config.Config) (*int, error) {
	if cfg == nil {
		return nil, errors.New("runtime config is required")
	}
	if err := cfg.Runtime.ValidateFanOutWorkers(); err != nil {
		return nil, err
	}
	if cfg.Runtime.FanOutWorkers == nil {
		return nil, nil
	}
	workers := *cfg.Runtime.FanOutWorkers
	return &workers, nil
}
