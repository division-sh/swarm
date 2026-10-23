package contracts

import (
	"fmt"

	"github.com/division-sh/swarm/internal/runtime/core/eventidentity"
)

func eventNameAdmissionError(cause error, names ...string) error {
	for _, name := range names {
		if err := eventidentity.ProviderKeyError(name); err != nil {
			return fmt.Errorf("%w; %w", cause, err)
		}
	}
	return cause
}
