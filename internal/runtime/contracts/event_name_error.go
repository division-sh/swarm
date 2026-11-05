package contracts

import (
	"fmt"

	runtimeidentity "github.com/division-sh/swarm/internal/runtime/core/identity"
)

func eventNameAdmissionError(cause error, names ...string) error {
	for _, name := range names {
		if err := runtimeidentity.ProviderEventKeyError(name); err != nil {
			return fmt.Errorf("%w; %w", cause, err)
		}
	}
	return cause
}
