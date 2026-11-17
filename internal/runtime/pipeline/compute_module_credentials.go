package pipeline

import (
	"context"
	"errors"
	"sort"
	"strings"
	"unicode/utf8"

	"github.com/division-sh/swarm/internal/runtime/credentials"
)

func (pc *PipelineCoordinator) computeSecretInputs(ctx context.Context, bindings map[string]string) (map[string]string, error) {
	// The engine passes only the selected admitted row. No store capability leaves
	// this adapter, and observations are reused within this invocation only.
	owner, err := credentials.NewSnapshotOwner(pc.credentials)
	if err != nil {
		return nil, errors.New("compute_module secret input unavailable")
	}
	parameters := make([]string, 0, len(bindings))
	for parameter := range bindings {
		parameters = append(parameters, parameter)
	}
	sort.Strings(parameters)
	observed := map[string]string{}
	inputs := make(map[string]string, len(bindings))
	for _, parameter := range parameters {
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		key := bindings[parameter]
		value, present := observed[key]
		if !present {
			snapshot, err := owner.Observe(ctx, key)
			if err != nil || !snapshot.Present {
				return nil, errors.New("compute_module secret input unavailable")
			}
			value = snapshot.CredentialValue()
			if strings.TrimSpace(value) == "" || !utf8.ValidString(value) {
				return nil, errors.New("compute_module secret input unavailable")
			}
			observed[key] = value
		}
		inputs[parameter] = value
	}
	return inputs, nil
}
