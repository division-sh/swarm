package startuprecovery

import (
	"context"
	"errors"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/runbundle"
)

type AvailabilityReader interface {
	ActiveNonStandingRunBundleAvailabilities(context.Context) ([]runbundle.Availability, error)
}

type Request struct {
	AvailabilityReader AvailabilityReader
}

type Result struct {
	CheckedAvailabilities []runbundle.Availability
	DataIntegrityErrors   []runbundle.Availability
}

type DataIntegrityError struct {
	Conflicts []runbundle.Availability
}

func (e DataIntegrityError) Error() string {
	details := make([]string, 0, len(e.Conflicts))
	for _, conflict := range e.Conflicts {
		details = append(details, conflict.DetailString())
	}
	return fmt.Sprintf("%s: source artifact data integrity failure for %d active non-standing run(s): %s", runbundle.CodeBundleDataIntegrityError, len(e.Conflicts), strings.Join(details, "; "))
}

func IsDataIntegrityError(err error) bool {
	var target DataIntegrityError
	return errors.As(err, &target)
}

func Recover(ctx context.Context, req Request) (Result, error) {
	if ctx == nil {
		ctx = context.Background()
	}
	if req.AvailabilityReader == nil {
		return Result{}, fmt.Errorf("startup recovery availability reader is required")
	}
	availabilities, err := req.AvailabilityReader.ActiveNonStandingRunBundleAvailabilities(ctx)
	if err != nil {
		return Result{}, err
	}
	result := Result{CheckedAvailabilities: append([]runbundle.Availability(nil), availabilities...)}
	for _, availability := range availabilities {
		switch {
		case availability.Available():
			continue
		case availability.DataIntegrityError():
			result.DataIntegrityErrors = append(result.DataIntegrityErrors, availability)
		default:
			return result, fmt.Errorf("startup recovery unsupported bundle availability for run %s: %s", availability.RunID, availability.DetailString())
		}
	}
	if len(result.DataIntegrityErrors) > 0 {
		return result, DataIntegrityError{Conflicts: append([]runbundle.Availability(nil), result.DataIntegrityErrors...)}
	}
	return result, nil
}
