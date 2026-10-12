package selected

import (
	"context"
	"errors"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/startupownership"
)

type authorityObservationFixture struct {
	probe func(context.Context) (startupownership.PossessionObservation, error)
}

func (s authorityObservationFixture) InspectAuthority(context.Context) (startupownership.AuthorityInspection, error) {
	return startupownership.EmptyAuthorityInspection("sqlite_retained_owner")
}

func (s authorityObservationFixture) ProbePossession(ctx context.Context) (startupownership.PossessionObservation, error) {
	return s.probe(ctx)
}

func TestAuthorityInspectionBoundsOnlyObservationAndPreservesCancellation(t *testing.T) {
	for _, name := range []string{"unbounded", "shorter-deadline", "cancelled", "probe-failure"} {
		t.Run(name, func(t *testing.T) {
			ctx := context.Background()
			if name == "shorter-deadline" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithTimeout(ctx, time.Second)
				defer cancel()
			}
			if name == "cancelled" {
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			failure := errors.New("native observation failed")
			var observed context.Context
			inspection := &AuthorityInspection{store: authorityObservationFixture{probe: func(bounded context.Context) (startupownership.PossessionObservation, error) {
				observed = bounded
				deadline, ok := bounded.Deadline()
				if !ok || time.Until(deadline) > 5*time.Second {
					t.Fatalf("observation is not bounded: %v %t", deadline, ok)
				}
				if parent, ok := ctx.Deadline(); ok && !deadline.Equal(parent) {
					t.Fatalf("caller deadline changed: %v -> %v", parent, deadline)
				}
				if err := bounded.Err(); err != nil {
					return startupownership.PossessionObservation{}, err
				}
				if name == "probe-failure" {
					return startupownership.PossessionObservation{}, failure
				}
				now := time.Now().UTC()
				return startupownership.PossessionObservation{Backend: "sqlite_retained_owner", StartedAt: now, FinishedAt: now}, nil
			}}}
			result, err := inspection.InspectAuthority(ctx)
			switch name {
			case "cancelled":
				if !errors.Is(err, context.Canceled) {
					t.Fatalf("caller cancellation lost: %v", err)
				}
			case "probe-failure":
				if !errors.Is(err, failure) {
					t.Fatalf("native error lost: %v", err)
				}
			default:
				if err != nil || result.Possession == nil || result.Possession.Available {
					t.Fatalf("healthy held observation: %+v %v", result, err)
				}
			}
			if observed == nil || !errors.Is(observed.Err(), context.Canceled) {
				t.Fatalf("observation context was not closed: %v", observed)
			}
		})
	}
}
