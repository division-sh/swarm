package channelonboarding

import (
	"context"
	"fmt"
	"strings"
)

// The serving owner's current admitted catalog is the source of a rollover,
// never a fuzzy durable-successor match or authority borrowed from an old bundle.
// Execution remains deferred to Recover, after the serving grant is admitted.
func (s *Service) RollSessionSources(ctx context.Context) error {
	owner, supported := s.sessions.(interface {
		RollSessionSource(context.Context, Operation, Candidate) (Operation, error)
	})
	if !supported {
		return nil
	}
	catalog, err := s.catalog()
	if err != nil {
		return err
	}
	operations, err := s.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		return err
	}
	principal, err := s.identities.Principal()
	if err != nil {
		return err
	}
	for _, op := range operations {
		if op.PrincipalID != principal.ID || op.Phase != PhaseSucceeded || !op.AccountConnection() || op.IdentityOperationID != "" {
			continue
		}
		if _, current := catalog.FindDurableSuccessor(op.Provider, op.Interface, op.Coordinate, op.TargetSelector, op.Posture, op.Ceremony); current {
			continue
		}
		current, err := RetainedSessionCurrent(ctx, s.store, op)
		if err != nil {
			return err
		}
		if !current {
			continue
		}
		name, _, valid := strings.Cut(op.Coordinate.BundleIdentity, "@")
		if !valid || name == "" {
			return ErrRevisionConflict
		}
		var matches []Candidate
		for _, candidate := range catalog.candidates {
			nextName, _, _ := strings.Cut(candidate.Coordinate.BundleIdentity, "@")
			if nextName != name || candidate.Provider != op.Provider || candidate.Posture != op.Posture || candidate.Ceremony != op.Ceremony ||
				candidate.Interface.Normalized() != op.Interface.Normalized() || candidate.Target.Selector != op.TargetSelector ||
				candidate.Coordinate.BundleHash == op.Coordinate.BundleHash {
				continue
			}
			matches = append(matches, candidate)
		}
		switch len(matches) {
		case 0:
			continue
		case 1:
		default:
			return fmt.Errorf("%w: source rollover requires one exact current channel declaration", ErrConflict)
		}
		unlock, err := s.lockDrive(ctx, op.OperationID)
		if err != nil {
			return err
		}
		_, err = owner.RollSessionSource(ctx, op, matches[0])
		unlock()
		if err != nil {
			return err
		}
	}
	return nil
}
