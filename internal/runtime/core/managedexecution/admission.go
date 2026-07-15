package managedexecution

import (
	"context"
	"fmt"
	"slices"
	"strings"
	"time"

	"github.com/google/uuid"
)

type Kind string

const (
	KindNormalRuntime        Kind = "normal_runtime"
	KindSelectedContractFork Kind = "selected_contract_fork"
)

type Admission struct {
	ID                     string    `json:"id"`
	Kind                   Kind      `json:"kind"`
	ExecutionAuthorityID   string    `json:"execution_authority_id"`
	Generation             uint64    `json:"generation"`
	RunID                  string    `json:"run_id,omitempty"`
	ActorCensusFingerprint string    `json:"actor_census_fingerprint"`
	BundleFingerprint      string    `json:"bundle_fingerprint"`
	CapabilitySurfaceIDs   []string  `json:"capability_surface_ids,omitempty"`
	IssuedAt               time.Time `json:"issued_at"`
}

func New(kind Kind, executionAuthorityID string, generation uint64, runID, actorCensusFingerprint, bundleFingerprint string, surfaceIDs []string) (Admission, error) {
	admission := Admission{
		ID: uuid.NewString(), Kind: kind, ExecutionAuthorityID: strings.TrimSpace(executionAuthorityID),
		Generation: generation, RunID: strings.TrimSpace(runID), ActorCensusFingerprint: strings.TrimSpace(actorCensusFingerprint),
		BundleFingerprint: strings.TrimSpace(bundleFingerprint), CapabilitySurfaceIDs: normalizeUUIDs(surfaceIDs), IssuedAt: time.Now().UTC(),
	}
	return admission, admission.Validate()
}

func (a Admission) Validate() error {
	if _, err := uuid.Parse(strings.TrimSpace(a.ID)); err != nil {
		return fmt.Errorf("managed execution admission id is invalid: %w", err)
	}
	if strings.TrimSpace(a.ExecutionAuthorityID) == "" || a.Generation == 0 || strings.TrimSpace(a.ActorCensusFingerprint) == "" || strings.TrimSpace(a.BundleFingerprint) == "" || a.IssuedAt.IsZero() {
		return fmt.Errorf("managed execution admission identity is incomplete")
	}
	switch a.Kind {
	case KindNormalRuntime:
		if a.RunID != "" {
			return fmt.Errorf("normal managed execution admission cannot carry fork run identity")
		}
	case KindSelectedContractFork:
		if _, err := uuid.Parse(strings.TrimSpace(a.ExecutionAuthorityID)); err != nil {
			return fmt.Errorf("selected-fork execution authority id is invalid: %w", err)
		}
		if _, err := uuid.Parse(strings.TrimSpace(a.RunID)); err != nil {
			return fmt.Errorf("selected-fork run id is invalid: %w", err)
		}
	default:
		return fmt.Errorf("managed execution admission kind %q is invalid", a.Kind)
	}
	for _, id := range a.CapabilitySurfaceIDs {
		if _, err := uuid.Parse(id); err != nil {
			return fmt.Errorf("managed execution admission surface id is invalid: %w", err)
		}
	}
	return nil
}

func (a Admission) AuthorizesNormal() bool {
	return a.Validate() == nil && a.Kind == KindNormalRuntime
}

func (a Admission) AuthorizesSelected(executionID, runID string, generation uint64) bool {
	return a.Validate() == nil && a.Kind == KindSelectedContractFork &&
		a.ExecutionAuthorityID == strings.TrimSpace(executionID) && a.RunID == strings.TrimSpace(runID) && a.Generation == generation
}

func (a Admission) WithCapabilitySurfaces(surfaceIDs []string) (Admission, error) {
	if err := a.Validate(); err != nil {
		return Admission{}, err
	}
	next := a
	next.CapabilitySurfaceIDs = normalizeUUIDs(surfaceIDs)
	return next, next.Validate()
}

type contextKey struct{}

func WithAdmission(ctx context.Context, admission Admission) context.Context {
	if ctx == nil {
		ctx = context.Background()
	}
	return context.WithValue(ctx, contextKey{}, admission)
}

func FromContext(ctx context.Context) (Admission, bool) {
	if ctx == nil {
		return Admission{}, false
	}
	admission, ok := ctx.Value(contextKey{}).(Admission)
	return admission, ok && admission.Validate() == nil
}

func Require(ctx context.Context) (Admission, error) {
	admission, ok := FromContext(ctx)
	if !ok {
		return Admission{}, fmt.Errorf("managed execution admission is required")
	}
	return admission, nil
}

func normalizeUUIDs(values []string) []string {
	out := make([]string, 0, len(values))
	for _, value := range values {
		value = strings.TrimSpace(value)
		if value != "" {
			out = append(out, value)
		}
	}
	slices.Sort(out)
	return slices.Compact(out)
}
