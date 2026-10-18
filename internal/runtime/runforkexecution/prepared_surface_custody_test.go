package runforkexecution

import (
	"context"
	"errors"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/runtime/agenttopology"
	"github.com/division-sh/swarm/internal/runtime/core/agentidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedcapabilities"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/google/uuid"
)

type preparedSurfaceCustodyStore struct {
	managedcapabilities.Persistence
	t        *testing.T
	process  startupownership.Authority
	calls    int
	retained context.Context
	refusal  error
}

func (s *preparedSurfaceCustodyStore) SaveManagedCapabilitySurface(ctx context.Context, surface managedcapabilities.Surface) error {
	s.t.Helper()
	original, err := startupownership.CaptureMutationAuthority(ctx)
	if err != nil || original.HasRuntimeGrant() || original.ProcessAuthority() != s.process ||
		!startupownership.PreparedOperationEntered(ctx, startupownership.PreparedManagedCapabilitySurface, s) {
		s.t.Fatalf("prepared save lost exact process custody: %+v %v", original, err)
	}
	s.calls++
	s.retained = context.WithoutCancel(ctx)
	if s.refusal != nil {
		return s.refusal
	}
	return s.Persistence.SaveManagedCapabilitySurface(ctx, surface)
}

func provePreparedSurfaceCustody(t *testing.T, backend string) {
	t.Helper()
	var selected startupownership.Store
	if backend == "sqlite" {
		selected = storetest.StartSQLiteRuntimeStore(t)
	} else {
		selected = storetest.StartPostgresRuntimeStore(t)
	}
	ctx := runForkTestContext(t)
	process := selectedContractTestProcessCapability(t, ctx, selected)
	evidence, err := process.Evidence()
	if err != nil {
		t.Fatal(err)
	}
	identity := selectedContractTestAgentIdentityForRun(t, uuid.NewString(), "prepared-worker", "global")
	actor, err := identity.Plan()
	if err != nil {
		t.Fatal(err)
	}
	fingerprint, err := actor.Fingerprint()
	if err != nil {
		t.Fatal(err)
	}
	plan := managedcapabilities.Plan{
		ActorPlan: actor, RuntimeMode: "startup_probe", Provider: "test", Transport: "cli", ProviderContract: "prepared-custody-proof", CreatedAt: time.Now().UTC(),
		Authority: managedcapabilities.Authority{
			Kind: managedcapabilities.AuthorityStartupProbe, ID: uuid.NewString(), ExecutionKind: managedcapabilities.ExecutionSelectedForkPreparation, ExecutionAuthorityID: uuid.NewString(),
			Preparation: &managedcapabilities.PreparedSelectedForkProbeAuthority{
				SelectedForkPreparationCoordinates: managedcapabilities.SelectedForkPreparationCoordinates{
					ProcessAuthorityID: evidence.AuthorityID, ProcessOwnerID: evidence.OwnerID, ProcessBootID: evidence.BootID,
					BundleHash: runForkTestBundleHash, SourceFingerprint: strings.Repeat("a", 64), AdmittedPlanFingerprint: strings.Repeat("b", 64),
					ConfigurationFingerprint: strings.Repeat("c", 64), CatalogFingerprint: strings.Repeat("d", 64),
				}, ActorPlanFingerprint: fingerprint,
			},
		},
	}
	surface, err := managedcapabilities.New(plan)
	if err != nil {
		t.Fatal(err)
	}
	store := &preparedSurfaceCustodyStore{Persistence: selected.(managedcapabilities.Persistence), t: t, process: evidence}
	save := func(command context.Context, authority managedcapabilities.Authority, candidate managedcapabilities.Surface) error {
		operation, err := startupownership.PreparedManagedCapabilitySurfaceOperation(command, process, store, authority, candidate)
		if err != nil {
			return err
		}
		return process.RunPreparedOperation(command, operation)
	}
	if err := save(ctx, surface.Authority, surface); err != nil {
		t.Fatal(err)
	}
	bound, err := startupownership.WithProcessMutationAuthority(ctx, process)
	if err != nil {
		t.Fatal(err)
	}
	if err := save(bound, surface.Authority, surface); err != nil || store.calls != 2 {
		t.Fatalf("same-process preparation failed: calls=%d err=%v", store.calls, err)
	}
	if startupownership.HasMutationAuthority(ctx) || startupownership.PreparedOperationEntered(store.retained, startupownership.PreparedManagedCapabilitySurface, store) {
		t.Fatal("prepared command mutated its parent or retained an ended scope")
	}
	before, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil {
		t.Fatal(err)
	}
	foreignStore := storetest.StartSQLiteRuntimeStore(t)
	foreign := selectedContractTestProcessCapability(t, ctx, foreignStore)
	foreignCtx, err := startupownership.WithProcessMutationAuthority(ctx, foreign)
	if err != nil {
		t.Fatal(err)
	}
	refusedCtx, _ := startupownership.WithRuntimeMutationAuthority(ctx, nil)
	canceled, cancel := context.WithCancel(ctx)
	cancel()
	for _, negative := range []struct {
		name string
		ctx  context.Context
	}{{"foreign_original", foreignCtx}, {"refused_original", refusedCtx}, {"canceled", canceled}} {
		t.Run(negative.name, func(t *testing.T) {
			if err := save(negative.ctx, surface.Authority, surface); err == nil || store.calls != 2 {
				t.Fatalf("invalid original reached surface persistence: calls=%d err=%v", store.calls, err)
			}
		})
	}
	for _, field := range []string{"normal", "execution", "probe", "preparation", "actor", "catalog", "process", "integrity"} {
		t.Run(field, func(t *testing.T) {
			candidate := surface.Clone()
			switch field {
			case "normal":
				candidate.Authority.ExecutionKind = managedcapabilities.ExecutionNormalAgent
				candidate.Authority.Preparation = nil
				candidate.Authority.StartupOwnerID = "ordinary-startup"
				candidate.Authority.StartupGeneration = 1
			case "execution":
				candidate.Authority.ExecutionKind = managedcapabilities.ExecutionSelectedContractFork
				candidate.Authority.Kind = managedcapabilities.AuthorityProviderTurn
				candidate.Authority.Preparation = nil
				candidate.Authority.RunID = identity.RunID
				candidate.Authority.SessionID = uuid.NewString()
				candidate.Authority.TurnOrdinal = 1
			case "probe":
				candidate.Authority.ID = uuid.NewString()
			case "preparation":
				candidate.Authority.ExecutionAuthorityID = uuid.NewString()
			case "actor":
				candidate.Authority.Preparation.ActorPlanFingerprint = strings.Repeat("e", 64)
			case "catalog":
				candidate.Authority.Preparation.CatalogFingerprint = strings.Repeat("e", 64)
			case "process":
				candidate.Authority.Preparation.ProcessAuthorityID = uuid.NewString()
			case "integrity":
				candidate.IntegrityHash = strings.Repeat("e", 64)
			}
			if field != "integrity" && field != "actor" {
				changed := plan
				changed.Authority = candidate.Authority
				if field == "execution" {
					changed.ActorPlan, changed.ActorIdentity = agentidentity.Plan{}, identity
				}
				var err error
				candidate, err = managedcapabilities.New(changed)
				if err != nil {
					t.Fatal(err)
				}
			}
			expected := surface.Authority
			if field == "normal" || field == "execution" {
				expected = candidate.Authority
			}
			if err := save(ctx, expected, candidate); err == nil || store.calls != 2 {
				t.Fatalf("crossed surface reached persistence: calls=%d err=%v", store.calls, err)
			}
		})
	}
	after, err := storetest.ReadSelectedForkApplicationStorageSnapshot(ctx, selected)
	if err != nil || !reflect.DeepEqual(before, after) {
		t.Fatalf("refused preparation changed application storage: %v", err)
	}
	sourcePlan, err := agenttopology.NewSourceSetPlan([]agenttopology.SourceCoordinate{{BundleHash: runForkTestBundleHash}}, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := process.InstallCompleteSourceSet(ctx, agenttopology.SourceSetCommitRequest{OperationID: uuid.NewString(), Plan: sourcePlan}); err != nil {
		t.Fatal(err)
	}
	grant, err := process.IssueGenerationGrant(ctx, startupownership.GrantRequest{BundleHash: runForkTestBundleHash, RuntimeInstanceID: evidence.RuntimeInstanceID, RuntimeGeneration: 1, SourceSetRevision: sourcePlan.Revision})
	if err != nil {
		t.Fatal(err)
	}
	original, err := startupownership.WithRuntimeMutationAuthority(ctx, grant)
	if err != nil {
		t.Fatal(err)
	}
	frozen, err := startupownership.FreezeMutationAuthority(original)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := grant.MarkProbesSettled(ctx, nil); err != nil {
		t.Fatal(err)
	}
	if err := save(frozen, surface.Authority, surface); err == nil || store.calls != 2 {
		t.Fatalf("stale original reached surface persistence: calls=%d err=%v", store.calls, err)
	}
	if err := grant.Retire(ctx); err != nil {
		t.Fatal(err)
	}
	if err := save(original, surface.Authority, surface); err == nil || store.calls != 2 {
		t.Fatalf("retired original reached surface persistence: calls=%d err=%v", store.calls, err)
	}
	store.refusal = errors.New("exact synchronous surface refusal")
	if err := save(ctx, surface.Authority, surface); !errors.Is(err, store.refusal) || store.calls != 3 {
		t.Fatalf("prepared command changed store refusal: calls=%d err=%v", store.calls, err)
	}
	if err := process.Release(context.Background()); err != nil {
		t.Fatal(err)
	}
	if err := save(ctx, surface.Authority, surface); err == nil || store.calls != 3 {
		t.Fatalf("released process reached persistence: calls=%d err=%v", store.calls, err)
	}
}
