package runforkexecution

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorread"
	"github.com/division-sh/swarm/internal/runtime/authoractivity"
	"github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/core/managedexecution"
	"github.com/division-sh/swarm/internal/runtime/core/worklifetime"
	"github.com/division-sh/swarm/internal/runtime/correlation"
	"github.com/division-sh/swarm/internal/runtime/decisioncard"
	"github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionmode"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/replycontext"
	"github.com/division-sh/swarm/internal/runtime/runfork"
	"github.com/division-sh/swarm/internal/runtime/runforkadmission"
	"github.com/division-sh/swarm/internal/runtime/semanticvalue"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/runtime/testfixtures/canonicalrouting"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil"
	"github.com/google/uuid"
)

const pendingReplyBeforeJournalCrashCut = "retained_pending_effect_before_journal"

type pendingReplyCrashEvidence struct {
	OriginalSourceRun string
	PrincipalID       string
	Authority         effects.Authority
	Admission         managedexecution.Admission
	Grant             startupownership.GrantEvidence
}

// The selected checkpoint has no ordinary-authority plan generation. Capture
// only its exact selected coordinates, never serialize the full authority sum.
type pendingReplyCrashCheckpointDTO struct {
	OriginalSourceRun string
	PrincipalID       string
	Authority         pendingReplySelectedAuthorityDTO
	Admission         pendingReplySelectedAdmissionDTO
	Grant             pendingReplySelectedGrantDTO
}

type pendingReplySelectedAuthorityDTO struct {
	Kind            effects.AuthorityKind
	ID              string
	SelectedFork    effects.SelectedContractForkAuthority
	ExecutionOwner  string
	LeaseExpiresAt  time.Time
	FenceGeneration uint64
	BudgetScopes    []effects.BudgetAdmissionScope
	ExecutionMode   executionmode.Mode
}

type pendingReplySelectedAdmissionDTO struct {
	ID                     string
	Kind                   managedexecution.Kind
	ExecutionAuthorityID   string
	Generation             uint64
	RunID                  string
	ActorCensusFingerprint string
	BundleHash             string
	CapabilitySurfaceIDs   []string
	IssuedAt               time.Time
}

type pendingReplySelectedGrantDTO struct {
	GrantID            string
	ProcessAuthorityID string
	ProcessOwnerID     string
	ProcessBootID      string
	BundleHash         string
	RuntimeInstanceID  string
	RuntimeGeneration  uint64
	SourceSetRevision  string
	StateVersion       uint64
	State              startupownership.GrantState
	ProbeSurfaceIDs    []string
	SelectedFork       *startupownership.SelectedForkGrantBinding
}

func (e pendingReplyCrashEvidence) checkpointDTO() pendingReplyCrashCheckpointDTO {
	a, m, g := e.Authority, e.Admission, e.Grant
	return pendingReplyCrashCheckpointDTO{
		OriginalSourceRun: e.OriginalSourceRun, PrincipalID: e.PrincipalID,
		Authority: pendingReplySelectedAuthorityDTO{Kind: a.Kind, ID: a.ID, SelectedFork: a.SelectedFork,
			ExecutionOwner: a.ExecutionOwner, LeaseExpiresAt: a.LeaseExpiresAt, FenceGeneration: a.FenceGeneration,
			BudgetScopes: a.BudgetScopes, ExecutionMode: a.ExecutionMode},
		Admission: pendingReplySelectedAdmissionDTO{ID: m.ID, Kind: m.Kind, ExecutionAuthorityID: m.ExecutionAuthorityID,
			Generation: m.Generation, RunID: m.RunID, ActorCensusFingerprint: m.ActorCensusFingerprint,
			BundleHash: m.BundleHash, CapabilitySurfaceIDs: m.CapabilitySurfaceIDs, IssuedAt: m.IssuedAt},
		Grant: pendingReplySelectedGrantDTO{GrantID: g.GrantID, ProcessAuthorityID: g.ProcessAuthorityID,
			ProcessOwnerID: g.ProcessOwnerID, ProcessBootID: g.ProcessBootID, BundleHash: g.BundleHash,
			RuntimeInstanceID: g.RuntimeInstanceID, RuntimeGeneration: g.RuntimeGeneration, SourceSetRevision: g.SourceSetRevision,
			StateVersion: g.StateVersion, State: g.State, ProbeSurfaceIDs: g.ProbeSurfaceIDs, SelectedFork: g.SelectedFork},
	}
}

func (d pendingReplyCrashCheckpointDTO) evidence() pendingReplyCrashEvidence {
	a, m, g := d.Authority, d.Admission, d.Grant
	return pendingReplyCrashEvidence{
		OriginalSourceRun: d.OriginalSourceRun, PrincipalID: d.PrincipalID,
		Authority: effects.Authority{Kind: a.Kind, ID: a.ID, SelectedFork: a.SelectedFork,
			ExecutionOwner: a.ExecutionOwner, LeaseExpiresAt: a.LeaseExpiresAt, FenceGeneration: a.FenceGeneration,
			BudgetScopes: a.BudgetScopes, ExecutionMode: a.ExecutionMode},
		Admission: managedexecution.Admission{ID: m.ID, Kind: m.Kind, ExecutionAuthorityID: m.ExecutionAuthorityID,
			Generation: m.Generation, RunID: m.RunID, ActorCensusFingerprint: m.ActorCensusFingerprint,
			BundleHash: m.BundleHash, CapabilitySurfaceIDs: m.CapabilitySurfaceIDs, IssuedAt: m.IssuedAt},
		Grant: startupownership.GrantEvidence{GrantID: g.GrantID, ProcessAuthorityID: g.ProcessAuthorityID,
			ProcessOwnerID: g.ProcessOwnerID, ProcessBootID: g.ProcessBootID, BundleHash: g.BundleHash,
			RuntimeInstanceID: g.RuntimeInstanceID, RuntimeGeneration: g.RuntimeGeneration, SourceSetRevision: g.SourceSetRevision,
			StateVersion: g.StateVersion, State: g.State, ProbeSurfaceIDs: g.ProbeSurfaceIDs, SelectedFork: g.SelectedFork},
	}
}

func TestPendingReplyCrashCheckpointDTORetainsCapturedEvidence(t *testing.T) {
	for _, test := range []struct {
		name     string
		surfaces []string
	}{{"nil", nil}, {"empty", []string{}}, {"populated", []string{uuid.NewString()}}} {
		t.Run(test.name, func(t *testing.T) {
			at := time.Unix(1700000000, 123456000).UTC()
			executionID, runID := uuid.NewString(), uuid.NewString()
			fingerprint := "sha256:" + strings.Repeat("a", 64)
			selected := effects.SelectedContractForkAuthority{ExecutionID: executionID, ForkRunID: runID, Generation: 7,
				AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint,
				ActorCensusFingerprint: fingerprint, EffectiveConfigFingerprint: fingerprint}
			captured := pendingReplyCrashEvidence{
				OriginalSourceRun: uuid.NewString(), PrincipalID: uuid.NewString(),
				Authority: effects.Authority{Kind: effects.AuthoritySelectedContractFork, ID: executionID, SelectedFork: selected,
					ExecutionOwner: "captured-executor", LeaseExpiresAt: at.Add(time.Minute), FenceGeneration: 11, ExecutionMode: executionmode.Live},
				Admission: managedexecution.Admission{ID: uuid.NewString(), Kind: managedexecution.KindSelectedContractFork,
					ExecutionAuthorityID: executionID, Generation: selected.Generation, RunID: runID,
					ActorCensusFingerprint: fingerprint, BundleHash: runForkTestBundleHash, CapabilitySurfaceIDs: test.surfaces, IssuedAt: at},
				Grant: startupownership.GrantEvidence{GrantID: uuid.NewString(), ProcessAuthorityID: uuid.NewString(),
					ProcessOwnerID: "captured-process", ProcessBootID: uuid.NewString(), BundleHash: runForkTestBundleHash,
					RuntimeInstanceID: runForkTestRuntimeInstanceID, RuntimeGeneration: selected.Generation,
					StateVersion: 3, State: startupownership.GrantAdmitted, ProbeSurfaceIDs: test.surfaces,
					SelectedFork: &startupownership.SelectedForkGrantBinding{BindingID: uuid.NewString(), ForkRunID: runID,
						ExecutionID: executionID, ExecutionGeneration: selected.Generation, FenceGeneration: 11,
						ExecutionOwner: "captured-executor", AdmissionFingerprint: fingerprint, ContainerPlanFingerprint: fingerprint,
						ActorCensusFingerprint: fingerprint, EffectiveConfigFingerprint: fingerprint,
						DeclarationPlanFingerprint: fingerprint, PreparationFingerprint: fingerprint}},
			}
			if !captured.Authority.Valid() || captured.Admission.Validate() != nil || captured.Grant.Validate() != nil {
				t.Fatal("round-trip fixture requires valid selected evidence")
			}
			raw, err := json.Marshal(captured.checkpointDTO())
			if err != nil {
				t.Fatal(err)
			}
			var retained pendingReplyCrashCheckpointDTO
			if err := json.Unmarshal(raw, &retained); err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(captured, retained.evidence()) {
				t.Fatal("DTO changed captured authority, configuration, admission or grant coordinates")
			}
		})
	}
}

// C08 starts with an activated, copied pending card and NO activity journal or
// provider result. Recorded-result recovery is a different crash boundary.
func TestIssue642FreshPendingEffectBeforeJournalSIGKILLRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected startupownership.Store
			var reopen func() startupownership.Store
			var construct func() SelectedContractExecutionOwner
			var dsn string
			if backend == "sqlite" {
				s, again := storetest.StartSQLiteRuntimeStoreWithReopen(t, context.Background())
				selected, dsn = s, s.Path()
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractSQLiteExecutionOwnerForTest(t, s) }
			} else {
				dsn = testutil.StartPostgresDSN(t)
				s, again := storetest.StartPostgresRuntimeStoreWithReopen(t, dsn)
				selected = s
				reopen = func() startupownership.Store { s = again(); return s }
				construct = func() SelectedContractExecutionOwner { return newSelectedContractExecutionOwnerForTest(t, s) }
			}
			var calls atomic.Int32
			provider := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
				calls.Add(1)
				var input map[string]string
				if err := json.NewDecoder(r.Body).Decode(&input); err != nil || r.Method != http.MethodPost ||
					!reflect.DeepEqual(input, map[string]string{"account_id": "original-account", "request_id": "original-request"}) {
					t.Errorf("fresh successor provider input: method=%s input=%+v error=%v", r.Method, input, err)
					w.WriteHeader(http.StatusBadRequest)
					return
				}
				w.Header().Set("Content-Type", "application/json")
				_ = json.NewEncoder(w).Encode(map[string]string{"account_id": input["account_id"], "request_id": input["request_id"], "result": "child-approved-only"})
			}))
			t.Cleanup(provider.Close)
			t.Setenv("SWARM_SELECTED_PENDING_REPLY_PROVIDER", provider.URL)
			// The killed process cannot release sealed projections. The parent
			// owns all scratch roots without extending the shared crash helper.
			root := t.TempDir()
			t.Cleanup(func() {
				if err := filepath.WalkDir(root, func(path string, entry os.DirEntry, err error) error {
					if err != nil {
						return err
					}
					if entry.IsDir() {
						return os.Chmod(path, 0o700)
					}
					return nil
				}); err != nil {
					t.Errorf("unseal owned pending-card crash fixture: %v", err)
				}
			})
			t.Setenv("TMPDIR", root)
			evidencePath := filepath.Join(root, "pending-card-checkpoint.json")
			t.Setenv("SWARM_SELECTED_PENDING_REPLY_CHECKPOINT", evidencePath)
			checkpoint := killSelectedForkAtCheckpoint(t, backend, dsn, pendingReplyBeforeJournalCrashCut)
			raw, err := os.ReadFile(evidencePath)
			if err != nil {
				t.Fatal(err)
			}
			var captured pendingReplyCrashCheckpointDTO
			decodeErr := json.Unmarshal(raw, &captured)
			previous := captured.evidence()
			if decodeErr != nil || !previous.Authority.Valid() || previous.Grant.Validate() != nil ||
				previous.Grant.SelectedFork == nil || previous.Grant.State != startupownership.GrantAdmitted || previous.PrincipalID == "" ||
				previous.OriginalSourceRun == "" || previous.Authority.SelectedFork.ForkRunID != checkpoint.ForkRun ||
				previous.Grant.SelectedFork.ExecutionID != previous.Authority.ID ||
				!previous.Admission.AuthorizesSelected(previous.Authority.ID, checkpoint.ForkRun, previous.Authority.SelectedFork.Generation) || calls.Load() != 0 {
				t.Fatalf("pre-journal SIGKILL lacks actual pending executor evidence: %+v calls=%d error=%v", previous, calls.Load(), decodeErr)
			}
			read, stopRead := context.WithTimeout(t.Context(), 20*time.Second)
			defer stopRead()
			store := selected.(pendingReplyExecutionStore)
			card := requirePendingReplyCard(t, read, store, checkpoint.ForkRun)
			pending, err := store.LoadProposedEffectContinuation(read, card.CardID)
			if err != nil {
				t.Fatal(err)
			}
			rows := requirePendingReplyBeforeJournal(t, read, selected, checkpoint.ForkRun, pending)
			operation := loadPendingReplyRecoveryOperation(t, read, selected, checkpoint.ForkRun)
			if operation.Request.SourceRunID != checkpoint.SourceRun || !operation.Request.AtStart || operation.Request.ResolvedPoint == nil {
				t.Fatalf("pending-card crash lost the permanent context-only start cut: %+v", operation)
			}
			state, err := storetest.ReadSelectedExecutionStorage(read, selected, checkpoint.ForkRun)
			if err != nil || state.Occurrences != 1 || state.State != "running" || state.RunStatus != "running" {
				t.Fatalf("pending-card crash was not activated current execution: %+v error=%v", state, err)
			}
			sourceRuns := []string{previous.OriginalSourceRun, checkpoint.SourceRun}
			sourceDomains := make([]map[string]storetest.SelectedForkStorageTableSnapshot, len(sourceRuns))
			sourceCards := make([]decisioncard.Card, len(sourceRuns))
			sourceEffects := make([]decisioncard.ProposedEffectContinuation, len(sourceRuns))
			sourceReplies := make([][]replycontext.Record, len(sourceRuns))
			for i, sourceRun := range sourceRuns {
				cards, cursor, err := store.ListDecisionCards(read, decisioncard.ListOptions{RunID: sourceRun, Limit: 2})
				if err != nil || len(cards) != 1 || cursor != "" {
					t.Fatalf("exact source card inventory: %+v cursor=%q error=%v", cards, cursor, err)
				}
				sourceCards[i], err = store.GetDecisionCard(read, cards[0].CardID)
				if err != nil {
					t.Fatal(err)
				}
				sourceEffects[i], err = store.LoadProposedEffectContinuation(read, cards[0].CardID)
				if err != nil {
					t.Fatal(err)
				}
				sourceReplies[i], err = storetest.ReadReplyContextRecords(read, selected, sourceRun)
				if err != nil || len(sourceReplies[i]) != 2 || pending.ReplyContextID == sourceEffects[i].ReplyContextID ||
					card.CardID == cards[0].CardID || !pending.Input.Equal(sourceEffects[i].Input) {
					t.Fatalf("pending child borrowed source card/input/return: error=%v", err)
				}
				for _, sourceRow := range sourceReplies[i] {
					for _, childRow := range rows {
						if sourceRow.ID == childRow.ID {
							t.Fatal("pending child reused a source reply identity")
						}
					}
				}
				if attempts, err := storetest.ReadActivityAttemptStorage(read, selected, sourceRun); err != nil || len(attempts) != 0 {
					t.Fatalf("pending source already executed its journal: %+v error=%v", attempts, err)
				}
				sourceDomains[i], err = storetest.ReadSelectedForkSourceDomain(read, selected, sourceRun)
				if err != nil {
					t.Fatal(err)
				}
			}
			if sourceCards[0].Status != decisioncard.StatusPending || sourceCards[1].Status != decisioncard.StatusSuperseded || sourceCards[1].SupersededReason != "run_forked" {
				t.Fatalf("source journey lost the pending ancestor or exact immediate-source freeze: %+v", sourceCards)
			}
			frozenSource, err := store.LoadRunLifecycleSnapshot(read, checkpoint.SourceRun)
			if err != nil || frozenSource.Status != runfork.RunForkSourceFrozenStatus || frozenSource.EndedAt == nil ||
				!sourceCards[1].UpdatedAt.Equal(*frozenSource.EndedAt) {
				t.Fatalf("immediate source lost its exact mandatory freeze: %+v error=%v", frozenSource, err)
			}
			selected = reopen()
			store = selected.(pendingReplyExecutionStore)
			persistedCard, err := store.GetDecisionCard(read, card.CardID)
			persistedPending, pendingErr := store.LoadProposedEffectContinuation(read, card.CardID)
			persistedRows := requirePendingReplyBeforeJournal(t, read, selected, checkpoint.ForkRun, persistedPending)
			if err != nil || pendingErr != nil || !reflect.DeepEqual(card, persistedCard) || !reflect.DeepEqual(pending, persistedPending) || !reflect.DeepEqual(rows, persistedRows) {
				t.Fatalf("native reopen changed the unapproved card/effect/returns: errors=%v/%v", err, pendingErr)
			}
			ctx := runForkTestContext(t)
			capability := selectedContractTestProcessCapability(t, ctx, selected)
			owner := construct()
			process, _ := worklifetime.ProcessFromContext(ctx)
			if err := owner.BindSelectedProcess(ctx, process, capability); err != nil {
				t.Fatal(err)
			}
			baseline := process.ActiveCount()
			t.Cleanup(func() {
				cleanup, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := owner.RetireSelectedContexts(cleanup); err != nil {
					t.Error(err)
				}
			})
			claims := make(chan effects.Authority, 4)
			owner.ports.runtimeExecution = selectedActivitySuccessorClaim{owner.ports.runtimeExecution, claims}
			loader := SourceArtifactSelectedContractSourceLoader{RepoRoot: runForkExecutionRepoRoot(t), Store: selected.(SourceArtifactSelectedContractSourceStore)}
			wait, cancel := context.WithTimeout(ctx, 20*time.Second)
			defer cancel()
			recovered, err := owner.RecoverSelectedForkContexts(wait, effects.NewRecoveryRequest(time.Now().UTC(), executionposture.Live), SelectedForkRecoveryEnvironment{
				SourceLoader: loader, AgentRuntime: SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability}})
			if err != nil || len(recovered) != 2 {
				t.Fatalf("pending-card native recovery: %+v error=%v", recovered, err)
			}
			matched := 0
			for _, entry := range recovered {
				if entry.RunID == checkpoint.ForkRun {
					matched++
					if entry.Disposition != runfork.SelectedForkRecoveryResume || entry.Operation == nil || !reflect.DeepEqual(operation, *entry.Operation) {
						t.Fatalf("pending child did not resume its original acknowledgment: %+v", entry)
					}
				} else if entry.RunID != checkpoint.SourceRun || entry.Disposition != runfork.SelectedForkRecoveryTerminal || entry.ExecutionID != "" || entry.Continuation != nil {
					t.Fatalf("recovery did not terminal-preserve the mandatorily frozen context-only source: %+v", entry)
				}
			}
			if matched != 1 {
				t.Fatalf("pending-card recovery did not resume exactly one child: %+v", recovered)
			}
			var current effects.Authority
			select {
			case current = <-claims:
			case <-wait.Done():
				t.Fatal("pending-card recovery never claimed its current successor")
			}
			if len(claims) != 0 || !current.Valid() || current.SelectedFork.ForkRunID != checkpoint.ForkRun ||
				current.ID == previous.Authority.ID || current.SelectedFork.Generation <= previous.Authority.SelectedFork.Generation {
				t.Fatalf("pending-card recovery retained old executor: old=%+v current=%+v", previous.Authority, current)
			}
			if err := owner.ports.runtimeExecution.HeartbeatRunForkSelectedContractRuntimeExecution(wait, current, time.Minute); err != nil {
				t.Fatalf("current pending successor lacks native authority: %v", err)
			}
			if err := owner.ports.runtimeExecution.HeartbeatRunForkSelectedContractRuntimeExecution(wait, previous.Authority, time.Minute); err == nil {
				t.Fatal("SIGKILL predecessor retained native execution authority")
			}
			principal, err := store.EnsureOperatorPrincipal(wait, time.Now().UTC())
			if err != nil || principal.ID != previous.PrincipalID {
				t.Fatalf("recovery changed its durable operator principal: %+v error=%v", principal, err)
			}
			requirePendingReplyRecoveryRefusals(t, wait, selected, store, owner, card, principal.ID, previous, current)
			requirePendingReplyBeforeJournal(t, wait, selected, checkpoint.ForkRun, pending)
			if calls.Load() != 0 {
				t.Fatalf("recovery/refusal executed an unapproved provider: calls=%d", calls.Load())
			}
			approvePendingReplyCard(t, wait, store, owner, card)
			waitPendingReplyReturn(t, wait, selected, store, checkpoint.ForkRun, rows)
			decided, err := store.GetDecisionCard(wait, card.CardID)
			if err != nil || decided.Status != decisioncard.StatusDecided || decided.Verdict != "approve" || decided.DecidedBy != principal.ID || decided.DecisionEventID == "" {
				t.Fatalf("successor decision is not its own principal-bound approval: %+v error=%v", decided, err)
			}
			journal := requirePendingReplyRecoveryResult(t, wait, selected, store, pending, decided, rows)
			if calls.Load() != 1 {
				t.Fatalf("fresh pending-card successor provider calls=%d, want 1", calls.Load())
			}
			retire, stop := context.WithTimeout(context.WithoutCancel(ctx), 10*time.Second)
			defer stop()
			if err := owner.RetireSelectedContexts(retire); err != nil || process.ActiveCount() != baseline {
				t.Fatalf("pending successor did not join explicit retirement: work=%d baseline=%d error=%v", process.ActiveCount(), baseline, err)
			}
			selected = reopen()
			store = selected.(pendingReplyExecutionStore)
			read, stopFinalRead := context.WithTimeout(context.WithoutCancel(ctx), 20*time.Second)
			defer stopFinalRead()
			durable, found, err := store.LoadActivityAttempt(read, journal.RequestEventID)
			if err != nil || !found || !reflect.DeepEqual(journal, durable) {
				t.Fatalf("fresh successor journal did not survive native reopen: %+v error=%v", durable, err)
			}
			if actual, err := store.GetDecisionCard(read, card.CardID); err != nil || !reflect.DeepEqual(decided, actual) {
				t.Fatalf("fresh decision did not survive native reopen: %+v error=%v", actual, err)
			}
			waitPendingReplyReturn(t, read, selected, store, checkpoint.ForkRun, rows)
			requirePendingReplyRecoveryResult(t, read, selected, store, pending, decided, rows)
			if actual := loadPendingReplyRecoveryOperation(t, read, selected, checkpoint.ForkRun); !reflect.DeepEqual(operation, actual) {
				t.Fatal("pending-card recovery changed its permanent activation acknowledgment")
			}
			for i, sourceRun := range sourceRuns {
				after, domainErr := storetest.ReadSelectedForkSourceDomain(read, selected, sourceRun)
				actualCard, cardErr := store.GetDecisionCard(read, sourceCards[i].CardID)
				actualEffect, effectErr := store.LoadProposedEffectContinuation(read, sourceCards[i].CardID)
				actualRows, rowsErr := storetest.ReadReplyContextRecords(read, selected, sourceRun)
				if domainErr != nil || cardErr != nil || effectErr != nil || rowsErr != nil || !reflect.DeepEqual(sourceDomains[i], after) ||
					!reflect.DeepEqual(sourceCards[i], actualCard) || !reflect.DeepEqual(sourceEffects[i], actualEffect) || !reflect.DeepEqual(sourceReplies[i], actualRows) {
					t.Fatalf("pending-card successor changed source %s: errors=%v/%v/%v/%v", sourceRun, domainErr, cardErr, effectErr, rowsErr)
				}
				if attempts, err := storetest.ReadActivityAttemptStorage(read, selected, sourceRun); err != nil || len(attempts) != 0 {
					t.Fatalf("pending successor executed source journal: %+v error=%v", attempts, err)
				}
			}
			if actual, err := store.LoadRunLifecycleSnapshot(read, checkpoint.SourceRun); err != nil || !reflect.DeepEqual(frozenSource, actual) {
				t.Fatalf("successor changed the immediate source freeze: %+v error=%v", actual, err)
			}
			if calls.Load() != 1 || process.ActiveCount() != baseline {
				t.Fatalf("retirement/reopen repeated provider work or retained leases: calls=%d work=%d baseline=%d", calls.Load(), process.ActiveCount(), baseline)
			}
			t.Logf("ISSUE642_PENDING_CARD_BEFORE_JOURNAL_SIGKILL_RECOVERED backend=%s child=%s old_generation=%d new_generation=%d provider_calls=%d leases=%d", backend,
				checkpoint.ForkRun, previous.Authority.SelectedFork.Generation, current.SelectedFork.Generation, calls.Load(), process.ActiveCount())
		})
	}
}

func runPendingEffectReplyCrashHelper(t *testing.T, ctx context.Context, selected any, owner SelectedContractExecutionOwner, capability startupownership.ProcessCapability) {
	t.Helper()
	ctx, cancel := context.WithTimeout(ctx, 20*time.Second)
	defer cancel()
	repo := runForkExecutionRepoRoot(t)
	loader := admittedFixtureSelectedContractSourceLoader{RepoRoot: repo,
		SourceRoot: canonicalrouting.CopyNestedPendingActivityReply(t, os.Getenv("SWARM_SELECTED_PENDING_REPLY_PROVIDER")), PlatformSpecPath: filepath.Join(repo, "platform-spec.yaml")}
	loaded, err := loader.LoadRunForkSelectedContractSource(ctx, runfork.RunForkContractSelection{Mode: "selected_contracts"})
	if err != nil {
		t.Fatal(err)
	}
	sourceRun := uuid.NewString()
	ctx = correlation.WithRunID(correlation.WithSourceArtifactFact(ctx, loaded.SourceArtifactFact), sourceRun)
	ctx = effects.WithExecutionMode(ctx, executionmode.Live)
	ctx = authoractivity.WithScope(ctx, authoractivity.BundleScope(runForkTestRuntimeInstanceID, loaded.SourceArtifactFact.BundleHash()))
	at := time.Now().UTC()
	bundle, _ := semanticview.Bundle(loaded.Source)
	storetest.RequireRun(t, ctx, selected.(storetest.RunFixtureStore), storetest.RunFixture{Origin: storetest.ScenarioSetupOrigin(), RunID: sourceRun,
		BundleHash: loaded.SourceArtifactFact.BundleHash(), Artifact: bundle.SourceArtifact, StartedAt: at})
	rt, _ := nodeReplySourceRuntime(t, ctx, selected, owner, loaded, executionposture.Live)
	ctx = worklifetime.WithOccurrence(ctx, rt.WorkOccurrence())
	created, err := rt.Manager.EnsureFlowInstance(ctx, pipeline.FlowInstanceActivationRequest{ContractBundle: loaded.Source,
		Instance: flowidentity.Stored(loaded.Source, ".", sourceRun, sourceRun, "", ""), OccurredAt: at})
	if err != nil || !created {
		t.Fatalf("construct pending recovery source: created=%t error=%v", created, err)
	}
	store := selected.(pendingReplyExecutionStore)
	publishPendingReplyInput(t, ctx, rt.Bus, sourceRun, "request.started", []byte(`{"account_id":"original-account","request_id":"original-request"}`))
	original := requirePendingReplyCard(t, ctx, store, sourceRun)
	originalEffect, err := store.LoadProposedEffectContinuation(ctx, original.CardID)
	if err != nil {
		t.Fatal(err)
	}
	originalRows := requirePendingReplyBeforeJournal(t, ctx, selected, sourceRun, originalEffect)
	publishPendingReplyInput(t, ctx, rt.Bus, sourceRun, "request.note", []byte(`{"note":"approval-still-pending"}`))
	page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: sourceRun, EventName: "request.noted"}, Limit: 2})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("pending recovery lacks actual note cut: %+v error=%v", page, err)
	}
	before, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	if err != nil {
		t.Fatal(err)
	}
	selection := runforkadmission.SelectedContractSelection(loaded.Source)
	options := SelectedContractAgentRuntimeOptions{ExecutionPosture: executionposture.Live, ProcessCapability: capability}
	prepared, err := owner.Prepare(ctx, SelectedContractExecutionRequest{SourceRunID: sourceRun, At: page.Events[0].EventID, AllowSourceFreeze: true,
		Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options})
	if err != nil {
		t.Fatal(err)
	}
	defer prepared.Close()
	request, err := prepared.MaterializationRequest()
	if err != nil {
		t.Fatal(err)
	}
	staged, err := owner.ports.fork.MaterializeRunForkForSelectedContractExecution(ctx, request)
	if err != nil {
		t.Fatalf("stage actual pending-card reply journey: %v", err)
	}
	if err := prepared.Close(); err != nil {
		t.Fatal(err)
	}
	stagedCard := requirePendingReplyCard(t, ctx, store, staged.ForkRunID)
	stagedBefore, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, staged.ForkRunID)
	if err != nil {
		t.Fatal(err)
	}
	result, err := ExecuteSelectedContractRunFork(ctx, SelectedContractExecutionRequest{SourceRunID: staged.ForkRunID, AtStart: true, AllowSourceFreeze: true,
		Owner: owner, SourceLoader: loader, ContractSelection: selection, AgentRuntime: options,
		ForkOperation: &runfork.ForkOperationRequest{OperationID: uuid.NewString(), Actor: "pending-reply-crash", IdempotencyKey: "before-journal",
			TransportHash: "pending-reply-crash-transport", SourceRunID: staged.ForkRunID, AtStart: true, TargetBundleHash: loaded.SourceArtifactFact.BundleHash(),
			AllowSourceFreeze: true, ContractSelection: selection}})
	if err != nil || !result.Activation.Activated || !result.Activation.SourceFrozen {
		t.Fatalf("pending crash child did not activate/freeze its immediate source: %+v error=%v", result, err)
	}
	requirePendingReplySourceFreeze(t, ctx, store, stagedCard)
	card := requirePendingReplyCard(t, ctx, store, result.Materialization.ForkRunID)
	pending, err := store.LoadProposedEffectContinuation(ctx, card.CardID)
	if err != nil || pending.ReplyContextID == originalEffect.ReplyContextID || !pending.Input.Equal(originalEffect.Input) {
		t.Fatalf("pending crash child borrowed source effect: %+v error=%v", pending, err)
	}
	requirePendingReplyBeforeJournal(t, ctx, selected, card.RunID, pending)
	after, err := storetest.ReadSelectedForkSourceDomain(ctx, selected, sourceRun)
	stagedAfter, stagedErr := storetest.ReadSelectedForkSourceDomain(ctx, selected, staged.ForkRunID)
	actualCard, cardErr := store.GetDecisionCard(ctx, original.CardID)
	actualEffect, effectErr := store.LoadProposedEffectContinuation(ctx, original.CardID)
	actualRows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, sourceRun)
	if err != nil || stagedErr != nil || cardErr != nil || effectErr != nil || rowsErr != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(stagedBefore, stagedAfter) ||
		!reflect.DeepEqual(original, actualCard) || !reflect.DeepEqual(originalEffect, actualEffect) || !reflect.DeepEqual(originalRows, actualRows) {
		t.Fatalf("pending crash preparation changed source work: errors=%v/%v/%v/%v/%v", err, stagedErr, cardErr, effectErr, rowsErr)
	}
	use, isSelected, err := owner.AcquireSelectedDecisionCardControl(ctx, card.RunID, runfork.ControlMailboxDecide)
	if err != nil || !isSelected || use == nil {
		t.Fatalf("activated pending crash child lacks actual control: selected=%t error=%v", isSelected, err)
	}
	authority, hasAuthority := effects.AuthorityFromContext(use.WorkContext())
	admission, hasAdmission := managedexecution.FromContext(use.WorkContext())
	grant, hasGrant := startupownership.GenerationGrantEvidenceFromContext(use.WorkContext())
	principal, err := store.EnsureOperatorPrincipal(ctx, time.Now().UTC())
	if err != nil || !hasAuthority || !hasAdmission || !hasGrant {
		t.Fatalf("pending crash control lost actual admitted evidence: error=%v", err)
	}
	if err := use.Close(); err != nil {
		t.Fatal(err)
	}
	// Test-only evidence preserves the actual predecessor grant, not a new
	// database fixture, shared checkpoint type or execution owner.
	captured := pendingReplyCrashEvidence{OriginalSourceRun: sourceRun, PrincipalID: principal.ID, Authority: authority, Admission: admission, Grant: grant}
	raw, err := json.Marshal(captured.checkpointDTO())
	if err != nil {
		t.Fatal(err)
	}
	var roundTrip pendingReplyCrashCheckpointDTO
	if err := json.Unmarshal(raw, &roundTrip); err != nil {
		t.Fatal(err)
	}
	if !reflect.DeepEqual(captured, roundTrip.evidence()) {
		t.Fatal("selected checkpoint DTO changed or omitted captured predecessor evidence")
	}
	if err := os.WriteFile(os.Getenv("SWARM_SELECTED_PENDING_REPLY_CHECKPOINT"), raw, 0o600); err != nil {
		t.Fatal(err)
	}
	pipe := os.NewFile(3, "selected-pending-card-checkpoint")
	if err := json.NewEncoder(pipe).Encode(selectedForkCrashCheckpoint{SourceRun: staged.ForkRunID, ForkRun: card.RunID}); err != nil {
		t.Fatal(err)
	}
	_ = pipe.Close()
	var b [1]byte
	_, err = os.Stdin.Read(b[:])
	t.Fatalf("pre-journal pending-card checkpoint resumed without SIGKILL: %v", err)
}

func requirePendingReplyBeforeJournal(t *testing.T, ctx context.Context, selected any, runID string, pending decisioncard.ProposedEffectContinuation) []replycontext.Record {
	t.Helper()
	actual, err := selected.(pendingReplyExecutionStore).LoadProposedEffectContinuation(ctx, pending.CardID)
	attempts, attemptsErr := storetest.ReadActivityAttemptStorage(ctx, selected, runID)
	storage, storageErr := storetest.ReadProposedEffectRunExecutionStorage(ctx, selected, runID)
	rows, rowsErr := storetest.ReadReplyContextRecords(ctx, selected, runID)
	if err != nil || attemptsErr != nil || storageErr != nil || rowsErr != nil || !reflect.DeepEqual(pending, actual) || pending.RunID != runID ||
		pending.State != decisioncard.ProposedEffectPending || pending.DecisionEventID != "" || pending.RouteEventID != "" || pending.ReplyContextID == "" ||
		len(attempts) != 0 || storage.Requests != 0 || storage.SuccessfulAttempts != 0 || len(rows) != 2 {
		t.Fatalf("boundary is not a fresh pending effect before ANY journal/request: pending=%+v attempts=%+v storage=%+v rows=%+v errors=%v/%v/%v/%v", pending, attempts, storage, rows, err, attemptsErr, storageErr, rowsErr)
	}
	byID := map[string]replycontext.Record{}
	for _, row := range rows {
		if row.RunID != runID || row.State != replycontext.StateOpen || row.AcceptedReplyEventID != "" || row.TerminalAt != nil {
			t.Fatalf("pre-journal boundary lost its own open return: %+v", row)
		}
		byID[row.ID] = row
	}
	inner, found := byID[pending.ReplyContextID]
	outer, enclosed := byID[inner.EnclosingReplyContextID]
	if !found || !enclosed || inner.ID == outer.ID || outer.EnclosingReplyContextID != "" || inner.ValidateEnclosing(outer) != nil {
		t.Fatalf("pre-journal boundary lost its exact two-level reply chain: inner=%+v outer=%+v", inner, outer)
	}
	for _, name := range []string{"t/compute.succeeded", "t/t.replied", "provider/provider.replied", "request.finished"} {
		if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, runID, name); err != nil || count != 0 {
			t.Fatalf("pre-journal boundary already has %s: count=%d error=%v", name, count, err)
		}
	}
	return rows
}

func loadPendingReplyRecoveryOperation(t *testing.T, ctx context.Context, selected any, child string) runfork.ForkOperationRecord {
	t.Helper()
	operation, found, err := selected.(interface {
		LoadForkOperation(context.Context, string, string, string) (runfork.ForkOperationRecord, bool, error)
	}).LoadForkOperation(ctx, "pending-reply-crash", "before-journal", "pending-reply-crash-transport")
	if err != nil || !found || operation.Status != runfork.ForkOperationActivated || operation.Result == nil || operation.ForkRunID != child {
		t.Fatalf("pending-card crash lost activated acknowledgment: %+v found=%t error=%v", operation, found, err)
	}
	return operation
}

func requirePendingReplyRecoveryRefusals(t *testing.T, ctx context.Context, selected any, store pendingReplyExecutionStore, owner SelectedContractExecutionOwner,
	card decisioncard.Card, principal string, previous pendingReplyCrashEvidence, current effects.Authority) {
	t.Helper()
	before, err := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
	if err != nil {
		t.Fatal(err)
	}
	use, isSelected, err := owner.AcquireSelectedDecisionCardControl(ctx, card.RunID, runfork.ControlMailboxDecide)
	if err != nil || !isSelected || use == nil {
		t.Fatalf("pending successor lacks its current decision control: selected=%t error=%v", isSelected, err)
	}
	defer use.Close()
	owned := use.WorkContext()
	grant, present := startupownership.GenerationGrantEvidenceFromContext(owned)
	if !present || grant.SelectedFork == nil || grant.SelectedFork.ExecutionID != current.ID || grant.ProcessAuthorityID == previous.Grant.ProcessAuthorityID {
		t.Fatalf("pending decision did not acquire its fresh process grant: %+v", grant)
	}
	oldBinding := *previous.Grant.SelectedFork
	oldGeneration := grant
	oldGeneration.SelectedFork, oldGeneration.RuntimeGeneration = &oldBinding, oldBinding.ExecutionGeneration
	if err := oldGeneration.Validate(); err != nil {
		t.Fatalf("current-process old-generation negative is not valid evidence: %v", err)
	}
	staleVersion := grant
	staleVersion.StateVersion++
	old := startupownership.WithGenerationGrantEvidence(managedexecution.WithAdmission(effects.WithAuthority(owned, previous.Authority), previous.Admission), previous.Grant)
	currentProcessOldGeneration := startupownership.WithGenerationGrantEvidence(managedexecution.WithAdmission(effects.WithAuthority(owned, previous.Authority), previous.Admission), oldGeneration)
	for _, refused := range []struct {
		name string
		ctx  context.Context
	}{{"SIGKILL predecessor", old}, {"current process old execution generation", currentProcessOldGeneration},
		{"current grant stale state version", startupownership.WithGenerationGrantEvidence(owned, staleVersion)}} {
		_, err := storetest.DecisionCardDomain(selected).ApplyDecisionForTest(refused.ctx, decisioncard.DecideRequest{CardID: card.CardID, Verdict: "approve", PrincipalID: principal,
			ObservedContentHash: card.CardContentHash, Fields: semanticvalue.EmptyObject(), DecisionEventID: uuid.NewString(), Now: time.Now().UTC()})
		if err == nil || !strings.Contains(err.Error(), "run execution generation grant is no longer current") {
			t.Fatalf("%s did not refuse before approval: %v", refused.name, err)
		}
	}
	if err := use.Close(); err != nil {
		t.Fatal(err)
	}
	after, err := store.ListDecisionCardChanges(ctx, decisioncard.SubscriptionOptions{Limit: 100})
	actual, cardErr := store.GetDecisionCard(ctx, card.CardID)
	if err != nil || cardErr != nil || !reflect.DeepEqual(before, after) || !reflect.DeepEqual(card, actual) {
		t.Fatalf("refused old authority changed pending card/history: errors=%v/%v", err, cardErr)
	}
}

func requirePendingReplyRecoveryResult(t *testing.T, ctx context.Context, selected any, store pendingReplyExecutionStore,
	pending decisioncard.ProposedEffectContinuation, decided decisioncard.Card, rows []replycontext.Record) pipeline.ActivityAttemptRecord {
	t.Helper()
	attempts, err := storetest.ReadActivityAttemptStorage(ctx, selected, pending.RunID)
	if err != nil || len(attempts) != 1 {
		t.Fatalf("fresh approval did not produce exactly one actual journal: %+v error=%v", attempts, err)
	}
	journal, found, err := store.LoadActivityAttempt(ctx, attempts[0].RequestEventID)
	if err != nil || !found || journal.Status != pipeline.ActivityAttemptStatusSucceeded || journal.RunID != pending.RunID || journal.ExecutionMode != executionmode.Live ||
		journal.RequestEventID != pending.RequestEventID || journal.ReplyContextID != pending.ReplyContextID || journal.ResultEventID == "" || journal.CompletedAt == nil || journal.Failure != nil {
		t.Fatalf("fresh approval journal borrowed request/return authority: %+v error=%v", journal, err)
	}
	continued, err := store.LoadProposedEffectContinuation(ctx, pending.CardID)
	if err != nil || continued.State != decisioncard.ProposedEffectRequestReleased || continued.Verdict != "approve" || continued.DecisionEventID != decided.DecisionEventID || continued.RouteEventID != decided.DecisionEventID {
		t.Fatalf("fresh approval did not release its own exact request: %+v error=%v", continued, err)
	}
	storage, err := storetest.ReadProposedEffectRunExecutionStorage(ctx, selected, pending.RunID)
	if err != nil || storage.Requests != 1 || storage.SuccessfulAttempts != 1 {
		t.Fatalf("fresh successor request/journal cardinality: %+v error=%v", storage, err)
	}
	result := storetest.LoadCanonicalEventRecord(t, ctx, selected, journal.ResultEventID)
	request := storetest.LoadCanonicalEventRecord(t, ctx, selected, journal.RequestEventID)
	if result.RunID() != pending.RunID || result.Type() != "t/compute.succeeded" || request.RunID() != pending.RunID ||
		request.Type() != "platform.activity_requested" || request.ParentEventID() != decided.DecisionEventID || result.ParentEventID() != request.ParentEventID() {
		t.Fatalf("fresh provider result lost exact child decision cause: request=%+v result=%+v", request, result)
	}
	journalPayload, err := json.Marshal(journal.ResultPayload)
	if err != nil {
		t.Fatal(err)
	}
	if err := pipeline.ValidateRecordedActivityPublication(request, result, journal.ResultEventID, journalPayload); err != nil {
		t.Fatalf("fresh journal disagrees with its actual native publication: %v", err)
	}
	if receipt := storetest.ObservePipelineReceipt(t, ctx, selected, request.ID()); receipt.Count != 1 || receipt.Outcome != "success" {
		t.Fatalf("fresh approved request did not settle exactly once: %+v", receipt)
	}
	for _, row := range rows {
		actual, err := store.LoadReplyContext(ctx, row.ID)
		if err != nil || !actual.SameIdentity(row) || actual.State != replycontext.StateTerminal || actual.TerminalAt == nil {
			t.Fatalf("fresh reply did not settle its own retained identity: %+v error=%v", actual, err)
		}
		returned := storetest.LoadCanonicalEventRecord(t, ctx, selected, actual.AcceptedReplyEventID)
		var payload map[string]string
		if returned.RunID() != pending.RunID || json.Unmarshal(returned.Payload(), &payload) != nil ||
			!reflect.DeepEqual(payload, map[string]string{"account_id": "original-account", "request_id": "original-request", "result": "child-approved-only"}) {
			t.Fatalf("fresh nested reply is not the actual provider output: event=%+v payload=%+v", returned, payload)
		}
	}
	for _, name := range []string{"platform.activity_requested", "t/compute.succeeded", "t/t.replied", "provider/provider.replied", "request.finished"} {
		if count, err := storetest.ReadLifecycleEventCardinality(ctx, selected, pending.RunID, name); err != nil || count != 1 {
			t.Fatalf("fresh successor %s cardinality=%d error=%v", name, count, err)
		}
	}
	page, err := store.ListOperatorEvents(ctx, operatorread.OperatorEventListOptions{Filter: operatorread.OperatorEventListFilter{RunID: pending.RunID, EventName: "request.finished"}, Limit: 2})
	if err != nil || len(page.Events) != 1 || page.NextCursor != "" {
		t.Fatalf("fresh final requester return inventory: %+v error=%v", page, err)
	}
	finished := storetest.LoadCanonicalEventRecord(t, ctx, selected, page.Events[0].EventID)
	var output map[string]string
	var outerReplyID string
	for _, row := range rows {
		if row.EnclosingReplyContextID == "" {
			actual, err := store.LoadReplyContext(ctx, row.ID)
			if err != nil {
				t.Fatal(err)
			}
			outerReplyID = actual.AcceptedReplyEventID
		}
	}
	if finished.ParentEventID() != outerReplyID || finished.RunID() != pending.RunID ||
		json.Unmarshal(finished.Payload(), &output) != nil || !reflect.DeepEqual(output, map[string]string{"request_id": "original-request", "result": "child-approved-only"}) {
		t.Fatalf("fresh final requester output does not match its own provider: %+v", output)
	}
	if receipt := storetest.ObservePipelineReceipt(t, ctx, selected, finished.ID()); receipt.Count != 1 || receipt.Outcome != "success" {
		t.Fatalf("fresh final requester output did not settle its real consumer: %+v", receipt)
	}
	return journal
}
