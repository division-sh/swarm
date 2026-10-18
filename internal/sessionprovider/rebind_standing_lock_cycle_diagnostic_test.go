//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"reflect"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
)

// DIAGNOSTIC ONLY, reviewer-g 6253304321. The invariant is native rebind and
// standing-read lock ordering with genuine session admission and saved proof.
func TestSavedProofRebindStandingLockOrderDiagnosticBothStores(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			f := newActiveInputFixtureWithProof(t, backend, false, true)
			activateLockCyclePredecessor(t, f)
			for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration,
				channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
				var err error
				f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
					OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: phase, Now: time.Now().UTC()})
				if err != nil {
					lockCycleStop(t, "predecessor_completion", err)
				}
			}
			pending, err := f.selected.ListPendingProofResponsibilities(f.ctx)
			if err != nil {
				lockCycleStop(t, "pending_proof_read", err)
			}
			proof, found, err := f.proofs.Get(f.ctx, f.operation.Interface)
			if err != nil {
				lockCycleStop(t, "saved_proof_read", err)
			}
			if len(pending) != 0 || !found || proof.Status != operatorchannel.ProofActive || proof.Revision < 1 {
				t.Fatal("authentic predecessor proof responsibility is not discharged")
			}
			if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || !current {
				lockCycleStop(t, "predecessor_standing_not_current", err)
			}
			logLockCycle(t, map[string]any{"stage": "preparation_ready", "predecessor_standing_current": true,
				"saved_proof_active": true, "unresolved_proofs_empty": true, "activation_proof_matches_binding": true})
			replacement, native := prepareLockCycleReplacement(t, f)
			defer native.CloseExecution()
			if replacement.OperationID == f.operation.OperationID || replacement.SessionConnectionID == f.operation.SessionConnectionID ||
				!replacement.SaveProof || native.RequireExecutable() != nil {
				t.Fatal("replacement did not retain genuine independent session admission")
			}
			now := time.Now().UTC()
			request := operatorchannel.BeginRequest{OperationID: uuid.NewString(), Kind: operatorchannel.OperationRebind,
				PrincipalID: replacement.PrincipalID, Interface: replacement.Interface, ExpectedRevision: f.binding.Revision,
				RequestKeyHash: uuid.NewString(), RequestHash: uuid.NewString(), OnboardingOperationID: replacement.OperationID,
				SaveProof: true, PlannedProofID: proof.ProofID, PlannedProofRevision: proof.Revision + 1,
				ProviderAuthority: native, RequestedAt: now, ExpiresAt: now.Add(operatorchannel.DefaultChallengeTTL)}
			beforeBindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
			if err != nil {
				lockCycleStop(t, "before_binding_read", err)
			}
			beforeOperations, err := f.selected.ListOperatorChannelOperations(f.ctx, f.operation.PrincipalID)
			if err != nil {
				lockCycleStop(t, "before_identity_read", err)
			}
			beforeParent, err := f.selected.GetChannelOnboarding(f.ctx, replacement.OperationID)
			if err != nil {
				lockCycleStop(t, "before_parent_read", err)
			}

			var rebindErr, standingErr error
			var created operatorchannel.Operation
			var standing bool
			var milestones map[string]bool
			scheduleReached := true
			if backend == "postgres" {
				selected, probe := storetest.StartChannelCycleDiagnostic(t, f.location)
				fk, err := probe.PrincipalFKExists(f.ctx)
				if err != nil {
					lockCycleStop(t, "principal_fk_read", err)
				}
				if !fk {
					t.Fatal("selected schema lacks the original nondeferrable principal FK")
				}
				before, err := probe.Snapshot(f.ctx)
				if err != nil {
					lockCycleStop(t, "before_snapshot", err)
				}
				ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
				var workers sync.WaitGroup
				workers.Add(2)
				defer func() { probe.Release(); cancel(); workers.Wait() }()
				go func() {
					defer workers.Done()
					created, rebindErr = selected.BeginChannelBinding(probe.RebindContext(ctx), request)
				}()
				go func() {
					defer workers.Done()
					standing, standingErr = selected.SessionStandingBindingCurrent(probe.StandingContext(ctx), f.operation)
				}()
				for _, acquired := range []<-chan struct{}{probe.BindingAcquired(), probe.PrincipalAcquired()} {
					select {
					case <-acquired:
					case <-ctx.Done():
						scheduleReached = false
					}
				}
				probe.Release()
				workers.Wait()
				cancel()
				milestones = probe.Milestones()
				after, err := probe.Snapshot(f.ctx)
				if err != nil {
					lockCycleStop(t, "after_snapshot", err)
				}
				unchanged := before.UnchangedExceptNewIdentity(after, request.OperationID, rebindErr == nil)
				logLockCycle(t, map[string]any{"stage": "joined", "schedule_reached": scheduleReached, "actual_fk": fk,
					"distinct_parents": true, "saved_proof": true, "milestones": milestones,
					"rebind": lockCycleError(rebindErr), "standing": lockCycleError(standingErr),
					"rebind_committed": rebindErr == nil, "standing_current": standing, "rollback_image_exact": unchanged})
				if !unchanged {
					t.Fatal("native diagnostic changed rows beyond the one allowed committed identity")
				}
				if !scheduleReached {
					t.Fatal("original native schedule was unreachable; retained finite evidence names the boundary")
				}
				if lockCycleIsDeadlock(rebindErr) == lockCycleIsDeadlock(standingErr) ||
					(rebindErr != nil && !lockCycleIsDeadlock(rebindErr)) || (standingErr != nil && !lockCycleIsDeadlock(standingErr)) {
					t.Fatal("native two-owner control did not produce exactly one 40P01 outcome; no retry")
				}
				if !milestones["rebind_insert_operation_started"] || !milestones["standing_binding_query_started"] {
					t.Fatal("original next acquisitions were not reached")
				}
			} else {
				// SQLite owner parity is sequential; do not impose PG's two-writer schedule.
				created, rebindErr = f.selected.BeginChannelBinding(f.ctx, request)
				standing, standingErr = f.selected.SessionStandingBindingCurrent(f.ctx, f.operation)
				logLockCycle(t, map[string]any{"stage": "sqlite_sequential", "saved_proof": true, "distinct_parents": true,
					"rebind": lockCycleError(rebindErr), "standing": lockCycleError(standingErr), "standing_current": standing})
				if rebindErr != nil || standingErr != nil || !standing {
					t.Fatal("sequential SQLite native-owner parity failed")
				}
			}
			afterBindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
			if err != nil {
				lockCycleStop(t, "after_binding_read", err)
			}
			afterParent, err := f.selected.GetChannelOnboarding(f.ctx, replacement.OperationID)
			if err != nil {
				lockCycleStop(t, "after_parent_read", err)
			}
			afterOperations, err := f.selected.ListOperatorChannelOperations(f.ctx, f.operation.PrincipalID)
			if err != nil {
				lockCycleStop(t, "after_identity_read", err)
			}
			if !reflect.DeepEqual(beforeBindings, afterBindings) || !reflect.DeepEqual(beforeParent, afterParent) {
				t.Fatal("diagnostic mutated the exact binding or replacement parent")
			}
			newCount := 0
			for _, op := range afterOperations {
				if op.OperationID == request.OperationID {
					newCount++
					if rebindErr != nil || op.State != operatorchannel.StateAwaitingClaim || op.Kind != operatorchannel.OperationRebind ||
						!op.SaveProof || op.PlannedProofID != request.PlannedProofID || op.PlannedProofRevision != request.PlannedProofRevision ||
						op.OnboardingOperationID != replacement.OperationID || op.ProviderAuthority.Session != replacement.SessionAccount || created.OperationID != op.OperationID {
						t.Fatal("committed identity is not the exact admitted saved-proof rebind")
					}
					continue
				}
				matched := false
				for _, old := range beforeOperations {
					if reflect.DeepEqual(old, op) {
						matched = true
					}
				}
				if !matched {
					t.Fatal("diagnostic changed a pre-existing identity row")
				}
			}
			expected := 0
			if rebindErr == nil {
				expected = 1
			}
			if newCount != expected || len(afterOperations) != len(beforeOperations)+expected {
				t.Fatal("rolled-back identity is present or an existing identity is missing")
			}
			logLockCycle(t, map[string]any{"stage": "final_readback", "binding_exact": true, "parent_exact": true,
				"existing_identities_exact": true, "new_identity_count": newCount, "workers_joined": true})
		})
	}
}

// Diagnostic-only counterpart of activeInputFixture.activate. The initial
// native publication carries its genuinely confirmed proof (6253601074).
func activateLockCyclePredecessor(t *testing.T, f *activeInputFixture) {
	t.Helper()
	identity := f.confirm(t)
	ctx, now := f.ctx, time.Now().UTC().Truncate(time.Microsecond)
	if !f.operation.SaveProof || f.binding.ProofID == "" || f.binding.ProofRevision < 1 {
		t.Fatal("native confirmation did not produce the required saved proof")
	}
	var err error
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseAwaitingOperatorConfirmation, channelonboarding.PhasePublishingActivation} {
		f.operation, err = f.selected.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: f.operation.OperationID,
			ExpectedRevision: f.operation.Revision, Phase: phase, IdentityOperationID: identity.OperationID, BindingRevision: f.binding.Revision, Now: now})
		if err != nil {
			lockCycleStop(t, "predecessor_activation_checkpoint", err)
		}
	}
	f.operation, f.activation, err = f.selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, ActivationID: uuid.NewString(),
		BindingRevision: f.binding.Revision, ConversationRef: f.binding.ConversationRef,
		ProofID: f.binding.ProofID, ProofRevision: f.binding.ProofRevision, Now: now})
	if err != nil {
		lockCycleStop(t, "predecessor_activation_publication", err)
	}
	if f.activation.ProofID != f.binding.ProofID || f.activation.ProofRevision != f.binding.ProofRevision {
		t.Fatal("original native activation publication lost exact confirmed proof")
	}
	source := sourceartifactfixture.Require(t, ctx, f.selected)
	f.standing, err = f.selected.ReconcileStandingService(ctx, pipeline.StandingServiceCandidate{
		BindingEnabled: f.activation.Status == channelonboarding.ActivationCurrent && f.binding.Status == operatorchannel.BindingCurrent,
		ServiceID:      flowidentity.StandingServiceID("."), FlowPath: ".", Source: source})
	if err != nil {
		lockCycleStop(t, "predecessor_standing_reconciliation", err)
	}
	f.standing.PublicationSequence, err = f.selected.PublishStandingService(ctx, f.standing.ServiceID, f.standing.RunID, f.standing.Generation)
	if err != nil {
		lockCycleStop(t, "predecessor_standing_publication", err)
	}
	f.setScope(channelonboarding.SessionInputBusiness)
}

func prepareLockCycleReplacement(t *testing.T, f *activeInputFixture) (channelonboarding.Operation, operatorchannel.ProviderAuthority) {
	t.Helper()
	now, id := time.Now().UTC(), uuid.NewString()
	op, err := f.selected.ReserveChannelOnboarding(f.ctx, channelonboarding.StartRequest{OperationID: id,
		RequestKeyHash: id, RequestHash: id, PrincipalID: f.operation.PrincipalID, Verb: channelonboarding.VerbRebind,
		Provider: f.operation.Provider, Interface: f.operation.Interface, Coordinate: f.operation.Coordinate,
		TargetSelector: f.operation.TargetSelector, Posture: channelonboarding.ActivationSessionConnection,
		Ceremony: f.operation.Ceremony, SaveProof: true, RequestedAt: now})
	if err != nil {
		lockCycleStop(t, "replacement_reservation", err)
	}
	state := sessionStateFixture(t, f.basePath, op.SessionConnectionID, "")
	device := newSDKDeviceFixture(t, state.database)
	peer := newSDKPeer(t)
	occurrence, err := state.newOccurrence(f.ctx, uuid.NewString())
	if err != nil {
		lockCycleStop(t, "replacement_occurrence", err)
	}
	peer.attach(t, occurrence.client)
	if err := occurrence.connect(occurrence.ctx); err != nil || !occurrence.client.WaitForConnection(5*time.Second) {
		lockCycleStop(t, "replacement_sdk_connection", err)
	}
	account := operatorchannel.SessionAccountAdmission{Provider: op.Provider, ConnectionID: op.SessionConnectionID,
		AccountRef: device.ID.ToNonAD().String(), AdmissionID: uuid.NewString(), Revision: 1}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhaseCredentialsAdmitted, channelonboarding.PhaseActivatingProvider, channelonboarding.PhaseAwaitingExternalIdentity} {
		req := channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now}
		if phase == channelonboarding.PhaseAwaitingExternalIdentity {
			req.SessionAccount = &account
		}
		op, err = f.selected.AdvanceChannelOnboarding(f.ctx, req)
		if err != nil {
			lockCycleStop(t, "replacement_account_checkpoint", err)
		}
	}
	owner, err := newSessionAuthorityOwner(state, f.selected, op, nil)
	if err != nil {
		lockCycleStop(t, "replacement_native_owner", err)
	}
	structural := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}
	native, current, err := structural.AdmitExecution(f.ctx, owner)
	if err != nil || !current {
		native.CloseExecution()
		lockCycleStop(t, "replacement_native_admission", err)
	}
	return op, native
}

func logLockCycle(t *testing.T, evidence map[string]any) {
	t.Helper()
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal("finite diagnostic evidence encoding failed")
	}
	t.Log("NATIVE_LOCK_CYCLE " + string(raw))
}
func lockCycleStop(t *testing.T, stage string, err error) {
	t.Helper()
	logLockCycle(t, map[string]any{"stage": stage, "cause": lockCycleError(err), "schedule_reached": false})
	t.Fatal("native diagnostic stopped at the recorded authentic-preparation boundary")
}
func lockCycleIsDeadlock(err error) bool {
	return lockCycleError(err)["deadlock"] == true
}
func lockCycleError(err error) map[string]any {
	nodes := []map[string]any{}
	deadlock, truncated := false, false
	var visit func(error, int)
	visit = func(e error, depth int) {
		if e == nil {
			return
		}
		if len(nodes) >= 32 || depth > 8 {
			truncated = true
			return
		}
		node := map[string]any{"type": reflect.TypeOf(e).String()}
		if pg, ok := e.(*pq.Error); ok && pg != nil {
			state := string(pg.Code)
			valid := len(state) == 5
			for _, c := range state {
				if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
					valid = false
				}
			}
			if valid {
				node["sqlstate"] = state
			}
			if pg.Code == "40P01" {
				deadlock = true
				node["cycle"] = onboardingDeadlockMetadata(pg.Detail)
			}
		}
		nodes = append(nodes, node)
		if joined, ok := e.(interface{ Unwrap() []error }); ok {
			for _, child := range joined.Unwrap() {
				if len(nodes) >= 32 {
					truncated = true
					break
				}
				visit(child, depth+1)
			}
		} else if wrapped, ok := e.(interface{ Unwrap() error }); ok {
			visit(wrapped.Unwrap(), depth+1)
		}
	}
	visit(err, 0)
	return map[string]any{"present": err != nil, "deadlock": deadlock, "truncated": truncated, "nodes": nodes}
}
