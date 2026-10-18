//go:build linux || darwin

package sessionprovider

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"reflect"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiidempotency"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	flowidentity "github.com/division-sh/swarm/internal/runtime/core/flowidentity"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	"github.com/division-sh/swarm/internal/runtime/pipeline"
	"github.com/division-sh/swarm/internal/runtime/registration"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"github.com/division-sh/swarm/internal/store/storetest"
	"github.com/division-sh/swarm/internal/testutil/sourceartifactfixture"
	"github.com/google/uuid"
	"github.com/lib/pq"
	"go.mau.fi/whatsmeow/proto/waE2E"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

// Regression for both native parent/principal/binding lock-order cycles.
func TestSessionChannelMutationsShareLockOrderBothStores(t *testing.T) {
	for _, backend := range []string{"postgres", "sqlite"} {
		t.Run(backend, func(t *testing.T) {
			t.Run("quoted_ack", func(t *testing.T) { runQuotedACKLockOrder(t, backend) })
			t.Run("rebind", func(t *testing.T) {
				f := newActiveInputFixtureWithProof(t, backend, false, true)
				activateLockOrderPredecessor(t, f)
				for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration,
					channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
					var err error
					f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
						OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: phase, Now: time.Now().UTC()})
					if err != nil {
						lockOrderStop(t, "predecessor_completion", err)
					}
				}
				pending, err := f.selected.ListPendingProofResponsibilities(f.ctx)
				if err != nil {
					lockOrderStop(t, "pending_proof_read", err)
				}
				proof, found, err := f.proofs.Get(f.ctx, f.operation.Interface)
				if err != nil {
					lockOrderStop(t, "saved_proof_read", err)
				}
				if len(pending) != 0 || !found || proof.Status != operatorchannel.ProofActive || proof.Revision < 1 {
					t.Fatal("authentic predecessor proof responsibility is not discharged")
				}
				if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || !current {
					lockOrderStop(t, "predecessor_standing_not_current", err)
				}
				logLockOrder(t, map[string]any{"stage": "preparation_ready", "predecessor_standing_current": true,
					"saved_proof_active": true, "unresolved_proofs_empty": true, "activation_proof_matches_binding": true})
				replacement, native := prepareLockOrderReplacement(t, f)
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
					lockOrderStop(t, "before_binding_read", err)
				}
				beforeOperations, err := f.selected.ListOperatorChannelOperations(f.ctx, f.operation.PrincipalID)
				if err != nil {
					lockOrderStop(t, "before_identity_read", err)
				}
				beforeParent, err := f.selected.GetChannelOnboarding(f.ctx, replacement.OperationID)
				if err != nil {
					lockOrderStop(t, "before_parent_read", err)
				}

				var rebindErr, standingErr error
				var created operatorchannel.Operation
				var standing bool
				var milestones map[string]bool
				scheduleReached := true
				if backend == "postgres" {
					selected, probe := storetest.StartChannelLockOrder(t, f.location)
					fk, err := probe.PrincipalFKExists(f.ctx)
					if err != nil {
						lockOrderStop(t, "principal_fk_read", err)
					}
					if !fk {
						t.Fatal("selected schema lacks the original nondeferrable principal FK")
					}
					before, err := probe.Snapshot(f.ctx)
					if err != nil {
						lockOrderStop(t, "before_snapshot", err)
					}
					ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
					var workers sync.WaitGroup
					workers.Add(2)
					defer func() { probe.Release(); cancel(); workers.Wait() }()
					go func() {
						defer workers.Done()
						select {
						case <-probe.PrincipalAcquired():
						case <-ctx.Done():
							rebindErr = ctx.Err()
							return
						}
						created, rebindErr = selected.BeginChannelBinding(probe.RebindContext(ctx), request)
					}()
					go func() {
						defer workers.Done()
						standing, standingErr = selected.SessionStandingBindingCurrent(probe.StandingContext(ctx), f.operation)
					}()
					for _, acquired := range []<-chan struct{}{probe.PrincipalAcquired(), probe.RebindReady()} {
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
						lockOrderStop(t, "after_snapshot", err)
					}
					unchanged := before.UnchangedExceptNewIdentity(after, request.OperationID, rebindErr == nil)
					logLockOrder(t, map[string]any{"stage": "joined", "schedule_reached": scheduleReached, "actual_fk": fk,
						"distinct_parents": true, "saved_proof": true, "milestones": milestones,
						"rebind": lockOrderError(rebindErr), "standing": lockOrderError(standingErr),
						"rebind_committed": rebindErr == nil, "standing_current": standing, "rollback_image_exact": unchanged})
					if !unchanged {
						t.Fatal("native diagnostic changed rows beyond the one allowed committed identity")
					}
					if !scheduleReached {
						t.Fatal("original native schedule was unreachable; retained finite evidence names the boundary")
					}
					if rebindErr != nil || standingErr != nil || !standing {
						t.Fatal("rebind and standing must both finish successfully without 40P01")
					}
					if !milestones["rebind_insert_operation_started"] || !milestones["standing_binding_query_started"] {
						t.Fatal("original next acquisitions were not reached")
					}
				} else {
					// SQLite owner parity is sequential; do not impose PG's two-writer schedule.
					created, rebindErr = f.selected.BeginChannelBinding(f.ctx, request)
					standing, standingErr = f.selected.SessionStandingBindingCurrent(f.ctx, f.operation)
					logLockOrder(t, map[string]any{"stage": "sqlite_sequential", "saved_proof": true, "distinct_parents": true,
						"rebind": lockOrderError(rebindErr), "standing": lockOrderError(standingErr), "standing_current": standing})
					if rebindErr != nil || standingErr != nil || !standing {
						t.Fatal("sequential SQLite native-owner parity failed")
					}
				}
				afterBindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
				if err != nil {
					lockOrderStop(t, "after_binding_read", err)
				}
				afterParent, err := f.selected.GetChannelOnboarding(f.ctx, replacement.OperationID)
				if err != nil {
					lockOrderStop(t, "after_parent_read", err)
				}
				afterOperations, err := f.selected.ListOperatorChannelOperations(f.ctx, f.operation.PrincipalID)
				if err != nil {
					lockOrderStop(t, "after_identity_read", err)
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
				logLockOrder(t, map[string]any{"stage": "final_readback", "binding_exact": true, "parent_exact": true,
					"existing_identities_exact": true, "new_identity_count": newCount, "workers_joined": true})
				proveLockOrderBeginAdmission(t, f, request, created)
				if backend == "postgres" {
					proveLockOrderFenceCancellation(t, f, request)
				}
			})
		})
	}
}

func proveLockOrderBeginAdmission(t *testing.T, f *activeInputFixture, request operatorchannel.BeginRequest, created operatorchannel.Operation) {
	t.Helper()
	before, err := f.selected.ListOperatorChannelOperations(f.ctx, request.PrincipalID)
	if err != nil {
		lockOrderStop(t, "begin_before_admission_controls", err)
	}
	var durable operatorchannel.Operation
	for _, operation := range before {
		if operation.OperationID == created.OperationID {
			durable = operation
		}
	}
	if durable.OperationID == "" {
		t.Fatal("created identity has no exact durable row")
	}
	replay := request
	replay.OperationID = uuid.NewString()
	repeated, err := f.selected.BeginChannelBinding(f.ctx, replay)
	if err != nil || !reflect.DeepEqual(repeated, durable) {
		lockOrderStop(t, "begin_exact_replay", err)
	}
	conflicting := request
	conflicting.RequestHash = uuid.NewString()
	if _, err := f.selected.BeginChannelBinding(f.ctx, conflicting); !errors.Is(err, operatorchannel.ErrConflict) {
		lockOrderStop(t, "begin_conflicting_replay", err)
	}
	stale := request
	stale.OperationID, stale.RequestKeyHash = uuid.NewString(), uuid.NewString()
	stale.ExpectedRevision++
	if _, err := f.selected.BeginChannelBinding(f.ctx, stale); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
		lockOrderStop(t, "begin_stale_binding", err)
	}
	old := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: f.operation.SessionAccount}
	foreign, current, err := old.AdmitExecution(f.ctx, f.authority)
	if err != nil || !current {
		foreign.CloseExecution()
		lockOrderStop(t, "begin_foreign_native_owner", err)
	}
	defer foreign.CloseExecution()
	stale = request
	stale.OperationID, stale.RequestKeyHash = uuid.NewString(), uuid.NewString()
	stale.ProviderAuthority = foreign
	if _, err := f.selected.BeginChannelBinding(f.ctx, stale); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
		lockOrderStop(t, "begin_foreign_parent_refused", err)
	}
	after, err := f.selected.ListOperatorChannelOperations(f.ctx, request.PrincipalID)
	if err != nil || !reflect.DeepEqual(before, after) {
		lockOrderStop(t, "begin_admission_controls_exact_image", err)
	}
}

func proveLockOrderFenceCancellation(t *testing.T, f *activeInputFixture, request operatorchannel.BeginRequest) {
	t.Helper()
	selected, probe := storetest.StartChannelLockOrder(t, f.location)
	before, err := probe.Snapshot(f.ctx)
	if err != nil {
		lockOrderStop(t, "cancel_before_snapshot", err)
	}
	ctx, stop := context.WithTimeout(f.ctx, 5*time.Second)
	defer stop()
	beginCtx, cancel := context.WithCancel(ctx)
	defer cancel()
	var workers sync.WaitGroup
	defer func() { probe.Release(); stop(); workers.Wait() }()
	var current bool
	var standingErr, beginErr error
	standingDone, beginDone := make(chan struct{}), make(chan struct{})
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(standingDone)
		current, standingErr = selected.SessionStandingBindingCurrent(probe.StandingContext(ctx), f.operation)
	}()
	select {
	case <-probe.PrincipalAcquired():
	case <-ctx.Done():
		t.Fatal("cancellation control did not acquire the original principal fence")
	}
	request.OperationID, request.RequestKeyHash = uuid.NewString(), uuid.NewString()
	workers.Add(1)
	go func() {
		defer workers.Done()
		defer close(beginDone)
		_, beginErr = selected.BeginChannelBinding(probe.RebindContext(beginCtx), request)
	}()
	select {
	case <-probe.RebindReady():
	case <-ctx.Done():
		t.Fatal("cancellation control missed the original fence attempt")
	}
	cancel()
	// The canonical write owner drains SQL and checks cancellation before
	// COMMIT. Release the competing owner so that drain and rollback can join.
	probe.Release()
	select {
	case <-beginDone:
	case <-ctx.Done():
		t.Fatal("cancelled begin did not join after releasing the other owner")
	}
	workers.Wait()
	if !errors.Is(beginErr, context.Canceled) || standingErr != nil || !current {
		logLockOrder(t, map[string]any{"stage": "fence_cancel_joined", "begin": lockOrderError(beginErr), "standing": lockOrderError(standingErr), "standing_current": current})
		t.Fatal("cancelled fence admission did not roll back while preserving standing currentness")
	}
	after, err := probe.Snapshot(f.ctx)
	if err != nil || !before.UnchangedExceptNewIdentity(after, request.OperationID, false) {
		lockOrderStop(t, "cancel_exact_rollback", err)
	}
}

// Same observer, second native pair, reviewer-g 6253826975. No production locks
// are synthesized: each barrier follows an original successful row acquisition.
func runQuotedACKLockOrder(t *testing.T, backend string) {
	f := newActiveInputFixtureWithProof(t, backend, true, true)
	activateLockOrderPredecessor(t, f)
	device, err := f.state.device(f.ctx)
	if err != nil {
		lockOrderStop(t, "ack_native_device_read", err)
	}
	if err := f.sender.Sessions.MigratePNToLID(f.ctx, device.ID.ToNonAD(), device.LID); err != nil {
		lockOrderStop(t, "ack_sender_session_migration", err)
	}
	to, err := types.ParseJID(f.binding.ConversationRef)
	if err != nil {
		lockOrderStop(t, "ack_confirmed_recipient", err)
	}
	f.peer.admitMessageRecipient(to, 1)
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration,
		channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation, channelonboarding.PhaseSucceeded} {
		var err error
		f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
			OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: phase, Now: time.Now().UTC()})
		if err != nil {
			lockOrderStop(t, "ack_predecessor_completion", err)
		}
	}
	if current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation); err != nil || !current {
		lockOrderStop(t, "ack_predecessor_standing", err)
	}
	pending, noticeID, request := prepareLockOrderQuotedACK(t, f)
	if backend == "sqlite" {
		beforeBindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
		if err != nil {
			lockOrderStop(t, "sqlite_ack_before_bindings", err)
		}
		beforeParent, err := f.selected.GetChannelOnboarding(f.ctx, f.operation.OperationID)
		if err != nil {
			lockOrderStop(t, "sqlite_ack_before_parent", err)
		}
		completion, replayed, err := f.selected.(render.Store).AcknowledgeChannelNotice(f.ctx, request, pending.Fact)
		if err != nil || replayed || completion.ResourceID != noticeID {
			lockOrderStop(t, "sqlite_ack_completion", err)
		}
		current, err := f.selected.SessionStandingBindingCurrent(f.ctx, f.operation)
		if err != nil || !current {
			lockOrderStop(t, "sqlite_ack_standing", err)
		}
		afterBindings, err := f.selected.ListOperatorChannelBindings(f.ctx, f.operation.PrincipalID)
		if err != nil {
			lockOrderStop(t, "sqlite_ack_after_bindings", err)
		}
		afterParent, err := f.selected.GetChannelOnboarding(f.ctx, f.operation.OperationID)
		if err != nil {
			lockOrderStop(t, "sqlite_ack_after_parent", err)
		}
		if !reflect.DeepEqual(beforeBindings, afterBindings) || !reflect.DeepEqual(beforeParent, afterParent) {
			t.Fatal("sequential SQLite ACK changed the exact parent or binding")
		}
		proveLockOrderACKReplay(t, f, pending, request, completion)
		return
	}
	selected, probe := storetest.StartChannelLockOrder(t, f.location)
	before, err := probe.ACKSnapshot(f.ctx)
	if err != nil {
		lockOrderStop(t, "ack_before_snapshot", err)
	}
	ctx, cancel := context.WithTimeout(f.ctx, 5*time.Second)
	var workers sync.WaitGroup
	workers.Add(2)
	defer func() { probe.Release(); cancel(); workers.Wait() }()
	var standing bool
	var standingErr, ackErr error
	var completion apiidempotency.Completion
	var replayed bool
	go func() {
		defer workers.Done()
		standing, standingErr = selected.SessionStandingBindingCurrent(probe.ACKStandingContext(ctx), f.operation)
	}()
	go func() {
		defer workers.Done()
		select {
		case <-probe.ParentAcquired():
		case <-ctx.Done():
			ackErr = ctx.Err()
			return
		}
		completion, replayed, ackErr = selected.AcknowledgeChannelNotice(probe.ACKContext(ctx), request, pending.Fact)
	}()
	scheduleReached := true
	for _, acquired := range []<-chan struct{}{probe.ParentAcquired(), probe.ACKReady()} {
		select {
		case <-acquired:
		case <-ctx.Done():
			scheduleReached = false
		}
	}
	probe.Release()
	workers.Wait()
	cancel()
	after, err := probe.ACKSnapshot(f.ctx)
	if err != nil {
		lockOrderStop(t, "ack_after_snapshot", err)
	}
	exact := before.ExactExceptACK(after, noticeID, pending.PublicationID, ackErr == nil)
	completionExact, err := probe.ACKCompletionExact(f.ctx, request, ackErr == nil)
	if err != nil {
		lockOrderStop(t, "ack_completion_readback", err)
	}
	logLockOrder(t, map[string]any{"pair": "standing_quoted_ack", "stage": "joined", "schedule_reached": scheduleReached,
		"genuine_current_session": true, "native_sent_receipt": true, "native_quoted_reply": true,
		"milestones": probe.Milestones(), "standing": lockOrderError(standingErr), "standing_current": standing,
		"ack": lockOrderError(ackErr), "ack_committed": ackErr == nil, "ack_replayed": replayed,
		"rollback_image_exact": exact, "completion_exact": completionExact, "workers_joined": true})
	if !exact || !completionExact {
		t.Fatal("native ACK pair changed durable rows beyond the exact acknowledged notice/action/completion")
	}
	if !scheduleReached {
		t.Fatal("second native schedule was unreachable; no retry")
	}
	if ackErr != nil || standingErr != nil || !standing {
		t.Fatal("quoted ACK and standing must both finish successfully without 40P01")
	}
	if ackErr == nil && (replayed || completion.ResourceID != noticeID) || standingErr == nil && !standing {
		t.Fatal("successful native owner returned an inexact currentness or completion result")
	}
	milestones := probe.Milestones()
	if !milestones["standing_principal_fence_started"] || !milestones["ack_action_join_started"] {
		t.Fatal("second pair missed an original next acquisition")
	}
	proveLockOrderACKReplay(t, f, pending, request, completion)
}

func proveLockOrderACKReplay(t *testing.T, f *activeInputFixture, pending render.PendingAction, request apiidempotency.Request, completion apiidempotency.Completion) {
	t.Helper()
	store := f.selected.(render.Store)
	item, err := f.selected.(runtimetools.MailboxPersistence).GetMailboxItem(f.ctx, request.ResourceID)
	if err != nil || !item.Notified {
		lockOrderStop(t, "ack_exact_notice_readback", err)
	}
	repeated, replayed, err := store.AcknowledgeChannelNotice(f.ctx, request, pending.Fact)
	if err != nil || !replayed || !reflect.DeepEqual(repeated, completion) {
		lockOrderStop(t, "ack_exact_completion_replay", err)
	}
	conflict := request
	conflict.RequestHash = uuid.NewString()
	if _, _, err := store.AcknowledgeChannelNotice(f.ctx, conflict, pending.Fact); err == nil {
		t.Fatal("conflicting ACK replay was accepted")
	}
	after, err := f.selected.(runtimetools.MailboxPersistence).GetMailboxItem(f.ctx, request.ResourceID)
	if err != nil || !reflect.DeepEqual(after, item) {
		lockOrderStop(t, "ack_replay_exact_image", err)
	}
}

func prepareLockOrderQuotedACK(t *testing.T, f *activeInputFixture) (render.PendingAction, string, apiidempotency.Request) {
	t.Helper()
	store := f.selected.(render.Store)
	noticeID, err := f.selected.(runtimetools.MailboxPersistence).InsertMailboxItem(f.ctx, runtimetools.MailboxItem{
		Type: runtimetools.NotifyHumanMailboxItemType, Summary: "Lock order notice", Context: []byte(`{"message":"Native lock order notice."}`)})
	if err != nil {
		lockOrderStop(t, "ack_notice_creation", err)
	}
	plans, err := store.ListCurrentChannelDeliveryPlans(f.ctx, "", 100)
	if err != nil {
		lockOrderStop(t, "ack_notice_plan", err)
	}
	var candidate render.Candidate
	for _, plan := range plans {
		if plan.SourceID == noticeID {
			if candidate.DeliveryID != "" {
				t.Fatal("native notice plan is duplicated")
			}
			candidate = plan
		}
	}
	prepared, err := store.FreezeAndPersistChannelRender(f.ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 2000, LabelRunes: 64})
	if err != nil {
		lockOrderStop(t, "ack_notice_render", err)
	}
	text, err := render.TextReplyPresentation(prepared.Frozen, prepared.Actions)
	if err != nil {
		lockOrderStop(t, "ack_notice_presentation", err)
	}
	input, err := f.channel.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": text}}, map[string]any{"destination": f.binding.ConversationRef})
	if err != nil {
		lockOrderStop(t, "ack_notice_input", err)
	}
	opID, err := runtimeeffects.ChannelDeliveryOperationID(candidate.DeliveryID, prepared.RenderID)
	if err != nil {
		lockOrderStop(t, "ack_notice_effect_identity", err)
	}
	c, audience := f.activation.Coordinate, prepared.Frozen.Audience
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelDelivery, ID: opID,
		ExecutionOwner: "native-delivery:" + candidate.DeliveryID, LeaseExpiresAt: time.Now().Add(time.Minute),
		FenceGeneration: c.ContextPublicationGeneration, ExecutionMode: runtimeeffects.ExecutionModeLive,
		ChannelDelivery: runtimeeffects.ChannelDeliveryAuthority{
			EffectOperationID: opID, DeliveryID: candidate.DeliveryID, RenderID: prepared.RenderID, RenderHash: prepared.Frozen.Hash,
			PrincipalID: audience.PrincipalID, InterfaceKey: audience.InterfaceKey, DeliveryEpoch: audience.DeliveryEpoch,
			BindingRevision: candidate.BindingRevision, ExternalAccountRef: audience.ExternalAccountRef, ConversationRef: audience.ConversationRef,
			ActivationID: f.activation.ActivationID, ActivationRevision: f.activation.Revision,
			BundleHash: c.BundleHash, BundleIdentity: c.BundleIdentity, PackInventoryGeneration: c.PackInventoryGeneration,
			RuntimeInstanceID: c.RuntimeInstanceID, ContextPublicationGeneration: c.ContextPublicationGeneration, PlanGeneration: c.PlanGeneration, TargetGeneration: c.TargetGeneration}}
	ctx := runtimeeffects.WithExecutionMode(f.ctx, runtimeeffects.ExecutionModeLive)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(f.selected.(runtimeeffects.Store)).WithExecutionPosture(executionposture.Live))
	ctx = runtimeeffects.WithAuthority(ctx, authority)
	executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
	proveNativeJournalWrite(t, f, ctx, text, true, func() (registration.DeliveryResult, error) {
		return executor.DeliverChannelMessage(ctx, "deliver", input, nil)
	})
	receipt, found, err := store.GetCurrentChannelSentReceipt(f.ctx, candidate.DeliveryID, opID)
	if err != nil || !found || receipt.RenderID != prepared.RenderID || receipt.DeliveryReference != strings.ReplaceAll(opID, "-", "") {
		lockOrderStop(t, "ack_native_sent_receipt", err)
	}
	var control render.Action
	for _, action := range prepared.Actions {
		if action.Kind == "acknowledge_notice" {
			if control.Token != "" {
				t.Fatal("native notice has duplicate ACK controls")
			}
			control = action
		}
	}
	if control.Token == "" {
		t.Fatal("native notice has no ACK control")
	}
	bus := f.publicationBus(t)
	event := f.receiveCaptureMessageSDK(t, &waE2E.Message{ExtendedTextMessage: &waE2E.ExtendedTextMessage{
		Text: proto.String(control.Label), ContextInfo: &waE2E.ContextInfo{StanzaID: proto.String(receipt.DeliveryReference.(string)),
			QuotedMessage: &waE2E.Message{Conversation: proto.String(text)}}}}, uuid.NewString(), time.Now())
	admitted, err := f.owner.admit(f.ctx, SessionInputReference{ConnectionID: event.Scope.Session.ConnectionID,
		OccurrenceID: event.OccurrenceID, Conversation: event.Conversation, EventID: event.EventID, Kind: event.Kind})
	if err != nil {
		lockOrderStop(t, "ack_native_reply_admission", err)
	}
	defer admitted.Close()
	publication, err := prepareSessionBusinessPublication(f.ctx, admitted, f.trigger, "whatsapp", bus,
		f.selected.(sessionBusinessStore), executionposture.Live, f.channel)
	if err != nil {
		lockOrderStop(t, "ack_native_reply_preparation", err)
	}
	result, err := publication.commitAndDispatch()
	if err != nil || !result.Acknowledged {
		lockOrderStop(t, "ack_native_reply_commit", err)
	}
	texts, err := store.ListPendingChannelTexts(f.ctx, "", 100)
	if err != nil || len(texts) != 1 || texts[0].Fact.Text != control.Label || texts[0].Fact.ReplyToReference != receipt.DeliveryReference {
		lockOrderStop(t, "ack_native_reply_readback", err)
	}
	pending, found, err := store.AdmitChannelReplyAction(f.ctx, texts[0].Fact)
	if err != nil || !found || pending.Fact.Kind != operatorchannel.ActionSourceReply || pending.Fact.Token != control.Token {
		lockOrderStop(t, "ack_native_action_transfer", err)
	}
	resolved, found, err := store.ResolveChannelActionFact(f.ctx, pending.Fact.ActionFact)
	if err != nil || !found || !resolved.CurrentRender || resolved.SourceID != noticeID || resolved.Action.Kind != "acknowledge_notice" {
		lockOrderStop(t, "ack_native_action_currentness", err)
	}
	request := apiidempotency.Request{Method: "mailbox.acknowledge", Actor: apiidempotency.PrincipalActor(resolved.PrincipalID),
		IdempotencyKey: pending.PublicationID, ResourceID: resolved.SourceID,
		RequestHash: operatorchannel.Hash("channel-notice-ack-v1", pending.PublicationID, pending.Fact.ProviderAuthorization,
			pending.Fact.Interface.Key(), pending.Fact.Token, resolved.RenderHash, resolved.SourceID),
		TTL: 24 * time.Hour, Now: pending.ReceivedAt.UTC()}
	return pending, noticeID, request
}

// Saved-proof counterpart of activeInputFixture.activate. The initial
// native publication carries its genuinely confirmed proof (6253601074).
func activateLockOrderPredecessor(t *testing.T, f *activeInputFixture) {
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
			lockOrderStop(t, "predecessor_activation_checkpoint", err)
		}
	}
	f.operation, f.activation, err = f.selected.PublishConnectedChannelActivation(ctx, channelonboarding.PublishActivationRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, ActivationID: uuid.NewString(),
		BindingRevision: f.binding.Revision, ConversationRef: f.binding.ConversationRef,
		ProofID: f.binding.ProofID, ProofRevision: f.binding.ProofRevision, Now: now})
	if err != nil {
		lockOrderStop(t, "predecessor_activation_publication", err)
	}
	if f.activation.ProofID != f.binding.ProofID || f.activation.ProofRevision != f.binding.ProofRevision {
		t.Fatal("original native activation publication lost exact confirmed proof")
	}
	source := sourceartifactfixture.Require(t, ctx, f.selected)
	f.standing, err = f.selected.ReconcileStandingService(ctx, pipeline.StandingServiceCandidate{
		BindingEnabled: f.activation.Status == channelonboarding.ActivationCurrent && f.binding.Status == operatorchannel.BindingCurrent,
		ServiceID:      flowidentity.StandingServiceID("."), FlowPath: ".", Source: source})
	if err != nil {
		lockOrderStop(t, "predecessor_standing_reconciliation", err)
	}
	f.standing.PublicationSequence, err = f.selected.PublishStandingService(ctx, f.standing.ServiceID, f.standing.RunID, f.standing.Generation)
	if err != nil {
		lockOrderStop(t, "predecessor_standing_publication", err)
	}
	f.setScope(channelonboarding.SessionInputBusiness)
}

func prepareLockOrderReplacement(t *testing.T, f *activeInputFixture) (channelonboarding.Operation, operatorchannel.ProviderAuthority) {
	t.Helper()
	now, id := time.Now().UTC(), uuid.NewString()
	op, err := f.selected.ReserveChannelOnboarding(f.ctx, channelonboarding.StartRequest{OperationID: id,
		RequestKeyHash: id, RequestHash: id, PrincipalID: f.operation.PrincipalID, Verb: channelonboarding.VerbRebind,
		Provider: f.operation.Provider, Interface: f.operation.Interface, Coordinate: f.operation.Coordinate,
		TargetSelector: f.operation.TargetSelector, Posture: channelonboarding.ActivationSessionConnection,
		Ceremony: f.operation.Ceremony, SaveProof: true, RequestedAt: now})
	if err != nil {
		lockOrderStop(t, "replacement_reservation", err)
	}
	state := sessionStateFixture(t, f.basePath, op.SessionConnectionID, "")
	device := newSDKDeviceFixture(t, state.database)
	peer := newSDKPeer(t)
	occurrence, err := state.newOccurrence(f.ctx, uuid.NewString())
	if err != nil {
		lockOrderStop(t, "replacement_occurrence", err)
	}
	peer.attach(t, occurrence.client)
	if err := occurrence.connect(occurrence.ctx); err != nil || !occurrence.client.WaitForConnection(5*time.Second) {
		lockOrderStop(t, "replacement_sdk_connection", err)
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
			lockOrderStop(t, "replacement_account_checkpoint", err)
		}
	}
	owner, err := newSessionAuthorityOwner(state, f.selected, op, nil)
	if err != nil {
		lockOrderStop(t, "replacement_native_owner", err)
	}
	structural := operatorchannel.ProviderAuthority{Kind: operatorchannel.ProviderAuthoritySession, Session: account}
	native, current, err := structural.AdmitExecution(f.ctx, owner)
	if err != nil || !current {
		native.CloseExecution()
		lockOrderStop(t, "replacement_native_admission", err)
	}
	return op, native
}

func logLockOrder(t *testing.T, evidence map[string]any) {
	t.Helper()
	raw, err := json.Marshal(evidence)
	if err != nil {
		t.Fatal("finite diagnostic evidence encoding failed")
	}
	t.Log("CHANNEL_LOCK_ORDER " + string(raw))
}
func lockOrderStop(t *testing.T, stage string, err error) {
	t.Helper()
	logLockOrder(t, map[string]any{"stage": stage, "cause": lockOrderError(err), "schedule_reached": false})
	t.Fatal("native diagnostic stopped at the recorded authentic-preparation boundary")
}
func lockOrderError(err error) map[string]any {
	out := map[string]any{"present": err != nil}
	if err == nil {
		return out
	}
	out["type"] = fmt.Sprintf("%T", err)
	var pg *pq.Error
	if errors.As(err, &pg) && pg != nil {
		state := string(pg.Code)
		valid := len(state) == 5
		for _, c := range state {
			if !(c >= 'A' && c <= 'Z' || c >= '0' && c <= '9') {
				valid = false
			}
		}
		if valid {
			out["sqlstate"] = state
		}
	}
	return out
}
