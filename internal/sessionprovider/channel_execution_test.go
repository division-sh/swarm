//go:build linux || darwin

package sessionprovider

import (
	"context"
	"errors"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/packs"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	"github.com/division-sh/swarm/internal/runtime/contracts"
	runtimeeffects "github.com/division-sh/swarm/internal/runtime/effects"
	"github.com/division-sh/swarm/internal/runtime/executionposture"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/registration"
	runtimetools "github.com/division-sh/swarm/internal/runtime/tools"
	"go.mau.fi/libsignal/protocol"
	"go.mau.fi/libsignal/session"
	"go.mau.fi/whatsmeow/proto/waE2E"
	waStore "go.mau.fi/whatsmeow/store"
	"go.mau.fi/whatsmeow/types"
	"google.golang.org/protobuf/proto"
)

func (f *activeInputFixture) confirmationContext(t testing.TB) context.Context {
	t.Helper()
	op, a, c := f.operation, f.activation, f.operation.Coordinate
	authority := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelConfirmation, ID: op.ConfirmationOperationID,
		ExecutionOwner: "channel-onboarding:" + op.OperationID, LeaseExpiresAt: time.Now().Add(5 * time.Minute),
		FenceGeneration: c.ContextPublicationGeneration, ExecutionMode: runtimeeffects.ExecutionModeLive,
		ChannelConfirmation: runtimeeffects.ChannelConfirmationAuthority{
			EffectOperationID: op.ConfirmationOperationID, OnboardingOperationID: op.OperationID, OnboardingRevision: op.Revision,
			ActivationID: a.ActivationID, ActivationRevision: a.Revision, BindingRevision: f.binding.Revision,
			PrincipalID: op.PrincipalID, BundleHash: c.BundleHash, BundleIdentity: c.BundleIdentity,
			PackInventoryGeneration: c.PackInventoryGeneration, RuntimeInstanceID: c.RuntimeInstanceID,
			ContextPublicationGeneration: c.ContextPublicationGeneration, PlanGeneration: c.PlanGeneration, TargetGeneration: c.TargetGeneration}}
	if !authority.Valid() {
		t.Fatal("invalid selected confirmation fixture")
	}
	ctx := runtimeeffects.WithExecutionMode(f.ctx, runtimeeffects.ExecutionModeLive)
	ctx = runtimeeffects.WithController(ctx, runtimeeffects.NewController(f.selected.(runtimeeffects.Store)).WithExecutionPosture(executionposture.Live))
	return runtimeeffects.WithAuthority(ctx, authority)
}

func proveNativeJournalWrite(t *testing.T, f *activeInputFixture, ctx context.Context, text string, acknowledged bool,
	write func() (registration.DeliveryResult, error),
) registration.DeliveryResult {
	t.Helper()
	type result struct {
		delivery registration.DeliveryResult
		err      error
	}
	done := make(chan result, 1)
	joined := false
	t.Cleanup(func() {
		if !joined {
			f.peer.cancel()
			select {
			case <-done:
			case <-time.After(5 * time.Second):
				t.Error("native journal write did not join after fixture cancellation")
			}
		}
	})
	go func() {
		delivery, err := write()
		done <- result{delivery, err}
	}()
	var frame sdkPeerFrame
	select {
	case frame = <-f.peer.frames:
	case result := <-done:
		joined = true
		t.Fatalf("native send ended before its provider frame: %+v %v", result.delivery, result.err)
	case err := <-f.peer.failures:
		t.Fatal(err)
	case <-f.peer.ctx.Done():
		t.Fatal("native send did not reach provider frame")
	}
	selected, _ := runtimeeffects.AuthorityFromContext(ctx)
	journal := f.selected.(runtimeeffects.OutcomeStore)
	before, found, err := journal.GetExternalEffectOutcome(ctx, selected.ID)
	if err != nil || !found || before.AttemptState != runtimeeffects.StateLaunched {
		t.Fatalf("SDK send preceded committed launch: %+v found=%t %v", before, found, err)
	}
	requireDecryptedChannelSend(t, f, frame, text)
	if frame.node.AttrGetter().OptionalString("id") != strings.ReplaceAll(selected.ID, "-", "") {
		t.Fatal("SDK send lost exact journal operation identity")
	}
	if acknowledged {
		f.peer.acknowledge(t, frame)
	} else if err := frame.peer.conn.CloseNow(); err != nil {
		t.Fatal(err)
	}
	var got result
	select {
	case got = <-done:
		joined = true
	case <-f.peer.ctx.Done():
		t.Fatal("owned native journal send did not join")
	}
	if got.delivery.OperationID != selected.ID || (got.err == nil) != acknowledged {
		t.Fatalf("wrong native result: %+v %v", got.delivery, got.err)
	}
	want := runtimeeffects.StateOutcomeUncertain
	if acknowledged {
		want = runtimeeffects.StateSettled
	}
	after, found, err := journal.GetExternalEffectOutcome(ctx, selected.ID)
	if err != nil || !found || after.AttemptState != want {
		t.Fatalf("wrong durable settlement: %+v found=%t %v", after, found, err)
	}
	return got.delivery
}

func TestWhatsAppChannelDeliveryJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, acknowledged := range []bool{true, false} {
			outcome := "lost_acknowledgment"
			if acknowledged {
				outcome = "acknowledged"
			}
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				f := newNativeConfirmationFixture(t, backend)
				executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
				confirmation := map[string]any{"destination": f.binding.ConversationRef, "text": "Swarm channel connected."}
				confirmationCtx := f.confirmationContext(t)
				proveNativeJournalWrite(t, f, confirmationCtx, confirmation["text"].(string), true,
					func() (registration.DeliveryResult, error) {
						return executor.DeliverChannelConfirmation(confirmationCtx, "deliver", confirmation, nil)
					})
				var err error
				f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
					OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: channelonboarding.PhaseSucceeded, Now: time.Now().UTC().Truncate(time.Microsecond)})
				if err != nil {
					t.Fatal(err)
				}
				noticeID, err := f.selected.(runtimetools.MailboxPersistence).InsertMailboxItem(f.ctx, runtimetools.MailboxItem{
					Type: runtimetools.NotifyHumanMailboxItemType, Summary: "Native journal notice", Context: []byte(`{"message":"Exact native notice body."}`)})
				if err != nil {
					t.Fatal(err)
				}
				store := f.selected.(render.Store)
				plans, err := store.ListCurrentChannelDeliveryPlans(f.ctx, "", 100)
				if err != nil {
					t.Fatal(err)
				}
				var candidate render.Candidate
				for _, plan := range plans {
					if plan.SourceID == noticeID {
						candidate = plan
					}
				}
				prepared, err := store.FreezeAndPersistChannelRender(f.ctx, candidate.DeliveryID, packs.PresentationBounds{Actions: 8, TextRunes: 2000, LabelRunes: 64})
				if err != nil {
					t.Fatal(err)
				}
				text, err := render.TextReplyPresentation(prepared.Frozen, prepared.Actions)
				if err != nil {
					t.Fatal(err)
				}
				input, err := f.channel.PrepareOperationInput("deliver", map[string]any{"presentation": map[string]any{"text": text}}, map[string]any{"destination": f.binding.ConversationRef})
				if err != nil {
					t.Fatal(err)
				}
				opID, err := runtimeeffects.ChannelDeliveryOperationID(candidate.DeliveryID, prepared.RenderID)
				if err != nil {
					t.Fatal(err)
				}
				c, audience := f.activation.Coordinate, prepared.Frozen.Audience
				a := runtimeeffects.Authority{Kind: runtimeeffects.AuthorityChannelDelivery, ID: opID,
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
				ctx = runtimeeffects.WithAuthority(ctx, a)
				result := proveNativeJournalWrite(t, f, ctx, text, acknowledged,
					func() (registration.DeliveryResult, error) {
						return executor.DeliverChannelMessage(ctx, "deliver", input, nil)
					})
				receipt, found, err := store.GetCurrentChannelSentReceipt(f.ctx, candidate.DeliveryID, opID)
				if err != nil || found != acknowledged || found && (receipt.OperationID != opID || receipt.DeliveryReference != strings.ReplaceAll(opID, "-", "")) {
					t.Fatalf("compiled SDK receipt differs from durable plan: %+v found=%t %v; result=%+v", receipt, found, err, result)
				}
				if _, err := executor.DeliverChannelMessage(ctx, "deliver", input, nil); err == nil {
					t.Fatal("native delivery blindly replayed a settled/uncertain journal operation")
				}
				select {
				case extra := <-f.peer.frames:
					t.Fatalf("native delivery replayed: %+v", extra.node)
				default:
				}
			})
		}
	}
}

func newNativeConfirmationFixture(t *testing.T, backend string) *activeInputFixture {
	t.Helper()
	f := newActiveInputFixtureWithOutput(t, backend, true)
	f.activate(t)
	device, err := f.state.device(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	if err := f.sender.Sessions.MigratePNToLID(f.ctx, device.ID.ToNonAD(), device.LID); err != nil {
		t.Fatal(err)
	}
	for _, phase := range []channelonboarding.Phase{channelonboarding.PhasePromotingRegistration, channelonboarding.PhaseRetiringPredecessor, channelonboarding.PhaseDeliveringConfirmation} {
		f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
			OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: phase, Now: time.Now().UTC().Truncate(time.Microsecond)})
		if err != nil {
			t.Fatal(err)
		}
	}
	confirmationID, err := channelonboarding.ConfirmationOperationID(f.operation.OperationID, f.operation.ActivationRevision)
	if err != nil {
		t.Fatal(err)
	}
	f.operation, err = f.selected.AdvanceChannelOnboarding(f.ctx, channelonboarding.AdvanceRequest{
		OperationID: f.operation.OperationID, ExpectedRevision: f.operation.Revision, Phase: channelonboarding.PhaseDeliveringConfirmation,
		ConfirmationOperationID: confirmationID, Now: time.Now().UTC().Truncate(time.Microsecond)})
	if err != nil {
		t.Fatal(err)
	}
	to, err := types.ParseJID(f.binding.ConversationRef)
	if err != nil {
		t.Fatal(err)
	}
	f.peer.admitMessageRecipient(to, 1)
	return f
}

func requireDecryptedChannelSend(t testing.TB, f *activeInputFixture, frame sdkPeerFrame, expected string) {
	t.Helper()
	if frame.node.Tag != "message" || frame.node.AttrGetter().JID("to").String() != f.binding.ConversationRef {
		t.Fatalf("wrong SDK destination: %+v", frame.node)
	}
	participantNode := frame.node.GetChildByTag("participants")
	participants := participantNode.GetChildren()
	if len(participants) != 1 {
		t.Fatalf("expected one encrypted recipient, got %+v", participants)
	}
	encrypted := participants[0].GetChildByTag("enc")
	raw, ok := encrypted.Content.([]byte)
	if !ok || encrypted.AttrGetter().OptionalString("type") != "msg" {
		t.Fatalf("expected established Signal session: %+v", encrypted)
	}
	device, err := f.state.device(f.ctx)
	if err != nil {
		t.Fatal(err)
	}
	builder := session.NewBuilderFromSignal(f.sender, device.LID.SignalAddress(), waStore.SignalProtobufSerializer)
	message, err := protocol.NewSignalMessageFromBytes(raw, waStore.SignalProtobufSerializer.SignalMessage)
	if err != nil {
		t.Fatal(err)
	}
	plain, err := session.NewCipher(builder, device.LID.SignalAddress()).Decrypt(f.ctx, message)
	if err != nil || len(plain) == 0 {
		t.Fatal("decrypt SDK outbound message", err)
	}
	padding := int(plain[len(plain)-1])
	if padding < 1 || padding > len(plain) {
		t.Fatal("invalid protocol padding")
	}
	var decoded waE2E.Message
	if err := proto.Unmarshal(plain[:len(plain)-padding], &decoded); err != nil || decoded.GetConversation() != expected {
		t.Fatalf("SDK changed the exact admitted text: %+v %v", &decoded, err)
	}
}

func TestWhatsAppChannelConfirmationJournalBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, outcome := range []string{"acknowledged", "lost_acknowledgment"} {
			t.Run(backend+"/"+outcome, func(t *testing.T) {
				f := newNativeConfirmationFixture(t, backend)
				ctx := f.confirmationContext(t)
				executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
				input := map[string]any{"destination": f.binding.ConversationRef, "text": "Swarm channel connected."}
				type result struct {
					delivery registration.DeliveryResult
					err      error
				}
				done := make(chan result, 1)
				go func() {
					delivery, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil)
					done <- result{delivery, err}
				}()
				var frame sdkPeerFrame
				select {
				case frame = <-f.peer.frames:
				case result := <-done:
					t.Fatalf("native send ended before its provider frame: %+v %v", result.delivery, result.err)
				case err := <-f.peer.failures:
					t.Fatal(err)
				case <-f.peer.ctx.Done():
					t.Fatal("native send did not reach provider frame")
				}
				journal := f.selected.(runtimeeffects.OutcomeStore)
				before, found, err := journal.GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID)
				if err != nil || !found || before.AttemptState != runtimeeffects.StateLaunched {
					t.Fatalf("SDK send preceded committed launch: %+v found=%t %v", before, found, err)
				}
				requireDecryptedChannelSend(t, f, frame, input["text"].(string))
				if frame.node.AttrGetter().OptionalString("id") != strings.ReplaceAll(f.operation.ConfirmationOperationID, "-", "") {
					t.Fatal("SDK send lost exact journal operation identity")
				}
				if outcome == "acknowledged" {
					f.peer.acknowledge(t, frame)
				} else {
					if err := frame.peer.conn.CloseNow(); err != nil {
						t.Fatal(err)
					}
				}
				var got result
				select {
				case got = <-done:
				case <-f.peer.ctx.Done():
					t.Fatal("owned native journal send did not join")
				}
				if got.delivery.OperationID != f.operation.ConfirmationOperationID || (got.err == nil) != (outcome == "acknowledged") {
					t.Fatalf("wrong native result: %+v %v", got.delivery, got.err)
				}
				want := runtimeeffects.StateSettled
				if outcome != "acknowledged" {
					want = runtimeeffects.StateOutcomeUncertain
				}
				after, found, err := journal.GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID)
				if err != nil || !found || after.AttemptState != want {
					t.Fatalf("wrong durable settlement: %+v found=%t %v", after, found, err)
				}
				if _, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil); err == nil {
					t.Fatal("settled or uncertain native effect was blindly replayed")
				}
				f.restartBusinessConnection(t)
				f.authority, err = newSessionAuthorityOwner(f.state, f.selected, f.operation, nil)
				if err != nil {
					t.Fatal(err)
				}
				executor = sessionChannelExecutor{owner: f.authority, plan: f.channel}
				ctx = f.confirmationContext(t)
				if _, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil); err == nil {
					t.Fatal("normal SDK/standing restart replayed the original settled or uncertain send")
				}
				select {
				case extra := <-f.peer.frames:
					t.Fatalf("journal replay emitted another provider frame: %+v", extra.node)
				default:
				}
			})
		}
	}
}

func TestWhatsAppChannelPrelaunchResumeAndRecoveryBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, boundary := range []string{"authorized", "terminal_prelaunch", "startup_reconciled"} {
			t.Run(backend+"/"+boundary, func(t *testing.T) {
				f := newNativeConfirmationFixture(t, backend)
				ctx := f.confirmationContext(t)
				input := map[string]any{"destination": f.binding.ConversationRef, "text": "Exactly one admitted confirmation."}
				toolID, tool, err := f.channel.ConnectorOperation("deliver")
				if err != nil {
					t.Fatal(err)
				}
				fingerprint, err := registration.ChannelWriteFingerprint(toolID, tool, input)
				if err != nil {
					t.Fatal(err)
				}
				handle, err := runtimeeffects.BeginInProcessChannelConfirmation(ctx, contracts.ToolInProcessWhatsAppSendText, fingerprint, nil)
				if err != nil {
					t.Fatal(err)
				}
				if boundary == "terminal_prelaunch" {
					if err := handle.Fail(ctx, runtimeeffects.StateTerminalFailure, runtimefailures.ClassDependencyUnavailable,
						"test_native_prelaunch_rejected", "channel_confirmation", "dispatch", map[string]any{"launch_rejected": true}, errors.New("no send launched")); err == nil {
						t.Fatal("prelaunch rejection lost its failure")
					}
				} else if boundary == "startup_reconciled" {
					if _, err := f.selected.(runtimeeffects.ChannelOnboardingOutcomeStore).ReconcileChannelOnboardingEffectOutcomes(ctx, f.operation.OperationID, time.Now().UTC()); err != nil {
						t.Fatal(err)
					}
				}
				executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
				proveNativeJournalWrite(t, f, ctx, input["text"].(string), true,
					func() (registration.DeliveryResult, error) {
						return executor.DeliverChannelConfirmation(ctx, "deliver", input, nil)
					})
				if _, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil); err == nil {
					t.Fatal("prelaunch continuation duplicated a completed effect")
				}
			})
		}
	}
}

func TestWhatsAppChannelConfirmationRejectsForeignAndRetiredAuthorityBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			f := newNativeConfirmationFixture(t, backend)
			executor := sessionChannelExecutor{owner: f.authority, plan: f.channel}
			ctx := f.confirmationContext(t)
			for _, input := range []map[string]any{
				{"destination": "100000000004@s.whatsapp.net", "text": "wrong conversation"},
				{"destination": f.binding.ConversationRef, "text": "extra input", "callback": "invented"},
			} {
				if _, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil); err == nil {
					t.Fatal("native send accepted unowned input")
				}
			}
			input := map[string]any{"destination": f.binding.ConversationRef, "text": "exact"}
			if _, err := executor.DeliverChannelConfirmation(f.ctx, "deliver", input, nil); err == nil {
				t.Fatal("native send without selected effect authority")
			}
			if err := f.state.close(context.Background()); err != nil {
				t.Fatal(err)
			}
			if _, err := executor.DeliverChannelConfirmation(ctx, "deliver", input, nil); err == nil {
				t.Fatal("retired native session sent an effect")
			}
			if _, found, err := f.selected.(runtimeeffects.OutcomeStore).GetExternalEffectOutcome(ctx, f.operation.ConfirmationOperationID); err != nil || found {
				t.Fatalf("refused native authority left a journal operation: found=%t %v", found, err)
			}
			select {
			case extra := <-f.peer.frames:
				t.Fatalf("refused native authority emitted provider I/O: %+v", extra.node)
			default:
			}
		})
	}
}
