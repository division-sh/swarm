package runtimepersistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

const operatorChannelSupportedSurfaceToken = "operator-channel-supported-surface-token"

var errInjectedOperatorChannelProofWrite = errors.New("injected operator channel proof write failure")

type failOnceOperatorChannelProofStore struct {
	delegate operatorchannel.ProofStore
	failed   bool
}

func (s *failOnceOperatorChannelProofStore) List(ctx context.Context) ([]operatorchannel.VerifiedProof, error) {
	return s.delegate.List(ctx)
}

func (s *failOnceOperatorChannelProofStore) Get(ctx context.Context, identity operatorchannel.InterfaceIdentity) (operatorchannel.VerifiedProof, bool, error) {
	return s.delegate.Get(ctx, identity)
}

func (s *failOnceOperatorChannelProofStore) Put(ctx context.Context, proof operatorchannel.VerifiedProof) error {
	if !s.failed {
		s.failed = true
		return errInjectedOperatorChannelProofWrite
	}
	return s.delegate.Put(ctx, proof)
}

func (s *failOnceOperatorChannelProofStore) Revoke(ctx context.Context, identity operatorchannel.InterfaceIdentity, revision int64, now time.Time) (operatorchannel.VerifiedProof, error) {
	return s.delegate.Revoke(ctx, identity, revision, now)
}

func TestServedParityHarnessOperatorChannelLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelConnectLifecycle),
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelReconnectLifecycle),
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelRebindLifecycle),
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelConfirmLifecycle),
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelUnbindLifecycle),
		servedparity.MustScenario(servedparity.ScenarioOperatorChannelProofRevokeLifecycle),
	}
	servedparity.RunScenarioGroup(t, scenarios, runOperatorChannelSupportedSurface)
}

func TestOperatorChannelProofResponsibilityRecoversAfterCommittedBindingSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			fixture := openOperatorChannelContractFixture(t, backend)
			identity := operatorChannelContractIdentity("proof-responsibility-generation")
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			failingProofs := &failOnceOperatorChannelProofStore{delegate: proofs}
			service, err := operatorchannel.NewService(fixture.store, failingProofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
			principal, _, err := service.Bootstrap(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "proof-recovery-key", "proof-recovery-request", true, now)
			if err != nil {
				t.Fatal(err)
			}
			settlement, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeShared, "account-recovery", "conversation-recovery", uuid.NewString()), now.Add(time.Second))
			if err != nil || settlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
				t.Fatalf("claim settlement = %#v, %v", settlement, err)
			}
			confirmed, binding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(2*time.Second))
			if !errors.Is(err, errInjectedOperatorChannelProofWrite) || confirmed.State != operatorchannel.StateBound || binding.Revision != 1 {
				t.Fatalf("confirmation = op:%#v binding:%#v err:%v", confirmed, binding, err)
			}
			responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
			if err != nil || len(responsibilities) != 1 || responsibilities[0].Operation.OperationID != op.OperationID || responsibilities[0].Operation.ProofStatus != operatorchannel.ProofFailed {
				t.Fatalf("pending proof responsibilities = %#v, %v", responsibilities, err)
			}
			replayed, replayedBinding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(3*time.Second))
			if err != nil || replayed.State != operatorchannel.StateBound || replayed.ProofStatus != operatorchannel.ProofActive || replayedBinding.Revision != binding.Revision {
				t.Fatalf("same-service confirmation replay = op:%#v binding:%#v err:%v", replayed, replayedBinding, err)
			}

			recovered, err := operatorchannel.NewService(fixture.store, proofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			recoveredPrincipal, _, err := recovered.Bootstrap(ctx, now.Add(3*time.Second))
			if err != nil || recoveredPrincipal.ID != principal.ID {
				t.Fatalf("recovered principal = %#v, %v", recoveredPrincipal, err)
			}
			responsibilities, err = fixture.store.ListPendingProofResponsibilities(ctx)
			if err != nil || len(responsibilities) != 0 {
				t.Fatalf("remaining proof responsibilities = %#v, %v", responsibilities, err)
			}
			proof, found, err := proofs.Get(ctx, identity)
			if err != nil || !found || proof.Status != operatorchannel.ProofActive || proof.Revision != 1 {
				t.Fatalf("recovered proof = %#v found=%v err=%v", proof, found, err)
			}
			readback, err := recovered.Readback(ctx)
			if err != nil || len(readback) != 1 || readback[0].Status != operatorchannel.BindingCurrent || readback[0].BindingRevision != 1 || readback[0].ProofRevision != 1 {
				t.Fatalf("recovered readback = %#v, %v", readback, err)
			}
		})
	}
}

func TestOperatorChannelRetainedLifecycleProjectionSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			fixture := openOperatorChannelContractFixture(t, backend)
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 8, 24, 16, 0, 0, 0, time.UTC)
			boundIdentity := operatorChannelContractIdentity("retained-bound-generation")
			operationIdentity := operatorChannelContractIdentity("retained-operation-generation")
			proofOnlyIdentity := operatorChannelContractIdentity("retained-proof-generation")
			successorIdentity := operatorChannelContractIdentity("retained-successor-generation")

			for index, identity := range []operatorchannel.InterfaceIdentity{boundIdentity, proofOnlyIdentity} {
				binding := operatorchannel.Binding{
					PrincipalID: uuid.NewString(), Interface: identity,
					ExternalAccountRef: fmt.Sprintf("retained-account-%d", index), ConversationRef: fmt.Sprintf("retained-conversation-%d", index),
					ConversationScope: operatorchannel.ConversationScopeDirect, AccountPresentation: "@retained",
					Revision: 1, Status: operatorchannel.BindingCurrent, Source: operatorchannel.BindingSourceLiveVerification,
					OperationID: uuid.NewString(), UpdatedAt: now,
				}
				proof := operatorChannelContractProof(identity, binding, now)
				proof.Challenge = []string{"SWARM-BBBBBBBBBBBBBBBB", "SWARM-CCCCCCCCCCCCCCCC"}[index]
				if err := proofs.Put(ctx, proof); err != nil {
					t.Fatal(err)
				}
			}

			service, err := operatorchannel.NewService(fixture.store, proofs, []operatorchannel.InterfaceIdentity{boundIdentity, operationIdentity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			principal, bindings, err := service.Bootstrap(ctx, now.Add(time.Minute))
			if err != nil || principal.ID == "" || len(bindings) != 1 || bindings[0].Interface.Key() != boundIdentity.Key() {
				t.Fatalf("retained bootstrap principal=%#v bindings=%#v err=%v", principal, bindings, err)
			}
			pending, err := service.Begin(ctx, operationIdentity.Selector, operatorchannel.OperationConnect, 0, "retained-operation-key", "retained-operation-request", false, now.Add(2*time.Minute))
			if err != nil {
				t.Fatal(err)
			}
			idempotency, ok := fixture.store.(apiv1.APIIdempotencyStore)
			if !ok {
				t.Fatalf("selected store %T lacks API idempotency", fixture.store)
			}
			server := newOperatorChannelSupportedSurfaceServer(t, service, idempotency, principal.ID, operatorChannelSupportedSurfaceToken)
			if err := service.ReplaceInterfaces([]operatorchannel.InterfaceIdentity{successorIdentity}); err != nil {
				t.Fatal(err)
			}

			listed := operatorChannelSupportedSurfaceList(t, server.URL, operatorChannelSupportedSurfaceToken)
			if len(listed.Channels) != 4 {
				t.Fatalf("retained readback = %#v", listed)
			}
			rows := map[string]operatorchannel.Readback{}
			for _, row := range listed.Channels {
				rows[row.Interface.Key()] = row
			}
			if rows[boundIdentity.Key()].Status != operatorchannel.BindingStale || rows[proofOnlyIdentity.Key()].ProofStatus != operatorchannel.ProofActive || rows[proofOnlyIdentity.Key()].ProofID == "" ||
				rows[operationIdentity.Key()].PendingOperation == nil || rows[operationIdentity.Key()].PendingOperation.OperationID != pending.OperationID || rows[successorIdentity.Key()].Status != operatorchannel.BindingUnbound {
				t.Fatalf("retained projection rows = %#v", rows)
			}
			var revoked struct {
				Proof operatorchannel.VerifiedProof `json:"proof"`
			}
			operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.proof_revoke", map[string]any{
				"interface": proofOnlyIdentity.Selector, "expected_revision": 1, "idempotency_key": "retained-proof-revoke-" + backend,
			}, &revoked)
			if revoked.Proof.Status != operatorchannel.ProofRevoked || revoked.Proof.Revision != 2 {
				t.Fatalf("revoke retained proof-only identity = %#v", revoked.Proof)
			}
			var unbound struct {
				Operation operatorchannel.Operation `json:"operation"`
				Binding   operatorchannel.Binding   `json:"binding"`
			}
			operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.unbind", map[string]any{
				"interface": boundIdentity.Selector, "expected_revision": 1, "idempotency_key": "retained-unbind-" + backend,
			}, &unbound)
			if unbound.Operation.State != operatorchannel.StateUnbound || unbound.Binding.Status != operatorchannel.BindingUnbound || unbound.Binding.Revision != 2 {
				t.Fatalf("unbind retained binding = %#v", unbound)
			}
		})
	}
}

func TestOperatorChannelProofBootPreservesExactScopeSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			fixture := openOperatorChannelContractFixture(t, backend)
			proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 8, 24, 15, 30, 0, 0, time.UTC)
			identities := []operatorchannel.InterfaceIdentity{
				operatorChannelContractIdentity("proof-boot-direct-generation"),
				operatorChannelContractIdentity("proof-boot-shared-generation"),
			}
			wantScopes := map[string]operatorchannel.ConversationScope{
				identities[0].Key(): operatorchannel.ConversationScopeDirect,
				identities[1].Key(): operatorchannel.ConversationScopeShared,
			}
			for index, identity := range identities {
				binding := operatorchannel.Binding{
					PrincipalID: uuid.NewString(), Interface: identity,
					ExternalAccountRef: fmt.Sprintf("account-proof-%d", index), ConversationRef: fmt.Sprintf("conversation-proof-%d", index),
					ConversationScope: wantScopes[identity.Key()], AccountPresentation: "@operator",
					Revision: 1, Status: operatorchannel.BindingCurrent, Source: operatorchannel.BindingSourceLiveVerification,
					OperationID: uuid.NewString(), UpdatedAt: now,
				}
				proof := operatorChannelContractProof(identity, binding, now)
				proof.Challenge = []string{"SWARM-AAAAAAAAAAAAAAAA", "SWARM-AAAAAAAAAAAAAAAB"}[index]
				if err := proofs.Put(ctx, proof); err != nil {
					t.Fatal(err)
				}
			}
			service, err := operatorchannel.NewService(fixture.store, proofs, identities, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			principal, bindings, err := service.Bootstrap(ctx, now.Add(time.Minute))
			if err != nil || principal.ID == "" || len(bindings) != len(identities) {
				t.Fatalf("proof bootstrap principal=%#v bindings=%#v err=%v", principal, bindings, err)
			}
			for _, binding := range bindings {
				if binding.PrincipalID != principal.ID || binding.Source != operatorchannel.BindingSourceLocalProof || binding.ProofRevision != 1 || binding.ConversationScope != wantScopes[binding.Interface.Key()] {
					t.Fatalf("proof bootstrap binding = %#v", binding)
				}
			}
			if _, err := service.RevokeProof(ctx, identities[0].Selector, 1, now.Add(2*time.Minute)); err != nil {
				t.Fatal(err)
			}
			readback, err := service.Readback(ctx)
			if err != nil || len(readback) != len(identities) {
				t.Fatalf("proof bootstrap readback = %#v, %v", readback, err)
			}
			statuses := map[string]operatorchannel.BindingStatus{}
			for _, row := range readback {
				statuses[row.Interface.Key()] = row.Status
			}
			if statuses[identities[0].Key()] != operatorchannel.BindingRevoked || statuses[identities[1].Key()] != operatorchannel.BindingCurrent {
				t.Fatalf("scope-specific proof revocation statuses = %#v", statuses)
			}
		})
	}
}

func runOperatorChannelSupportedSurface(t *testing.T, backend servedparity.Backend) {
	t.Helper()
	fixture := openOperatorChannelContractFixture(t, operatorChannelBackendName(backend))
	identity := operatorChannelContractIdentity("supported-surface-generation")
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	service, err := operatorchannel.NewService(fixture.store, proofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Date(2026, 8, 24, 14, 0, 0, 0, time.UTC)
	principal, _, err := service.Bootstrap(context.Background(), now)
	if err != nil {
		t.Fatal(err)
	}
	idempotency, ok := fixture.store.(apiv1.APIIdempotencyStore)
	if !ok {
		t.Fatalf("selected store %T lacks API idempotency", fixture.store)
	}

	server := newOperatorChannelSupportedSurfaceServer(t, service, idempotency, principal.ID, operatorChannelSupportedSurfaceToken)
	list := operatorChannelSupportedSurfaceList(t, server.URL, operatorChannelSupportedSurfaceToken)
	if list.PrincipalID != principal.ID || len(list.Channels) != 1 || list.Channels[0].Interface.Selector != identity.Selector {
		t.Fatalf("%s initial channel.list = %#v", backend, list)
	}

	bindingRevision := int64(0)
	proofRevision := int64(0)
	var rejectedBegin struct {
		Operation operatorchannel.Operation `json:"operation"`
	}
	operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.connect", map[string]any{
		"interface": identity.Selector, "expected_revision": 0, "save_proof": false, "idempotency_key": "reject-" + string(backend),
	}, &rejectedBegin)
	rejectedSettlement, err := fixture.settle(context.Background(), operatorChannelContractClaim(rejectedBegin.Operation, operatorchannel.ConversationScopeDirect, "rejected-account", "rejected-conversation", uuid.NewString()), now.Add(30*time.Second))
	if err != nil {
		t.Fatal(err)
	}
	var rejectedResult map[string]any
	operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.confirm", map[string]any{
		"operation_id": rejectedSettlement.Operation.OperationID, "expected_revision": rejectedSettlement.Operation.Revision, "approve": false, "idempotency_key": "reject-confirm-" + string(backend),
	}, &rejectedResult)
	if _, present := rejectedResult["binding"]; present {
		t.Fatalf("%s rejected confirmation exposed an empty binding: %#v", backend, rejectedResult)
	}
	lifecycles := []struct {
		method       string
		account      string
		conversation string
		scope        operatorchannel.ConversationScope
	}{
		{method: "channel.connect", account: "account-a", conversation: "conversation-a", scope: operatorchannel.ConversationScopeDirect},
		{method: "channel.reconnect", account: "account-a", conversation: "conversation-a", scope: operatorchannel.ConversationScopeDirect},
		{method: "channel.rebind", account: "account-b", conversation: "conversation-b", scope: operatorchannel.ConversationScopeShared},
	}
	for index, lifecycle := range lifecycles {
		var begun struct {
			Operation operatorchannel.Operation `json:"operation"`
		}
		operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, lifecycle.method, map[string]any{
			"interface": identity.Selector, "expected_revision": bindingRevision, "save_proof": true,
			"idempotency_key": fmt.Sprintf("%s-%s", backend, lifecycle.method),
		}, &begun)
		if begun.Operation.State != operatorchannel.StateAwaitingClaim || begun.Operation.Interface.Selector != identity.Selector {
			t.Fatalf("%s %s begin = %#v", backend, lifecycle.method, begun.Operation)
		}
		claim := operatorChannelContractClaim(begun.Operation, lifecycle.scope, lifecycle.account, lifecycle.conversation, uuid.NewString())
		settlement, err := fixture.settle(context.Background(), claim, now.Add(time.Duration(index+1)*time.Minute))
		if err != nil || settlement.Disposition != operatorchannel.DispositionConsumedBinding || settlement.Operation.State != operatorchannel.StateAwaitingConfirmation {
			t.Fatalf("%s %s claim = %#v, %v", backend, lifecycle.method, settlement, err)
		}
		var confirmed struct {
			Operation operatorchannel.Operation `json:"operation"`
			Binding   operatorchannel.Binding   `json:"binding"`
		}
		confirmKey := fmt.Sprintf("%s-confirm-%d", backend, index)
		operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.confirm", map[string]any{
			"operation_id": settlement.Operation.OperationID, "expected_revision": settlement.Operation.Revision, "approve": true, "idempotency_key": confirmKey,
		}, &confirmed)
		bindingRevision++
		proofRevision++
		if confirmed.Operation.State != operatorchannel.StateBound || confirmed.Binding.Revision != bindingRevision || confirmed.Binding.ProofRevision != proofRevision || confirmed.Binding.ConversationScope != lifecycle.scope {
			t.Fatalf("%s %s confirmation = %#v", backend, lifecycle.method, confirmed)
		}
		var replayed struct {
			Operation operatorchannel.Operation `json:"operation"`
			Binding   operatorchannel.Binding   `json:"binding"`
		}
		operatorChannelSupportedSurfaceCall(t, server.URL, operatorChannelSupportedSurfaceToken, "channel.confirm", map[string]any{
			"operation_id": settlement.Operation.OperationID, "expected_revision": settlement.Operation.Revision, "approve": true, "idempotency_key": confirmKey,
		}, &replayed)
		if replayed.Operation.OperationID != confirmed.Operation.OperationID || replayed.Binding.Revision != confirmed.Binding.Revision {
			t.Fatalf("%s %s confirmation replay = %#v", backend, lifecycle.method, replayed)
		}
	}

	rotated := newOperatorChannelSupportedSurfaceServer(t, service, idempotency, principal.ID, "rotated-token")
	rotatedList := operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if rotatedList.PrincipalID != principal.ID || rotatedList.Channels[0].BindingRevision != bindingRevision {
		t.Fatalf("%s token rotation changed principal or binding: %#v", backend, rotatedList)
	}
	operatorChannelSupportedSurfaceUnauthorized(t, rotated.URL, operatorChannelSupportedSurfaceToken)

	successorIdentity := operatorChannelContractIdentity("supported-surface-successor-generation")
	if err := service.ReplaceInterfaces([]operatorchannel.InterfaceIdentity{successorIdentity}); err != nil {
		t.Fatal(err)
	}
	replacedList := operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if len(replacedList.Channels) != 2 {
		t.Fatalf("%s replacement channel.list = %#v, want predecessor and successor", backend, replacedList)
	}
	statuses := map[string]operatorchannel.BindingStatus{}
	reasons := map[string]string{}
	for _, row := range replacedList.Channels {
		statuses[row.Interface.Selector] = row.Status
		reasons[row.Interface.Selector] = row.Reason
	}
	if statuses[identity.Selector] != operatorchannel.BindingStale || !strings.Contains(reasons[identity.Selector], "semantic generation changed") || statuses[successorIdentity.Selector] != operatorchannel.BindingUnbound {
		t.Fatalf("%s replacement readback statuses=%#v reasons=%#v", backend, statuses, reasons)
	}
	if _, err := service.Begin(context.Background(), identity.Selector, operatorchannel.OperationReconnect, bindingRevision, "stale-key", "stale-request", true, now.Add(9*time.Minute)); !errors.Is(err, operatorchannel.ErrNotFound) {
		t.Fatalf("%s stale predecessor begin error = %v", backend, err)
	}
	if err := service.ReplaceInterfaces([]operatorchannel.InterfaceIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	rotatedList = operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if len(rotatedList.Channels) != 1 || rotatedList.Channels[0].Status != operatorchannel.BindingCurrent {
		t.Fatalf("%s restored interface readback = %#v", backend, rotatedList)
	}

	recoveredFixture := openOperatorChannelContractFixture(t, operatorChannelBackendName(backend))
	recoveredService, err := operatorchannel.NewService(recoveredFixture.store, proofs, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	recoveredPrincipal, recoveredBindings, err := recoveredService.Bootstrap(context.Background(), now.Add(10*time.Minute))
	if err != nil {
		t.Fatal(err)
	}
	if recoveredPrincipal.ID == principal.ID || len(recoveredBindings) != 1 ||
		recoveredBindings[0].Source != operatorchannel.BindingSourceLocalProof ||
		recoveredBindings[0].ProofRevision != proofRevision ||
		recoveredBindings[0].Interface.Key() != identity.Key() {
		t.Fatalf("%s fresh selected-store proof recovery = principal:%#v bindings:%#v", backend, recoveredPrincipal, recoveredBindings)
	}

	var revoked struct {
		Proof operatorchannel.VerifiedProof `json:"proof"`
	}
	operatorChannelSupportedSurfaceCall(t, rotated.URL, "rotated-token", "channel.proof_revoke", map[string]any{
		"interface": identity.Selector, "expected_revision": proofRevision, "idempotency_key": "revoke-" + string(backend),
	}, &revoked)
	if revoked.Proof.Status != operatorchannel.ProofRevoked || revoked.Proof.Revision != proofRevision+1 {
		t.Fatalf("%s proof revoke = %#v", backend, revoked.Proof)
	}
	var replayedRevoke struct {
		Proof operatorchannel.VerifiedProof `json:"proof"`
	}
	operatorChannelSupportedSurfaceCall(t, rotated.URL, "rotated-token", "channel.proof_revoke", map[string]any{
		"interface": identity.Selector, "expected_revision": proofRevision, "idempotency_key": "revoke-" + string(backend),
	}, &replayedRevoke)
	if replayedRevoke.Proof.Revision != revoked.Proof.Revision || replayedRevoke.Proof.Status != operatorchannel.ProofRevoked {
		t.Fatalf("%s proof revoke replay = %#v", backend, replayedRevoke.Proof)
	}
	list = operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if list.Channels[0].Status != operatorchannel.BindingRevoked {
		t.Fatalf("%s revoked channel.list = %#v", backend, list)
	}

	var unboundResult map[string]any
	operatorChannelSupportedSurfaceCall(t, rotated.URL, "rotated-token", "channel.unbind", map[string]any{
		"interface": identity.Selector, "expected_revision": bindingRevision, "idempotency_key": "unbind-" + string(backend),
	}, &unboundResult)
	operationResult := unboundResult["operation"].(map[string]any)
	bindingResult := unboundResult["binding"].(map[string]any)
	for _, absent := range []string{"expires_at", "claimed_at"} {
		if _, present := operationResult[absent]; present {
			t.Fatalf("%s unbind operation exposed zero %s: %#v", backend, absent, operationResult)
		}
	}
	for _, absent := range []string{"external_account_reference", "conversation_reference", "conversation_scope", "account_presentation", "source", "proof_id", "proof_revision"} {
		if _, present := bindingResult[absent]; present {
			t.Fatalf("%s unbind binding exposed inapplicable %s: %#v", backend, absent, bindingResult)
		}
	}
	rawUnbound, err := json.Marshal(unboundResult)
	if err != nil {
		t.Fatal(err)
	}
	var unbound struct {
		Operation operatorchannel.Operation `json:"operation"`
		Binding   operatorchannel.Binding   `json:"binding"`
	}
	if err := json.Unmarshal(rawUnbound, &unbound); err != nil {
		t.Fatal(err)
	}
	if unbound.Operation.State != operatorchannel.StateUnbound || unbound.Binding.Status != operatorchannel.BindingUnbound || unbound.Binding.Revision != bindingRevision+1 {
		t.Fatalf("%s unbind = %#v", backend, unbound)
	}
	list = operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if list.Channels[0].Status != operatorchannel.BindingUnbound || list.Channels[0].Reason != "explicit local unbind fence" {
		t.Fatalf("%s unbound channel.list = %#v", backend, list)
	}
}

func operatorChannelBackendName(backend servedparity.Backend) string {
	if backend == servedparity.BackendExplicitPostgres {
		return "postgres"
	}
	return "sqlite"
}

func newOperatorChannelSupportedSurfaceServer(t *testing.T, service *operatorchannel.Service, idempotency apiv1.APIIdempotencyStore, principalID, token string) *httptest.Server {
	t.Helper()
	registry, err := apiv1.LoadRegistry(filepath.Join(selectedStoreAbstractionRepoRoot(t), "platform-spec.yaml"))
	if err != nil {
		t.Fatal(err)
	}
	handler, err := apiv1.NewHandler(apiv1.Options{
		Registry: registry, AuthTokens: []string{token}, OperatorPrincipalID: principalID,
		Handlers: apiv1.OperatorChannelHandlers(apiv1.OperatorChannelHandlerOptions{Channels: service, Idempotency: idempotency}),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

func operatorChannelSupportedSurfaceList(t *testing.T, endpoint, token string) struct {
	PrincipalID string                     `json:"principal_id"`
	Channels    []operatorchannel.Readback `json:"channels"`
} {
	t.Helper()
	var result struct {
		PrincipalID string                     `json:"principal_id"`
		Channels    []operatorchannel.Readback `json:"channels"`
	}
	operatorChannelSupportedSurfaceCall(t, endpoint, token, "channel.list", map[string]any{}, &result)
	return result
}

func operatorChannelSupportedSurfaceCall(t *testing.T, endpoint, token, method string, params map[string]any, result any) {
	t.Helper()
	body, err := json.Marshal(map[string]any{"jsonrpc": "2.0", "id": uuid.NewString(), "method": method, "params": params})
	if err != nil {
		t.Fatal(err)
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/v1/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	req.Header.Set("Content-Type", "application/json")
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var envelope struct {
		Result json.RawMessage `json:"result"`
		Error  any             `json:"error"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&envelope); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || envelope.Error != nil {
		t.Fatalf("%s status=%d error=%#v", method, resp.StatusCode, envelope.Error)
	}
	if err := json.Unmarshal(envelope.Result, result); err != nil {
		t.Fatalf("decode %s result: %v", method, err)
	}
}

func operatorChannelSupportedSurfaceUnauthorized(t *testing.T, endpoint, token string) {
	t.Helper()
	body := []byte(`{"jsonrpc":"2.0","id":"revoked-token","method":"channel.list","params":{}}`)
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, endpoint+"/v1/rpc", bytes.NewReader(body))
	if err != nil {
		t.Fatal(err)
	}
	req.Header.Set("Authorization", "Bearer "+token)
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != http.StatusUnauthorized {
		t.Fatalf("revoked token status = %d, want %d", resp.StatusCode, http.StatusUnauthorized)
	}
}
