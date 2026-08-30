package channelonboarding

import (
	"context"
	"errors"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestConnectedChannelProofOptionalLifecycle(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity,
		principal: operatorchannel.Principal{ID: "principal-a"},
		binding:   operatorchannel.Binding{Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent},
		proofErr:  operatorchannel.ErrProofUnavailable,
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}

	if _, err := service.RevokeProof(context.Background(), identity.Selector, 1, "proofless-key", "proofless-hash"); !errors.Is(err, operatorchannel.ErrProofUnavailable) {
		t.Fatalf("proofless revoke error = %v", err)
	}
	if len(store.operations) != 0 || identities.revokeCalls != 0 {
		t.Fatalf("proofless revoke mutated lifecycle: operations=%#v revoke_calls=%d", store.operations, identities.revokeCalls)
	}
	if want := []string{"principal", "resolve", "current_proof", "list"}; !reflect.DeepEqual(calls, want) {
		t.Fatalf("proofless call order = %#v, want %#v", calls, want)
	}
}

func TestChannelProofRevokeRetiresProofLinkedActivationAuthority(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity,
		principal: operatorchannel.Principal{ID: "principal-a"},
		proof: operatorchannel.VerifiedProof{
			ProofID: "proof-a", Interface: identity, Revision: 2, Status: operatorchannel.ProofActive,
		},
		revoked: operatorchannel.VerifiedProof{
			ProofID: "proof-a", Interface: identity, Revision: 3, Status: operatorchannel.ProofRevoked,
		},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	revoked, err := service.RevokeProof(context.Background(), identity.Selector, 2, "proof-key", "proof-hash")
	if err != nil {
		t.Fatal(err)
	}
	if revoked.Status != operatorchannel.ProofRevoked || revoked.Revision != 3 {
		t.Fatalf("revoked proof = %#v", revoked)
	}
	want := []string{"principal", "resolve", "current_proof", "reserve", "retire", "refresh", "revoke_proof", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("proof revoke order = %#v, want %#v", calls, want)
	}
}

func TestChannelUnbindRetiresConnectedActivationAuthorityBeforeIdentity(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity,
		principal: operatorchannel.Principal{ID: "principal-a"},
		binding:   operatorchannel.Binding{Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Unbind(context.Background(), identity.Selector, 4, "unbind-key", "unbind-hash"); err != nil {
		t.Fatal(err)
	}
	want := []string{"principal", "resolve", "current_binding", "reserve", "retire", "refresh", "unbind", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("unbind call order = %#v, want %#v", calls, want)
	}
}

func TestChannelUnbindReleasesOnlyOperationOwnedCredentialsAfterAuthorityRetirement(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls, onboarding: []Operation{{
		OperationID: "operation-a", Interface: identity,
		CredentialAdmissions: []CredentialAdmission{
			{Role: "provider", StoreKey: "channel.telegram.provider", Kind: CredentialAdmissionWritten, Receipt: "receipt-a", Epoch: "epoch-a"},
			{Role: "signing", StoreKey: "channel.telegram.signing", Kind: CredentialAdmissionObserved, Receipt: "observation-a", Epoch: "epoch-b"},
		},
	}}}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity, principal: operatorchannel.Principal{ID: "principal-a"},
		binding: operatorchannel.Binding{Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{calls: &calls}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Unbind(context.Background(), identity.Selector, 4, "unbind-key", "unbind-hash"); err != nil {
		t.Fatal(err)
	}
	want := []string{"principal", "resolve", "current_binding", "reserve", "retire", "refresh", "release:channel.telegram.provider", "unbind", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("credential teardown order = %#v, want %#v", calls, want)
	}
}

func TestChannelUnbindReleasesUncheckpointedOperationCredentialAfterAuthorityRetirement(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	reservation := CredentialReservation{Role: "provider", StoreKey: "channel.telegram.provider"}
	op := Operation{OperationID: "operation-a", Interface: identity, CredentialReservations: []CredentialReservation{reservation}}
	store := &recordingTeardownStore{calls: &calls, onboarding: []Operation{op}}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity, principal: operatorchannel.Principal{ID: "principal-a"},
		binding: operatorchannel.Binding{Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent},
	}
	credentialStore, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	credentials, err := NewCredentialWriter(credentialStore)
	if err != nil {
		t.Fatal(err)
	}
	written, err := credentials.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role),
		Value:    "operation-secret", Receipt: credentialReceipt(op.OperationID, reservation.Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	service, err := NewDestructiveService(store, identities, credentials, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Unbind(context.Background(), identity.Selector, 4, "unbind-key", "unbind-hash"); err != nil {
		t.Fatal(err)
	}
	if _, found, err := credentialStore.Get(context.Background(), written.StoreKey); err != nil || found {
		t.Fatalf("uncheckpointed teardown credential found=%v err=%v", found, err)
	}
	refresh := indexOfCall(calls, "refresh")
	unbound := indexOfCall(calls, "unbind")
	if refresh < 0 || unbound < 0 || refresh >= unbound {
		t.Fatalf("destructive cleanup order = %#v", calls)
	}
}

func indexOfCall(calls []string, want string) int {
	for index, call := range calls {
		if call == want {
			return index
		}
	}
	return -1
}

func TestChannelActivationRetiresAuthorityBeforeRuntimeContextUnload(t *testing.T) {
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls}
	identities := &recordingDestructiveIdentities{
		calls: &calls, principal: operatorchannel.Principal{ID: "principal-a"},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.RetireContext(context.Background(), "bundle-v2:sha256:"+strings.Repeat("a", 64), 7, "context-key", "context-hash", "runtime_context_retired")
	if err != nil {
		t.Fatal(err)
	}
	if op.Phase != TeardownSucceeded || op.Scope.ContextPublicationGeneration != 7 {
		t.Fatalf("context teardown = %#v", op)
	}
	want := []string{"principal", "reserve", "retire", "refresh", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("context retirement order = %#v, want %#v", calls, want)
	}
}

func TestChannelActivationRetiresAuthorityBeforeInterfaceRemoval(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls}
	identities := &recordingDestructiveIdentities{
		calls: &calls, principal: operatorchannel.Principal{ID: "principal-a"},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	op, err := service.RetireInterface(context.Background(), identity, "interface-key", "interface-hash", "interface_retired")
	if err != nil {
		t.Fatal(err)
	}
	if op.Phase != TeardownSucceeded || op.Scope.Interface.Normalized() != identity.Normalized() {
		t.Fatalf("interface teardown = %#v", op)
	}
	want := []string{"principal", "reserve", "retire", "refresh", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("interface retirement order = %#v, want %#v", calls, want)
	}
}

func TestChannelTeardownClientCancellationStopsNarrationNotResponsibility(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	ctx, cancel := context.WithCancel(context.Background())
	store := &recordingTeardownStore{calls: &calls, cancelAfterReserve: cancel}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity,
		principal: operatorchannel.Principal{ID: "principal-a"},
		binding:   operatorchannel.Binding{Interface: identity, Revision: 4, Status: operatorchannel.BindingCurrent},
	}
	sawCanceledActivation := false
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls, sawCanceledContext: &sawCanceledActivation}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Unbind(ctx, identity.Selector, 4, "unbind-key", "unbind-hash"); err != nil {
		t.Fatal(err)
	}
	if ctx.Err() != context.Canceled {
		t.Fatalf("client context = %v, want canceled", ctx.Err())
	}
	if store.sawCanceledContext || identities.sawCanceledContext || sawCanceledActivation {
		t.Fatalf("admitted teardown observed client cancellation: store=%v identities=%v activations=%v",
			store.sawCanceledContext, identities.sawCanceledContext, sawCanceledActivation)
	}
	if len(store.operations) != 1 || store.operations[0].Phase != TeardownSucceeded {
		t.Fatalf("teardown responsibility = %#v", store.operations)
	}
}

func TestChannelProofRevokeRecoveryResumesAfterAuthorityRetirement(t *testing.T) {
	identity := testTeardownInterface()
	calls := []string{}
	store := &recordingTeardownStore{calls: &calls, operations: []TeardownOperation{{
		TeardownID: "teardown-a", RequestKeyHash: "revoke-key", RequestHash: "revoke-hash",
		Kind: TeardownProofRevoke, PrincipalID: "principal-a", Scope: TeardownScope{Interface: identity},
		ExpectedProofRevision: 2, Phase: TeardownAuthorityRetired, Revision: 2, RequestedAt: testTeardownNow(), UpdatedAt: testTeardownNow(),
	}}}
	identities := &recordingDestructiveIdentities{
		calls: &calls, identity: identity,
		principal: operatorchannel.Principal{ID: "principal-a"}, proofErr: operatorchannel.ErrProofUnavailable,
		revoked: operatorchannel.VerifiedProof{Interface: identity, Revision: 3, Status: operatorchannel.ProofRevoked},
	}
	service, err := NewDestructiveService(store, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := service.Recover(context.Background()); err != nil {
		t.Fatal(err)
	}
	if got := store.operations[0].Phase; got != TeardownSucceeded {
		t.Fatalf("recovered teardown phase = %s, want %s", got, TeardownSucceeded)
	}
	want := []string{"list", "principal", "resolve", "current_proof", "list", "reserve", "refresh", "revoke_proof", "get", "complete"}
	if !reflect.DeepEqual(calls, want) {
		t.Fatalf("proof recovery call order = %#v, want %#v", calls, want)
	}
}

func TestChannelProofRevokeRecoveryReplaysCommittedProofFileBeforeResponsibilityCompletion(t *testing.T) {
	ctx := context.Background()
	identity := testTeardownInterface()
	now := testTeardownNow()
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	proof := operatorchannel.VerifiedProof{
		Format: operatorchannel.ProofFormat, ProofID: operatorchannel.ProofIDForInterface(identity), Revision: 2,
		Status: operatorchannel.ProofActive, Interface: identity, ExternalAccountRef: "account-a",
		ConversationRef: "conversation-a", ConversationScope: operatorchannel.ConversationScopeDirect,
		Method: "manual", Challenge: operatorchannel.ChallengePrefix + "AAAAAAAAAAAAAAAA",
		OriginalOperationID: "proof-operation-a", MintingStoreID: "store-a", MintingDeploymentID: "deployment-a",
		VerifiedAt: now, OperatorConfirmed: true, ConsentScopes: []operatorchannel.ConsentScope{operatorchannel.ConsentNotify},
	}
	if err := proofs.Put(ctx, proof); err != nil {
		t.Fatal(err)
	}
	responsibilityFailure := errors.New("proof responsibility unavailable")
	operatorStore := &proofReplayOperatorStore{
		principal: operatorchannel.Principal{ID: "11111111-1111-4111-8111-111111111111", CreatedAt: now},
		operations: []operatorchannel.Operation{{
			OperationID: "proof-operation-a", PrincipalID: "11111111-1111-4111-8111-111111111111",
			Interface: identity, State: operatorchannel.StateBound, ProofID: proof.ProofID, ProofRevision: proof.Revision,
		}},
		completionFailure: responsibilityFailure,
	}
	identities, err := operatorchannel.NewService(operatorStore, proofs, []operatorchannel.InterfaceIdentity{identity}, "deployment-a")
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := identities.Bootstrap(ctx, now); err != nil {
		t.Fatal(err)
	}
	if _, err := identities.RevokeProof(ctx, identity.Selector, proof.Revision, now.Add(time.Minute)); !errors.Is(err, responsibilityFailure) {
		t.Fatalf("first proof revoke error = %v, want responsibility failure", err)
	}
	committed, found, err := proofs.Get(ctx, identity)
	if err != nil || !found || committed.Status != operatorchannel.ProofRevoked || committed.Revision != proof.Revision+1 {
		t.Fatalf("committed proof after responsibility failure = %#v, found=%v, err=%v", committed, found, err)
	}

	calls := []string{}
	teardowns := &recordingTeardownStore{calls: &calls, operations: []TeardownOperation{{
		TeardownID: "teardown-a", RequestKeyHash: "revoke-key", RequestHash: "revoke-hash",
		Kind: TeardownProofRevoke, PrincipalID: operatorStore.principal.ID, Scope: TeardownScope{Interface: identity},
		ExpectedProofRevision: proof.Revision, Phase: TeardownAuthorityRetired, Revision: 2,
		RequestedAt: now, UpdatedAt: now,
	}}}
	destructive, err := NewDestructiveService(teardowns, identities, recordingCredentialReleaser{}, recordingActivationRefresher{calls: &calls}, testTeardownNow, nil)
	if err != nil {
		t.Fatal(err)
	}
	if err := destructive.Recover(ctx); err != nil {
		t.Fatal(err)
	}
	if teardowns.operations[0].Phase != TeardownSucceeded || operatorStore.completionCalls != 2 {
		t.Fatalf("recovery teardown=%s responsibility completions=%d, want succeeded/2", teardowns.operations[0].Phase, operatorStore.completionCalls)
	}
	replayed, found, err := proofs.Get(ctx, identity)
	if err != nil || !found || !reflect.DeepEqual(replayed, committed) {
		t.Fatalf("replayed proof = %#v, found=%v, err=%v; want %#v", replayed, found, err, committed)
	}
}

func testTeardownInterface() operatorchannel.InterfaceIdentity {
	return operatorchannel.InterfaceIdentity{
		InterfaceRef:  operatorchannel.InterfaceHITLChannelV2,
		ChannelPackID: "provider.telegram.hitl_channel", ChannelPackVersion: "1.0.0",
		ChannelManifestHash: "sha256:manifest", SemanticGeneration: "sha256:generation",
	}.Normalized()
}

func testTeardownNow() time.Time { return time.Date(2026, 8, 26, 12, 0, 0, 0, time.UTC) }

type recordingTeardownStore struct {
	calls              *[]string
	operations         []TeardownOperation
	cancelAfterReserve context.CancelFunc
	sawCanceledContext bool
	onboarding         []Operation
}

type proofReplayOperatorStore struct {
	operatorchannel.Store
	principal         operatorchannel.Principal
	operations        []operatorchannel.Operation
	completionFailure error
	completionCalls   int
}

func (s *proofReplayOperatorStore) EnsureOperatorPrincipal(context.Context, time.Time) (operatorchannel.Principal, error) {
	return s.principal, nil
}

func (s *proofReplayOperatorStore) BindOperatorChannelFromProof(_ context.Context, req operatorchannel.BootBindRequest) (operatorchannel.Binding, error) {
	return operatorchannel.Binding{PrincipalID: req.PrincipalID, Interface: req.Interface, Revision: 1, Status: operatorchannel.BindingCurrent}, nil
}

func (s *proofReplayOperatorStore) ListOperatorChannelOperations(context.Context, string) ([]operatorchannel.Operation, error) {
	return append([]operatorchannel.Operation(nil), s.operations...), nil
}

func (s *proofReplayOperatorStore) ListOperatorChannelBindings(context.Context, string) ([]operatorchannel.Binding, error) {
	return nil, nil
}

func (s *proofReplayOperatorStore) ListPendingProofResponsibilities(context.Context) ([]operatorchannel.ProofResponsibility, error) {
	return nil, nil
}

func (s *proofReplayOperatorStore) CompleteProofResponsibility(context.Context, string, string, int64, operatorchannel.ProofStatus, string, time.Time) error {
	s.completionCalls++
	if s.completionFailure != nil {
		err := s.completionFailure
		s.completionFailure = nil
		return err
	}
	return nil
}

func (s *recordingTeardownStore) ListChannelOnboardingOperations(ctx context.Context) ([]Operation, error) {
	s.observe(ctx)
	return append([]Operation(nil), s.onboarding...), nil
}

func (s *recordingTeardownStore) record(call string) { *s.calls = append(*s.calls, call) }

func (s *recordingTeardownStore) observe(ctx context.Context) {
	if ctx.Err() != nil {
		s.sawCanceledContext = true
	}
}

func (s *recordingTeardownStore) ReserveChannelTeardown(ctx context.Context, req ReserveTeardownRequest) (TeardownOperation, error) {
	s.observe(ctx)
	s.record("reserve")
	for _, operation := range s.operations {
		if operation.RequestKeyHash == req.RequestKeyHash {
			if operation.RequestHash != req.RequestHash {
				return TeardownOperation{}, ErrConflict
			}
			return operation, nil
		}
	}
	operation := TeardownOperation{
		TeardownID: req.TeardownID, RequestKeyHash: req.RequestKeyHash, RequestHash: req.RequestHash,
		Kind: req.Kind, PrincipalID: req.PrincipalID, Scope: req.Scope, ExpectedBindingRevision: req.ExpectedBindingRevision,
		ExpectedProofRevision: req.ExpectedProofRevision, Phase: TeardownReserved, Revision: 1,
		RequestedAt: req.RequestedAt, UpdatedAt: req.RequestedAt,
	}
	s.operations = append(s.operations, operation)
	if s.cancelAfterReserve != nil {
		s.cancelAfterReserve()
	}
	return operation, nil
}

func (s *recordingTeardownStore) GetChannelTeardown(ctx context.Context, id string) (TeardownOperation, error) {
	s.observe(ctx)
	s.record("get")
	for _, operation := range s.operations {
		if operation.TeardownID == id {
			return operation, nil
		}
	}
	return TeardownOperation{}, ErrNotFound
}

func (s *recordingTeardownStore) ListChannelTeardowns(ctx context.Context) ([]TeardownOperation, error) {
	s.observe(ctx)
	s.record("list")
	return append([]TeardownOperation(nil), s.operations...), nil
}

func (s *recordingTeardownStore) RetireChannelTeardownAuthority(ctx context.Context, req RetireTeardownAuthorityRequest) (TeardownOperation, error) {
	s.observe(ctx)
	s.record("retire")
	for index := range s.operations {
		if s.operations[index].TeardownID == req.TeardownID {
			s.operations[index].Phase = TeardownAuthorityRetired
			s.operations[index].Revision++
			return s.operations[index], nil
		}
	}
	return TeardownOperation{}, ErrNotFound
}

func (s *recordingTeardownStore) CompleteChannelTeardown(ctx context.Context, req CompleteTeardownRequest) (TeardownOperation, error) {
	s.observe(ctx)
	s.record("complete")
	for index := range s.operations {
		if s.operations[index].TeardownID == req.TeardownID {
			if req.Succeeded {
				s.operations[index].Phase = TeardownSucceeded
			} else {
				s.operations[index].Phase = TeardownFailed
			}
			s.operations[index].Revision++
			return s.operations[index], nil
		}
	}
	return TeardownOperation{}, ErrNotFound
}

type recordingDestructiveIdentities struct {
	calls              *[]string
	principal          operatorchannel.Principal
	identity           operatorchannel.InterfaceIdentity
	binding            operatorchannel.Binding
	proof              operatorchannel.VerifiedProof
	proofErr           error
	revoked            operatorchannel.VerifiedProof
	revokeCalls        int
	sawCanceledContext bool
}

func (i *recordingDestructiveIdentities) record(call string) { *i.calls = append(*i.calls, call) }

func (i *recordingDestructiveIdentities) observe(ctx context.Context) {
	if ctx.Err() != nil {
		i.sawCanceledContext = true
	}
}
func (i *recordingDestructiveIdentities) Principal() (operatorchannel.Principal, error) {
	i.record("principal")
	return i.principal, nil
}
func (i *recordingDestructiveIdentities) ResolveRetainedInterface(ctx context.Context, _ string) (operatorchannel.InterfaceIdentity, error) {
	i.observe(ctx)
	i.record("resolve")
	return i.identity, nil
}
func (i *recordingDestructiveIdentities) CurrentBinding(ctx context.Context, _ operatorchannel.InterfaceIdentity) (operatorchannel.Binding, error) {
	i.observe(ctx)
	i.record("current_binding")
	return i.binding, nil
}
func (i *recordingDestructiveIdentities) CurrentProof(ctx context.Context, _ string) (operatorchannel.VerifiedProof, error) {
	i.observe(ctx)
	i.record("current_proof")
	return i.proof, i.proofErr
}
func (i *recordingDestructiveIdentities) Unbind(ctx context.Context, _ string, _ int64, _, _ string, _ time.Time) (operatorchannel.Operation, operatorchannel.Binding, error) {
	i.observe(ctx)
	i.record("unbind")
	return operatorchannel.Operation{}, operatorchannel.Binding{Interface: i.identity, Status: operatorchannel.BindingUnbound}, nil
}
func (i *recordingDestructiveIdentities) RevokeProof(ctx context.Context, _ string, _ int64, _ time.Time) (operatorchannel.VerifiedProof, error) {
	i.observe(ctx)
	i.record("revoke_proof")
	i.revokeCalls++
	return i.revoked, nil
}

type recordingActivationRefresher struct {
	calls              *[]string
	sawCanceledContext *bool
}

type recordingCredentialReleaser struct {
	calls *[]string
}

func (r recordingCredentialReleaser) Release(_ context.Context, admission CredentialAdmission) (bool, error) {
	if r.calls != nil && admission.Kind == CredentialAdmissionWritten {
		*r.calls = append(*r.calls, "release:"+admission.StoreKey)
	}
	return admission.Kind == CredentialAdmissionWritten, nil
}

func (r recordingCredentialReleaser) ReleaseOperation(ctx context.Context, operation Operation) error {
	for _, admission := range operation.CredentialAdmissions {
		if _, err := r.Release(ctx, admission); err != nil {
			return err
		}
	}
	return nil
}

func (r recordingActivationRefresher) RefreshChannelActivations(ctx context.Context) error {
	if r.sawCanceledContext != nil && ctx.Err() != nil {
		*r.sawCanceledContext = true
	}
	*r.calls = append(*r.calls, "refresh")
	return nil
}

func (r recordingActivationRefresher) PreflightChannelActivation(ctx context.Context, _ Operation, _ Candidate) error {
	if r.sawCanceledContext != nil && ctx.Err() != nil {
		*r.sawCanceledContext = true
	}
	return nil
}
