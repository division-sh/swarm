package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/packs"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/runtime/triggergeneration"
	"github.com/google/uuid"
)

func TestChannelOnboardingBoundBeforeParentCheckpointSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			fixture := openChannelOnboardingConfirmationFixture(t, backend)
			for _, verb := range []channelonboarding.Verb{channelonboarding.VerbConnect, channelonboarding.VerbReconnect, channelonboarding.VerbRebind} {
				for _, publishing := range []bool{false, true} {
					for _, proof := range []bool{false, true} {
						for _, restored := range []bool{false, true} {
							for _, recovery := range []bool{false, true} {
								t.Run(fmt.Sprintf("%s/publishing=%t/proof=%t/restored=%t/recovery=%t", verb, publishing, proof, restored, recovery), func(t *testing.T) {
									runBoundHandoff(t, fixture, verb, publishing, proof, restored, recovery)
								})
							}
						}
					}
				}
			}
		})
	}
}

func runBoundHandoff(t *testing.T, fixture channelOnboardingConfirmationFixture, verb channelonboarding.Verb, publishing, proof, restored, recovery bool) {
	t.Helper()
	rig := newBoundHandoffRig(t, fixture)
	ctx := context.Background()
	account := "account-a"
	if verb != channelonboarding.VerbConnect {
		prior := rig.start(t, channelonboarding.VerbConnect, "predecessor-token", proof)
		rig.confirm(t, prior, account)
		rig.complete(t, prior.Operation.OperationID)
	}
	if verb == channelonboarding.VerbRebind {
		account = "account-b"
	}
	begun := rig.start(t, verb, "original-token", proof)
	bound := rig.confirm(t, begun, account)
	parent := rig.parent(t, begun.Operation.OperationID)
	if parent.BindingRevision != 0 || bound.BindingRevision < 1 {
		t.Fatalf("real confirmation did not expose child-before-parent boundary: parent=%#v child=%#v", parent, bound)
	}
	if publishing {
		rig.stopAtPublication = true
		_, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: parent.OperationID})
		if !errors.Is(err, errHandoffPublicationStop) {
			t.Fatalf("publication barrier = %v", err)
		}
		rig.stopAtPublication = false
		parent = rig.parent(t, parent.OperationID)
		if parent.Phase != channelonboarding.PhasePublishingActivation {
			t.Fatalf("publication checkpoint = %#v", parent)
		}
	}
	if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "intervening-token"); err != nil {
		t.Fatal(err)
	}
	if recovery {
		if err := rig.service.ReconcileLocal(ctx); err != nil {
			t.Fatal(err)
		}
	} else {
		_, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: parent.OperationID})
		var required *channelonboarding.CredentialRequiredError
		if !errors.As(err, &required) {
			t.Fatalf("retry did not report credential remediation: %v", err)
		}
	}
	reset := rig.parent(t, parent.OperationID)
	if reset.Phase != channelonboarding.PhasePreparing || reset.BindingRevision != bound.BindingRevision || reset.IdentityOperationID != "" || len(reset.CredentialAdmissions) != 0 {
		t.Fatalf("reset lost exact committed child evidence: %#v", reset)
	}
	value := "replacement-token"
	if restored {
		value = "original-token"
	}
	resumed, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: parent.OperationID, ProviderCredential: value})
	if err != nil {
		t.Fatal(err)
	}
	child := resumed.IdentityOperation
	if child == nil || child.OperationID == bound.OperationID || child.Kind != operatorchannel.OperationReconnect || child.ExpectedBindingRevision != bound.BindingRevision || child.State != operatorchannel.StateAwaitingClaim {
		t.Fatalf("resume skipped fresh exact reconnect: %#v", resumed)
	}
	fresh := rig.confirm(t, resumed, account)
	if fresh.BindingRevision != bound.BindingRevision+1 {
		t.Fatalf("fresh revision=%d, predecessor=%d", fresh.BindingRevision, bound.BindingRevision)
	}
	rig.complete(t, parent.OperationID)
}

var errHandoffPublicationStop = errors.New("stop after real binding checkpoint")

type boundHandoffRig struct {
	fixture           channelOnboardingConfirmationFixture
	file              *runtimecredentials.FileStore
	service           *channelonboarding.Service
	identities        *operatorchannel.Service
	now               time.Time
	stopAtPublication bool
	testBarrier       func(channelonboarding.TestLifecycleBoundary) error
	currentness       *handoffCurrentness
	proofs            *handoffProofStore
	credentialFile    *handoffCredentialFile
}

type handoffCurrentness struct {
	file   runtimecredentials.Store
	calls  int
	failAt int
	err    error
}

func (c *handoffCurrentness) CurrentValueMatchesSeal(ctx context.Context, evidence runtimecredentials.ValueEvidence) (bool, error) {
	c.calls++
	if c.err != nil && (c.failAt == 0 || c.calls == c.failAt) {
		return false, c.err
	}
	return runtimecredentials.CurrentValueMatchesSeal(ctx, c.file, evidence)
}

type handoffProofStore struct {
	*operatorchannel.FileProofStore
	putErr error
}

func (p *handoffProofStore) Put(ctx context.Context, proof operatorchannel.VerifiedProof) error {
	if p.putErr != nil {
		return p.putErr
	}
	return p.FileProofStore.Put(ctx, proof)
}

type handoffCredentialFile struct {
	*runtimecredentials.FileStore
	afterRelease func() error
}

func (s *handoffCredentialFile) DeleteWithReceiptAndSeal(ctx context.Context, key, receipt string, seal runtimecredentials.ValueSeal) (bool, error) {
	deleted, err := s.FileStore.DeleteWithReceiptAndSeal(ctx, key, receipt, seal)
	if err == nil && s.afterRelease != nil {
		err = s.afterRelease()
	}
	return deleted, err
}

type handoffConfirmation struct{ retiredOnboardingConfirmation }

func (handoffConfirmation) DispatchChannelConfirmation(_ context.Context, req channelonboarding.ConfirmationRequest) (channelonboarding.ConfirmationResult, error) {
	return channelonboarding.ConfirmationResult{OperationID: req.Operation.ConfirmationOperationID, TerminalSuccess: true}, nil
}

func newBoundHandoffRig(t *testing.T, fixture channelOnboardingConfirmationFixture) *boundHandoffRig {
	t.Helper()
	rig := &boundHandoffRig{fixture: fixture, now: time.Date(2026, 9, 8, 1, 0, 0, 0, time.UTC)}
	var err error
	rig.file, err = runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	rig.credentialFile = &handoffCredentialFile{FileStore: rig.file}
	writer, err := channelonboarding.NewCredentialWriter(rig.credentialFile)
	if err != nil {
		t.Fatal(err)
	}
	request := channelOnboardingStartRequest("", rig.now)
	request.Interface.SemanticGeneration = uuid.NewString()
	request.Interface.Selector = ""
	request.Interface = request.Interface.Normalized()
	target, err := packs.ParseChannelRegistrationTarget(request.TargetSelector)
	if err != nil {
		t.Fatal(err)
	}
	candidate := channelonboarding.Candidate{
		Provider: request.Provider, Interface: request.Interface, Coordinate: request.Coordinate,
		Target:  channelonboarding.CandidateTarget{Selector: request.TargetSelector, ServiceID: "service-support", FlowPath: target.FlowPath, Alias: "support", Provider: target.Provider, Generation: request.Coordinate.TargetGeneration, PublicationSequence: 1, AdmissionGeneration: triggergeneration.FromCanonicalBytes([]byte("handoff")), SigningCredentialKey: "signing"},
		Posture: request.Posture, Ceremony: request.Ceremony, ProviderCredentialRole: "bot_token", SigningCredentialRole: "webhook_signing_secret", ConfirmationOperation: "deliver",
	}
	catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	rig.proofs = &handoffProofStore{FileProofStore: proofs}
	rig.currentness = &handoffCurrentness{file: rig.file}
	rig.identities, err = operatorchannel.NewService(fixture.store, rig.proofs, rig.currentness, []operatorchannel.InterfaceIdentity{candidate.Interface}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := rig.identities.Bootstrap(context.Background(), rig.now); err != nil {
		t.Fatal(err)
	}
	rig.service, err = channelonboarding.NewService(channelonboarding.ServiceOptions{
		Store: fixture.store, Identities: rig.identities, Credentials: writer,
		Catalog:     func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil },
		Activations: retiredOnboardingActivations{}, Confirmation: handoffConfirmation{}, Readiness: retiredOnboardingReadiness{},
		Now: func() time.Time { return rig.now },
		TestBarrier: func(boundary channelonboarding.TestLifecycleBoundary, _ string) error {
			if rig.testBarrier != nil {
				if err := rig.testBarrier(boundary); err != nil {
					return err
				}
			}
			if boundary == channelonboarding.TestAfterBindingCheckpointBeforeActivation && rig.stopAtPublication {
				return errHandoffPublicationStop
			}
			return nil
		},
	})
	if err != nil {
		t.Fatal(err)
	}
	return rig
}

func (r *boundHandoffRig) start(t *testing.T, verb channelonboarding.Verb, credential string, proof bool) channelonboarding.Result {
	t.Helper()
	result, err := r.service.Start(context.Background(), channelonboarding.StartInput{Verb: verb, Selection: channelonboarding.CandidateSelection{Provider: "telegram"}, ProviderCredential: credential, SaveProof: proof})
	if err != nil || result.IdentityOperation == nil {
		t.Fatalf("start: %#v, %v", result, err)
	}
	return result
}

func (r *boundHandoffRig) confirm(t *testing.T, begun channelonboarding.Result, account string) operatorchannel.Operation {
	t.Helper()
	claimed := r.claim(t, begun, account)
	bound, _, err := r.service.ConfirmIdentity(context.Background(), claimed.OperationID, claimed.Revision, true, r.now)
	if err != nil || bound.State != operatorchannel.StateBound {
		t.Fatalf("confirm: %#v, %v", bound, err)
	}
	return bound
}

func (r *boundHandoffRig) claim(t *testing.T, begun channelonboarding.Result, account string) operatorchannel.Operation {
	t.Helper()
	ctx := context.Background()
	claim := operatorChannelContractClaim(*begun.IdentityOperation, operatorchannel.ConversationScopeDirect, account, "conversation-"+account, uuid.NewString())
	settled, err := r.fixture.settle(ctx, claim, r.now)
	if err != nil || settled.Operation.State != operatorchannel.StateAwaitingConfirmation {
		t.Fatalf("claim: %#v, %v", settled, err)
	}
	awaiting, err := r.service.Retry(ctx, channelonboarding.RetryInput{OperationID: begun.Operation.OperationID})
	if err != nil || awaiting.Operation.Phase != channelonboarding.PhaseAwaitingOperatorConfirmation {
		t.Fatalf("awaiting confirmation: %#v, %v", awaiting, err)
	}
	return settled.Operation
}

func (r *boundHandoffRig) parent(t *testing.T, id string) channelonboarding.Operation {
	t.Helper()
	op, err := r.fixture.store.GetChannelOnboarding(context.Background(), id)
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func (r *boundHandoffRig) complete(t *testing.T, id string) {
	t.Helper()
	result, err := r.service.Retry(context.Background(), channelonboarding.RetryInput{OperationID: id})
	if err != nil || result.Operation.Phase != channelonboarding.PhaseSucceeded {
		t.Fatalf("eventual completion: %#v, %v", result, err)
	}
}
