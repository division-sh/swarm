package runtimepersistence

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/division-sh/swarm/internal/servedparity"
	"github.com/google/uuid"
)

const operatorChannelSupportedSurfaceToken = "operator-channel-supported-surface-token"

type operatorChannelCredentialCurrentness struct{}

func (operatorChannelCredentialCurrentness) CurrentValueMatchesSeal(_ context.Context, evidence runtimecredentials.ValueEvidence) (bool, error) {
	return evidence.Validate() == nil, nil
}

func operatorChannelProviderEvidence() runtimecredentials.ValueEvidence {
	return runtimecredentials.ValueEvidence{
		Key:  "channel.telegram.provider",
		Seal: runtimecredentials.ValueSeal("credential-value-seal-v1:" + strings.Repeat("a", 64)),
	}
}

var (
	errInjectedOperatorChannelProofWrite      = errors.New("injected operator channel proof write failure")
	errInjectedOperatorChannelProofCompletion = errors.New("injected operator channel proof completion failure")
)

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

type failOnceOperatorChannelProofCompletionStore struct {
	operatorchannel.Store
	failed bool
}

func (s *failOnceOperatorChannelProofCompletionStore) CompleteProofResponsibility(ctx context.Context, operationID, proofID string, proofRevision int64, status operatorchannel.ProofStatus, failure string, now time.Time) error {
	if !s.failed {
		s.failed = true
		return errInjectedOperatorChannelProofCompletion
	}
	return s.Store.CompleteProofResponsibility(ctx, operationID, proofID, proofRevision, status, failure, now)
}

type unresolvedOperatorChannelProofMode string

const (
	unresolvedOperatorChannelProofWriteFailed      unresolvedOperatorChannelProofMode = "file_write_failed"
	unresolvedOperatorChannelProofCompletionFailed unresolvedOperatorChannelProofMode = "store_completion_failed"
)

type unresolvedOperatorChannelProofFixture struct {
	service         *operatorchannel.Service
	proofs          *operatorchannel.FileProofStore
	operation       operatorchannel.Operation
	binding         operatorchannel.Binding
	confirmRevision int64
}

func seedUnresolvedOperatorChannelProof(t *testing.T, fixture operatorChannelContractFixture, identity operatorchannel.InterfaceIdentity, mode unresolvedOperatorChannelProofMode, now time.Time) unresolvedOperatorChannelProofFixture {
	t.Helper()
	ctx := context.Background()
	proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
	if err != nil {
		t.Fatal(err)
	}
	var serviceStore operatorchannel.Store = fixture.store
	var serviceProofs operatorchannel.ProofStore = proofs
	wantErr := errInjectedOperatorChannelProofWrite
	if mode == unresolvedOperatorChannelProofCompletionFailed {
		serviceStore = &failOnceOperatorChannelProofCompletionStore{Store: fixture.store}
		wantErr = errInjectedOperatorChannelProofCompletion
	} else {
		serviceProofs = &failOnceOperatorChannelProofStore{delegate: proofs}
	}
	service, err := operatorchannel.NewService(serviceStore, serviceProofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := service.Bootstrap(ctx, now); err != nil {
		t.Fatal(err)
	}
	op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "unresolved-"+string(mode)+"-key", "unresolved-"+string(mode)+"-request", operatorChannelProviderEvidence(), true, now)
	if err != nil {
		t.Fatal(err)
	}
	settlement, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, "account-"+string(mode), "conversation-"+string(mode), uuid.NewString()), now.Add(time.Second))
	if err != nil {
		t.Fatal(err)
	}
	confirmed, binding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(2*time.Second))
	if !errors.Is(err, wantErr) || confirmed.State != operatorchannel.StateBound || binding.Revision != 1 ||
		(confirmed.ProofStatus != operatorchannel.ProofPending && confirmed.ProofStatus != operatorchannel.ProofFailed) {
		t.Fatalf("seed unresolved proof = op:%#v binding:%#v err:%v", confirmed, binding, err)
	}
	return unresolvedOperatorChannelProofFixture{service: service, proofs: proofs, operation: confirmed, binding: binding, confirmRevision: settlement.Operation.Revision}
}

func TestServedParityHarnessOperatorChannelLifecycle(t *testing.T) {
	scenarios := []servedparity.Scenario{
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
			service, err := operatorchannel.NewService(fixture.store, failingProofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			now := time.Date(2026, 8, 24, 15, 0, 0, 0, time.UTC)
			principal, _, err := service.Bootstrap(ctx, now)
			if err != nil {
				t.Fatal(err)
			}
			op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "proof-recovery-key", "proof-recovery-request", operatorChannelProviderEvidence(), true, now)
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
			responsibility := responsibilities[0]
			if responsibility.Operation.ProviderCredential != operatorChannelProviderEvidence() ||
				responsibility.Binding.ProviderCredential != responsibility.Operation.ProviderCredential ||
				responsibility.Proof.ProviderCredential != responsibility.Operation.ProviderCredential {
				t.Fatalf("proof responsibility provider evidence diverged: %#v", responsibility)
			}
			replayed, replayedBinding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(3*time.Second))
			if err != nil || replayed.State != operatorchannel.StateBound || replayed.ProofStatus != operatorchannel.ProofActive || replayedBinding.Revision != binding.Revision {
				t.Fatalf("same-service confirmation replay = op:%#v binding:%#v err:%v", replayed, replayedBinding, err)
			}

			recovered, err := operatorchannel.NewService(fixture.store, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
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
			if err != nil || !found || proof.Status != operatorchannel.ProofActive || proof.Revision != 1 || proof.ProviderCredential != operatorChannelProviderEvidence() {
				t.Fatalf("recovered proof = %#v found=%v err=%v", proof, found, err)
			}
			readback, err := recovered.Readback(ctx)
			if err != nil || len(readback) != 1 || readback[0].Status != operatorchannel.BindingCurrent || readback[0].BindingRevision != 1 || readback[0].ProofRevision != 1 {
				t.Fatalf("recovered readback = %#v, %v", readback, err)
			}
		})
	}
}

func TestOperatorChannelProviderCredentialRotationSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, source := range []string{"file", "env_overlay"} {
			for _, saveProof := range []bool{false, true} {
				name := fmt.Sprintf("%s/%s/save_proof_%t", backend, source, saveProof)
				t.Run(name, func(t *testing.T) {
					ctx := context.Background()
					fixture := openOperatorChannelContractFixture(t, backend)
					identity := operatorChannelContractIdentity("provider-rotation-" + strings.ReplaceAll(name, "/", "-"))
					proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
					if err != nil {
						t.Fatal(err)
					}
					credentialPath := filepath.Join(t.TempDir(), "provider-credentials.json")
					writableCredentialStore, err := runtimecredentials.NewFileStore(credentialPath)
					if err != nil {
						t.Fatal(err)
					}
					var credentialStore runtimecredentials.Store = writableCredentialStore
					setCredential := writableCredentialStore.Set
					if source == "env_overlay" {
						t.Setenv("channel.telegram.provider", "provider-a")
						t.Setenv("channel.telegram.signing", "signing-a")
						credentialStore = runtimecredentials.NewOverlayStore(runtimecredentials.EnvStore{}, writableCredentialStore)
						setCredential = func(_ context.Context, key, value string) error { return os.Setenv(key, value) }
					}
					if err := setCredential(ctx, "channel.telegram.provider", "provider-a"); err != nil {
						t.Fatal(err)
					}
					if err := setCredential(ctx, "channel.telegram.signing", "signing-a"); err != nil {
						t.Fatal(err)
					}
					credentialOwner, err := runtimecredentials.NewSnapshotOwner(credentialStore)
					if err != nil {
						t.Fatal(err)
					}
					providerA, err := credentialOwner.SealCurrentValue(ctx, "channel.telegram.provider")
					if err != nil {
						t.Fatal(err)
					}
					service, err := operatorchannel.NewService(fixture.store, proofs, credentialOwner, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
					if err != nil {
						t.Fatal(err)
					}
					now := time.Date(2026, 8, 30, 12, 0, 0, 0, time.UTC)
					if _, _, err := service.Bootstrap(ctx, now); err != nil {
						t.Fatal(err)
					}
					op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "rotation-connect-key", "rotation-connect-request", providerA, saveProof, now)
					if err != nil {
						t.Fatal(err)
					}
					settlement, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, "account-a", "conversation-a", uuid.NewString()), now.Add(time.Second))
					if err != nil {
						t.Fatal(err)
					}
					_, binding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(2*time.Second))
					if err != nil || binding.ProviderCredential != providerA {
						t.Fatalf("initial binding = %#v, %v", binding, err)
					}

					if err := setCredential(ctx, "channel.telegram.signing", "signing-b"); err != nil {
						t.Fatal(err)
					}
					if current, err := service.CurrentBinding(ctx, identity); err != nil || current.Revision != 1 {
						t.Fatalf("signing rotation changed identity = %#v, %v", current, err)
					}
					if err := setCredential(ctx, "channel.telegram.provider", "provider-a"); err != nil {
						t.Fatal(err)
					}
					if _, err := service.CurrentBinding(ctx, identity); err != nil {
						t.Fatalf("same-value reset invalidated identity: %v", err)
					}
					if err := setCredential(ctx, "channel.telegram.provider", "provider-b"); err != nil {
						t.Fatal(err)
					}
					stale, err := service.CurrentBinding(ctx, identity)
					if !errors.Is(err, operatorchannel.ErrCredentialStale) || stale.Revision != 1 {
						t.Fatalf("provider rotation current binding = %#v, %v", stale, err)
					}
					providerB, err := credentialOwner.SealCurrentValue(ctx, "channel.telegram.provider")
					if err != nil {
						t.Fatal(err)
					}
					reconnect, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationReconnect, stale.Revision, "rotation-reconnect-key", "rotation-reconnect-request", providerB, saveProof, now.Add(3*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					settlement, err = fixture.settle(ctx, operatorChannelContractClaim(reconnect, operatorchannel.ConversationScopeDirect, "account-a", "conversation-a", uuid.NewString()), now.Add(4*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					_, binding, err = service.Confirm(ctx, reconnect.OperationID, settlement.Operation.Revision, true, now.Add(5*time.Second))
					if err != nil || binding.Revision != 2 || binding.ProviderCredential != providerB {
						t.Fatalf("reverified binding = %#v, %v", binding, err)
					}
					if saveProof {
						proof, found, err := proofs.Get(ctx, identity)
						if err != nil || !found || proof.ProviderCredential != providerB || proof.Revision != 2 {
							t.Fatalf("reverified proof = %#v found=%v err=%v", proof, found, err)
						}
					} else if _, found, err := proofs.Get(ctx, identity); err != nil || found {
						t.Fatalf("proofless binding created proof found=%v err=%v", found, err)
					}

					reopenedWritable, err := runtimecredentials.NewFileStore(credentialPath)
					if err != nil {
						t.Fatal(err)
					}
					var reopenedStore runtimecredentials.Store = reopenedWritable
					if source == "env_overlay" {
						reopenedStore = runtimecredentials.NewOverlayStore(runtimecredentials.EnvStore{}, reopenedWritable)
					}
					reopenedOwner, err := runtimecredentials.NewSnapshotOwner(reopenedStore)
					if err != nil {
						t.Fatal(err)
					}
					restarted, err := operatorchannel.NewService(fixture.store, proofs, reopenedOwner, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
					if err != nil {
						t.Fatal(err)
					}
					if _, _, err := restarted.Bootstrap(ctx, now.Add(6*time.Second)); err != nil {
						t.Fatal(err)
					}
					if current, err := restarted.CurrentBinding(ctx, identity); err != nil || current.ProviderCredential != providerB {
						t.Fatalf("restart binding = %#v, %v", current, err)
					}

					resetFixture := openOperatorChannelContractFixture(t, backend)
					resetService, err := operatorchannel.NewService(resetFixture.store, proofs, reopenedOwner, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
					if err != nil {
						t.Fatal(err)
					}
					_, resetBindings, err := resetService.Bootstrap(ctx, now.Add(7*time.Second))
					if err != nil {
						t.Fatal(err)
					}
					if saveProof && (len(resetBindings) != 1 || resetBindings[0].ProviderCredential != providerB) {
						t.Fatalf("proof-backed reset bindings = %#v", resetBindings)
					}
					if !saveProof && len(resetBindings) != 0 {
						t.Fatalf("proofless reset recovered authority: %#v", resetBindings)
					}
				})
			}
		}
	}
}

func TestOperatorChannelProofResponsibilityReconcilesCommittedFileSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			ctx := context.Background()
			now := time.Date(2026, 8, 25, 11, 0, 0, 0, time.UTC)

			t.Run("exact_file_converges_after_restart", func(t *testing.T) {
				fixture := openOperatorChannelContractFixture(t, backend)
				identity := operatorChannelContractIdentity("proof-file-first-recovery-generation")
				proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				completionStore := &failOnceOperatorChannelProofCompletionStore{Store: fixture.store}
				mintingDeploymentID := uuid.NewString()
				service, err := operatorchannel.NewService(completionStore, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, mintingDeploymentID)
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := service.Bootstrap(ctx, now); err != nil {
					t.Fatal(err)
				}
				op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "proof-file-first-key", "proof-file-first-request", operatorChannelProviderEvidence(), true, now)
				if err != nil {
					t.Fatal(err)
				}
				settlement, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeShared, "account-file-first", "conversation-file-first", uuid.NewString()), now.Add(time.Second))
				if err != nil {
					t.Fatal(err)
				}
				confirmed, binding, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(2*time.Second))
				if !errors.Is(err, errInjectedOperatorChannelProofCompletion) || confirmed.ProofStatus != operatorchannel.ProofPending || binding.Revision != 1 {
					t.Fatalf("file-first confirmation = op:%#v binding:%#v err:%v", confirmed, binding, err)
				}
				written, found, err := proofs.Get(ctx, identity)
				if err != nil || !found || written.Revision != 1 || written.MintingDeploymentID != mintingDeploymentID {
					t.Fatalf("written proof = %#v found=%v err=%v", written, found, err)
				}

				recovered, err := operatorchannel.NewService(completionStore, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := recovered.Bootstrap(ctx, now.Add(3*time.Second)); err != nil {
					t.Fatalf("recover file-first responsibility: %v", err)
				}
				responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
				if err != nil || len(responsibilities) != 0 {
					t.Fatalf("remaining proof responsibilities = %#v, %v", responsibilities, err)
				}
				proofList, err := proofs.List(ctx)
				if err != nil || len(proofList) != 1 || !reflect.DeepEqual(proofList[0], written) {
					t.Fatalf("reconciled proofs = %#v, %v; want unchanged %#v", proofList, err, written)
				}
				readback, err := recovered.Readback(ctx)
				if err != nil || len(readback) != 1 || readback[0].Status != operatorchannel.BindingCurrent || readback[0].ProofStatus != operatorchannel.ProofActive || readback[0].ProofRevision != written.Revision {
					t.Fatalf("recovered readback = %#v, %v", readback, err)
				}
			})

			t.Run("same_revision_mismatch_fails_closed", func(t *testing.T) {
				fixture := openOperatorChannelContractFixture(t, backend)
				identity := operatorChannelContractIdentity("proof-file-first-mismatch-generation")
				proofs, err := operatorchannel.NewFileProofStore(t.TempDir())
				if err != nil {
					t.Fatal(err)
				}
				failingProofs := &failOnceOperatorChannelProofStore{delegate: proofs}
				service, err := operatorchannel.NewService(fixture.store, failingProofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := service.Bootstrap(ctx, now.Add(10*time.Minute)); err != nil {
					t.Fatal(err)
				}
				op, err := service.Begin(ctx, identity.Selector, operatorchannel.OperationConnect, 0, "proof-mismatch-key", "proof-mismatch-request", operatorChannelProviderEvidence(), true, now.Add(10*time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				settlement, err := fixture.settle(ctx, operatorChannelContractClaim(op, operatorchannel.ConversationScopeDirect, "account-mismatch", "conversation-mismatch", uuid.NewString()), now.Add(11*time.Minute))
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := service.Confirm(ctx, op.OperationID, settlement.Operation.Revision, true, now.Add(12*time.Minute)); !errors.Is(err, errInjectedOperatorChannelProofWrite) {
					t.Fatalf("seed failed responsibility: %v", err)
				}
				responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
				if err != nil || len(responsibilities) != 1 || responsibilities[0].Operation.OperationID != op.OperationID {
					t.Fatalf("pending mismatch responsibility = %#v, %v", responsibilities, err)
				}
				conflicting := responsibilities[0].Proof
				conflicting.MintingDeploymentID = uuid.NewString()
				conflicting.ConversationRef = "different-conversation"
				if err := proofs.Put(ctx, conflicting); err != nil {
					t.Fatal(err)
				}
				recovered, err := operatorchannel.NewService(fixture.store, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := recovered.Bootstrap(ctx, now.Add(13*time.Minute)); !errors.Is(err, operatorchannel.ErrRevisionConflict) {
					t.Fatalf("mismatched proof recovery error = %v", err)
				}
				stored, found, err := proofs.Get(ctx, identity)
				if err != nil || !found || !reflect.DeepEqual(stored, conflicting) {
					t.Fatalf("mismatched proof changed = %#v found=%v err=%v", stored, found, err)
				}
			})
		})
	}
}

func TestOperatorChannelUnresolvedProofResponsibilityFencesBindingMutationSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []unresolvedOperatorChannelProofMode{unresolvedOperatorChannelProofWriteFailed, unresolvedOperatorChannelProofCompletionFailed} {
			t.Run(backend+"/"+string(mode), func(t *testing.T) {
				ctx := context.Background()
				fixture := openOperatorChannelContractFixture(t, backend)
				now := time.Date(2026, 8, 25, 13, 0, 0, 0, time.UTC)
				identity := operatorChannelContractIdentity("unresolved-mutation-" + string(mode))
				seed := seedUnresolvedOperatorChannelProof(t, fixture, identity, mode, now)

				attempts := []struct {
					name      string
					kind      operatorchannel.OperationKind
					saveProof bool
				}{
					{name: "reconnect", kind: operatorchannel.OperationReconnect, saveProof: true},
					{name: "rebind", kind: operatorchannel.OperationRebind, saveProof: true},
					{name: "no_save", kind: operatorchannel.OperationReconnect, saveProof: false},
				}
				for index, attempt := range attempts {
					_, err := seed.service.Begin(ctx, identity.Selector, attempt.kind, seed.binding.Revision,
						fmt.Sprintf("blocked-%s-%s", attempt.name, mode), fmt.Sprintf("blocked-request-%s-%s", attempt.name, mode),
						operatorChannelProviderEvidence(), attempt.saveProof, now.Add(time.Duration(index+3)*time.Second))
					if !errors.Is(err, operatorchannel.ErrConflict) {
						t.Fatalf("%s overtook unresolved %s responsibility: %v", attempt.name, mode, err)
					}
				}
				operations, err := fixture.store.ListOperatorChannelOperations(ctx, seed.operation.PrincipalID)
				if err != nil || len(operations) != 1 {
					t.Fatalf("blocked operations = %#v, %v", operations, err)
				}

				replayed, replayedBinding, err := seed.service.Confirm(ctx, seed.operation.OperationID, seed.confirmRevision, true, now.Add(7*time.Second))
				if err != nil || replayed.ProofStatus != operatorchannel.ProofActive || replayedBinding.Revision != seed.binding.Revision {
					t.Fatalf("responsibility recovery replay = op:%#v binding:%#v err:%v", replayed, replayedBinding, err)
				}
				later, err := seed.service.Begin(ctx, identity.Selector, operatorchannel.OperationReconnect, seed.binding.Revision,
					"later-"+string(mode)+"-key", "later-"+string(mode)+"-request", operatorChannelProviderEvidence(), false, now.Add(8*time.Second))
				if err != nil {
					t.Fatalf("later operation remained fenced: %v", err)
				}
				settlement, err := fixture.settle(ctx, operatorChannelContractClaim(later, operatorchannel.ConversationScopeDirect,
					seed.operation.ExternalAccountRef, seed.operation.ConversationRef, uuid.NewString()), now.Add(9*time.Second))
				if err != nil {
					t.Fatal(err)
				}
				advanced, advancedBinding, err := seed.service.Confirm(ctx, later.OperationID, settlement.Operation.Revision, true, now.Add(10*time.Second))
				if err != nil || advanced.ProofStatus != operatorchannel.ProofSkipped || advancedBinding.Revision != seed.binding.Revision+1 {
					t.Fatalf("later operation = op:%#v binding:%#v err:%v", advanced, advancedBinding, err)
				}
			})
		}
	}
}

func TestOperatorChannelUnbindDischargesUnresolvedProofResponsibilitySelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		for _, mode := range []unresolvedOperatorChannelProofMode{unresolvedOperatorChannelProofWriteFailed, unresolvedOperatorChannelProofCompletionFailed} {
			t.Run(backend+"/"+string(mode), func(t *testing.T) {
				ctx := context.Background()
				fixture := openOperatorChannelContractFixture(t, backend)
				now := time.Date(2026, 8, 25, 14, 0, 0, 0, time.UTC)
				identity := operatorChannelContractIdentity("unresolved-unbind-" + string(mode))
				seed := seedUnresolvedOperatorChannelProof(t, fixture, identity, mode, now)

				unboundOp, unbound, err := seed.service.Unbind(ctx, identity.Selector, seed.binding.Revision,
					"unresolved-unbind-"+string(mode)+"-key", "unresolved-unbind-"+string(mode)+"-request", now.Add(3*time.Second))
				if err != nil || unboundOp.State != operatorchannel.StateUnbound || unbound.Status != operatorchannel.BindingUnbound || unbound.Revision != seed.binding.Revision+1 {
					t.Fatalf("unbind unresolved responsibility = op:%#v binding:%#v err:%v", unboundOp, unbound, err)
				}
				responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
				if err != nil || len(responsibilities) != 0 {
					t.Fatalf("remaining responsibilities = %#v, %v", responsibilities, err)
				}
				operations, err := fixture.store.ListOperatorChannelOperations(ctx, seed.operation.PrincipalID)
				if err != nil {
					t.Fatal(err)
				}
				var predecessor operatorchannel.Operation
				for _, operation := range operations {
					if operation.OperationID == seed.operation.OperationID {
						predecessor = operation
					}
				}
				if predecessor.ProofStatus != operatorchannel.ProofRevoked {
					t.Fatalf("discharged predecessor = %#v", predecessor)
				}

				recovered, err := operatorchannel.NewService(fixture.store, seed.proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
				if err != nil {
					t.Fatal(err)
				}
				if _, _, err := recovered.Bootstrap(ctx, now.Add(4*time.Second)); err != nil {
					t.Fatalf("bootstrap after unbind: %v", err)
				}
				readback, err := recovered.Readback(ctx)
				if err != nil || len(readback) != 1 || readback[0].Status != operatorchannel.BindingUnbound {
					t.Fatalf("durable unbind readback = %#v, %v", readback, err)
				}
			})
		}
	}
}

func TestOperatorChannelUnresolvedProofResponsibilityConcurrentMutationSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			t.Run("completion_vs_begin", func(t *testing.T) {
				ctx := context.Background()
				fixture := openOperatorChannelContractFixture(t, backend)
				now := time.Date(2026, 8, 25, 15, 0, 0, 0, time.UTC)
				identity := operatorChannelContractIdentity("unresolved-concurrent-begin")
				seed := seedUnresolvedOperatorChannelProof(t, fixture, identity, unresolvedOperatorChannelProofCompletionFailed, now)
				start := make(chan struct{})
				completeErr := make(chan error, 1)
				beginErr := make(chan error, 1)
				go func() {
					<-start
					completeErr <- fixture.store.CompleteProofResponsibility(ctx, seed.operation.OperationID, seed.operation.ProofID, seed.operation.ProofRevision, operatorchannel.ProofActive, "", now.Add(3*time.Second))
				}()
				go func() {
					<-start
					_, err := seed.service.Begin(ctx, identity.Selector, operatorchannel.OperationReconnect, seed.binding.Revision,
						"concurrent-begin-key", "concurrent-begin-request", operatorChannelProviderEvidence(), false, now.Add(3*time.Second))
					beginErr <- err
				}()
				close(start)
				if err := <-completeErr; err != nil {
					t.Fatalf("concurrent completion: %v", err)
				}
				if err := <-beginErr; err != nil && !errors.Is(err, operatorchannel.ErrConflict) {
					t.Fatalf("concurrent begin: %v", err)
				}
				responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
				if err != nil || len(responsibilities) != 0 {
					t.Fatalf("concurrent begin responsibilities = %#v, %v", responsibilities, err)
				}
			})

			t.Run("completion_vs_unbind", func(t *testing.T) {
				ctx := context.Background()
				fixture := openOperatorChannelContractFixture(t, backend)
				now := time.Date(2026, 8, 25, 16, 0, 0, 0, time.UTC)
				identity := operatorChannelContractIdentity("unresolved-concurrent-unbind")
				seed := seedUnresolvedOperatorChannelProof(t, fixture, identity, unresolvedOperatorChannelProofCompletionFailed, now)
				start := make(chan struct{})
				completeErr := make(chan error, 1)
				unbindErr := make(chan error, 1)
				go func() {
					<-start
					completeErr <- fixture.store.CompleteProofResponsibility(ctx, seed.operation.OperationID, seed.operation.ProofID, seed.operation.ProofRevision, operatorchannel.ProofActive, "", now.Add(3*time.Second))
				}()
				go func() {
					<-start
					_, _, err := seed.service.Unbind(ctx, identity.Selector, seed.binding.Revision,
						"concurrent-unbind-key", "concurrent-unbind-request", now.Add(3*time.Second))
					unbindErr <- err
				}()
				close(start)
				completion := <-completeErr
				if completion != nil && !errors.Is(completion, operatorchannel.ErrRevisionConflict) {
					t.Fatalf("concurrent completion: %v", completion)
				}
				if err := <-unbindErr; err != nil {
					t.Fatalf("concurrent unbind: %v", err)
				}
				responsibilities, err := fixture.store.ListPendingProofResponsibilities(ctx)
				if err != nil || len(responsibilities) != 0 {
					t.Fatalf("concurrent unbind responsibilities = %#v, %v", responsibilities, err)
				}
				bindings, err := fixture.store.ListOperatorChannelBindings(ctx, seed.operation.PrincipalID)
				if err != nil || len(bindings) != 1 || bindings[0].Status != operatorchannel.BindingUnbound {
					t.Fatalf("concurrent unbind binding = %#v, %v", bindings, err)
				}
			})
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
					OperationID: uuid.NewString(), UpdatedAt: now, ProviderCredential: operatorChannelProviderEvidence(),
				}
				proof := operatorChannelContractProof(identity, binding, now)
				proof.Challenge = []string{"SWARM-BBBBBBBBBBBBBBBB", "SWARM-CCCCCCCCCCCCCCCC"}[index]
				if err := proofs.Put(ctx, proof); err != nil {
					t.Fatal(err)
				}
			}

			service, err := operatorchannel.NewService(fixture.store, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{boundIdentity, operationIdentity}, uuid.NewString())
			if err != nil {
				t.Fatal(err)
			}
			principal, bindings, err := service.Bootstrap(ctx, now.Add(time.Minute))
			if err != nil || principal.ID == "" || len(bindings) != 1 || bindings[0].Interface.Key() != boundIdentity.Key() {
				t.Fatalf("retained bootstrap principal=%#v bindings=%#v err=%v", principal, bindings, err)
			}
			pending, err := service.Begin(ctx, operationIdentity.Selector, operatorchannel.OperationConnect, 0, "retained-operation-key", "retained-operation-request", operatorChannelProviderEvidence(), false, now.Add(2*time.Minute))
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
				rows[row.Identity.Interface.Key()] = row.Identity
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
					OperationID: uuid.NewString(), UpdatedAt: now, ProviderCredential: operatorChannelProviderEvidence(),
				}
				proof := operatorChannelContractProof(identity, binding, now)
				proof.Challenge = []string{"SWARM-AAAAAAAAAAAAAAAA", "SWARM-AAAAAAAAAAAAAAAB"}[index]
				if err := proofs.Put(ctx, proof); err != nil {
					t.Fatal(err)
				}
			}
			service, err := operatorchannel.NewService(fixture.store, proofs, operatorChannelCredentialCurrentness{}, identities, uuid.NewString())
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
	service, err := operatorchannel.NewService(fixture.store, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
	if err != nil {
		t.Fatal(err)
	}
	now := time.Now().UTC().Truncate(time.Second)
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
	if list.PrincipalID != principal.ID || len(list.Channels) != 1 || list.Channels[0].Identity.Interface.Selector != identity.Selector {
		t.Fatalf("%s initial channel.list = %#v", backend, list)
	}

	bindingRevision := int64(0)
	proofRevision := int64(0)
	rejectedOperation, err := service.Begin(context.Background(), identity.Selector, operatorchannel.OperationConnect, 0,
		"reject-"+string(backend), "reject-request-"+string(backend), operatorChannelProviderEvidence(), false, now)
	if err != nil {
		t.Fatal(err)
	}
	rejectedSettlement, err := fixture.settle(context.Background(), operatorChannelContractClaim(rejectedOperation, operatorchannel.ConversationScopeDirect, "rejected-account", "rejected-conversation", uuid.NewString()), now.Add(30*time.Second))
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
		kind := map[string]operatorchannel.OperationKind{
			"channel.connect": operatorchannel.OperationConnect, "channel.reconnect": operatorchannel.OperationReconnect, "channel.rebind": operatorchannel.OperationRebind,
		}[lifecycle.method]
		begun, err := service.Begin(context.Background(), identity.Selector, kind, bindingRevision,
			fmt.Sprintf("%s-%s", backend, lifecycle.method), fmt.Sprintf("%s-%s-request", backend, lifecycle.method),
			operatorChannelProviderEvidence(), true, now.Add(time.Duration(index+1)*time.Second))
		if err != nil {
			t.Fatal(err)
		}
		if begun.State != operatorchannel.StateAwaitingClaim || begun.Interface.Selector != identity.Selector {
			t.Fatalf("%s %s begin = %#v", backend, lifecycle.method, begun)
		}
		claim := operatorChannelContractClaim(begun, lifecycle.scope, lifecycle.account, lifecycle.conversation, uuid.NewString())
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
	if rotatedList.PrincipalID != principal.ID || rotatedList.Channels[0].Identity.BindingRevision != bindingRevision {
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
		statuses[row.Identity.Interface.Selector] = row.Identity.Status
		reasons[row.Identity.Interface.Selector] = row.Identity.Reason
	}
	if statuses[identity.Selector] != operatorchannel.BindingStale || !strings.Contains(reasons[identity.Selector], "semantic generation changed") || statuses[successorIdentity.Selector] != operatorchannel.BindingUnbound {
		t.Fatalf("%s replacement readback statuses=%#v reasons=%#v", backend, statuses, reasons)
	}
	if _, err := service.Begin(context.Background(), identity.Selector, operatorchannel.OperationReconnect, bindingRevision, "stale-key", "stale-request", operatorChannelProviderEvidence(), true, now.Add(9*time.Minute)); !errors.Is(err, operatorchannel.ErrNotFound) {
		t.Fatalf("%s stale predecessor begin error = %v", backend, err)
	}
	if err := service.ReplaceInterfaces([]operatorchannel.InterfaceIdentity{identity}); err != nil {
		t.Fatal(err)
	}
	rotatedList = operatorChannelSupportedSurfaceList(t, rotated.URL, "rotated-token")
	if len(rotatedList.Channels) != 1 || rotatedList.Channels[0].Identity.Status != operatorchannel.BindingCurrent {
		t.Fatalf("%s restored interface readback = %#v", backend, rotatedList)
	}

	recoveredFixture := openOperatorChannelContractFixture(t, operatorChannelBackendName(backend))
	recoveredService, err := operatorchannel.NewService(recoveredFixture.store, proofs, operatorChannelCredentialCurrentness{}, []operatorchannel.InterfaceIdentity{identity}, uuid.NewString())
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
	if list.Channels[0].Identity.Status != operatorchannel.BindingRevoked {
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
	if list.Channels[0].Identity.Status != operatorchannel.BindingUnbound || list.Channels[0].Identity.Reason != "explicit local unbind fence" {
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
		Handlers: apiv1.OperatorChannelHandlers(apiv1.OperatorChannelHandlerOptions{
			Channels: service, Destructive: operatorChannelSupportedSurfaceDestructive{service: service},
			Readback: operatorChannelSupportedSurfaceReadback{service: service}, Idempotency: idempotency,
		}),
	})
	if err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(handler)
	t.Cleanup(server.Close)
	return server
}

type operatorChannelSupportedSurfaceReadback struct {
	service *operatorchannel.Service
}

func (a operatorChannelSupportedSurfaceReadback) ReadbackConnectedChannels(ctx context.Context) ([]channelonboarding.ConnectedChannelReadback, error) {
	identities, err := a.service.Readback(ctx)
	if err != nil {
		return nil, err
	}
	rows := make([]channelonboarding.ConnectedChannelReadback, 0, len(identities))
	for _, identity := range identities {
		rows = append(rows, channelonboarding.ConnectedChannelReadback{Identity: identity})
	}
	return rows, nil
}

type operatorChannelSupportedSurfaceDestructive struct {
	service *operatorchannel.Service
}

func (a operatorChannelSupportedSurfaceDestructive) Unbind(ctx context.Context, selector string, revision int64, requestKey, requestHash string) (operatorchannel.Operation, operatorchannel.Binding, error) {
	return a.service.Unbind(ctx, selector, revision, requestKey, requestHash, time.Now().UTC())
}

func (a operatorChannelSupportedSurfaceDestructive) RevokeProof(ctx context.Context, selector string, revision int64, _, _ string) (operatorchannel.VerifiedProof, error) {
	return a.service.RevokeProof(ctx, selector, revision, time.Now().UTC())
}

func operatorChannelSupportedSurfaceList(t *testing.T, endpoint, token string) struct {
	PrincipalID string                                       `json:"principal_id"`
	Channels    []channelonboarding.ConnectedChannelReadback `json:"channels"`
} {
	t.Helper()
	var result struct {
		PrincipalID string                                       `json:"principal_id"`
		Channels    []channelonboarding.ConnectedChannelReadback `json:"channels"`
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
