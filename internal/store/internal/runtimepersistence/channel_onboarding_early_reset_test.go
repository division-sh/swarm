package runtimepersistence

import (
	"context"
	"errors"
	"fmt"
	"reflect"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestChannelOnboardingEarlyResetCleanupSelectedStoreParity(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		fixture := openChannelOnboardingConfirmationFixture(t, backend)
		for _, entry := range []string{"credential_stale", "preflight_rejected", "local_reconciliation"} {
			for _, fault := range []string{"first_delete", "partial_delete", "before_reset_commit", "after_reset_commit"} {
				t.Run(backend+"/"+entry+"/"+fault, func(t *testing.T) {
					rig := newBoundHandoffRig(t, fixture)
					before, candidate := checkpointEarlyOnboardingAdmissions(t, rig)
					ctx := context.Background()
					if entry != "preflight_rejected" {
						if err := rig.file.Set(ctx, before.CredentialAdmissions[0].StoreKey, "successor-value"); err != nil {
							t.Fatal(err)
						}
					}
					interrupted := errors.New("early reset interrupted")
					file := &earlyResetCredentialFile{FileStore: rig.file, err: interrupted}
					if fault == "first_delete" {
						file.failAt = 1
					} else if fault == "partial_delete" {
						file.failAt = 2
					}
					store := &earlyResetStore{Store: fixture.store, err: interrupted}
					if fault == "before_reset_commit" || fault == "after_reset_commit" {
						store.commitFault = fault
					}
					activations := &earlyResetActivations{}
					if entry == "preflight_rejected" {
						activations.preflightErr = errors.New("preflight rejected")
					}
					service := newEarlyResetService(t, rig, store, file, candidate, activations)
					if err := executeEarlyResetEntry(ctx, service, before.OperationID, entry); !errors.Is(err, interrupted) {
						t.Fatalf("interruption cause = %v", err)
					}
					checkpoint := rig.parent(t, before.OperationID)
					if fault == "after_reset_commit" {
						if checkpoint.Phase != channelonboarding.PhasePreparing || checkpoint.Revision != before.Revision+1 || len(checkpoint.CredentialAdmissions) != 0 {
							t.Fatalf("actual committed reset lost: %#v", checkpoint)
						}
					} else if !reflect.DeepEqual(checkpoint, before) {
						t.Fatalf("incomplete cleanup/reset discarded durable evidence: before=%#v after=%#v", before, checkpoint)
					}
					if activations.refreshes != 0 || activations.publications != 0 || activations.promotions != 0 {
						t.Fatalf("failed reset granted effects: %#v", activations)
					}
					if entry != "preflight_rejected" {
						value, found, err := rig.file.Get(ctx, before.CredentialAdmissions[0].StoreKey)
						if err != nil || !found || value != "successor-value" {
							t.Fatalf("stale cleanup deleted successor: %q %t %v", value, found, err)
						}
					}
					// Reconstruct owners over the actual retained file/database after interruption.
					reopened, err := runtimecredentials.NewFileStore(rig.credentialPath)
					if err != nil {
						t.Fatal(err)
					}
					service = newEarlyResetService(t, rig, fixture.store, reopened, candidate, activations)
					if fault != "after_reset_commit" {
						err := executeEarlyResetEntry(ctx, service, before.OperationID, entry)
						var required *channelonboarding.CredentialRequiredError
						if entry == "local_reconciliation" && err != nil || entry == "credential_stale" && !errors.As(err, &required) || entry == "preflight_rejected" && err == nil {
							t.Fatalf("cleanup retry = %v", err)
						}
					}
					reset := rig.parent(t, before.OperationID)
					if reset.Phase != channelonboarding.PhasePreparing || len(reset.CredentialAdmissions) != 0 || reset.BindingRevision != before.BindingRevision || reset.Revision != before.Revision+1 {
						t.Fatalf("exact reset = %#v", reset)
					}
					activations.preflightErr = nil
					fresh, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID, ProviderCredential: "fresh-token"})
					if err != nil || fresh.IdentityOperation == nil || fresh.IdentityOperation.State != operatorchannel.StateAwaitingClaim {
						t.Fatalf("restart did not reach fresh ceremony: %#v %v", fresh, err)
					}
					if repeated, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: before.OperationID}); err != nil || repeated.IdentityOperation == nil || repeated.IdentityOperation.OperationID != fresh.IdentityOperation.OperationID {
						t.Fatalf("retry duplicated identity responsibility: %#v %v", repeated, err)
					}
				})
			}
		}
	}
}

func TestChannelOnboardingEarlyResetSelectedStoreFences(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		fixture := openChannelOnboardingConfirmationFixture(t, backend)
		for _, entry := range []string{"credential_stale", "preflight_rejected", "local_reconciliation"} {
			for _, timing := range []string{"before_cleanup", "during_cleanup"} {
				for _, change := range []string{"revision", "phase_advanced", "retired"} {
					t.Run(backend+"/"+entry+"/"+timing+"/"+change, func(t *testing.T) {
						rig := newBoundHandoffRig(t, fixture)
						before, candidate := checkpointEarlyOnboardingAdmissions(t, rig)
						ctx := context.Background()
						if entry != "preflight_rejected" {
							if err := rig.file.Set(ctx, before.CredentialAdmissions[0].StoreKey, "successor-value"); err != nil {
								t.Fatal(err)
							}
						}
						var winner channelonboarding.Operation
						advance := func() {
							if change == "retired" {
								retireChannelOnboardingParent(t, fixture.store, before, rig.now)
							} else {
								phase := before.Phase
								if change == "phase_advanced" {
									phase = channelonboarding.PhaseActivatingProvider
								}
								if _, err := fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: before.OperationID, ExpectedRevision: before.Revision, Phase: phase, Now: rig.now}); err != nil {
									t.Fatal(err)
								}
							}
							winner = rig.parent(t, before.OperationID)
						}
						store := &earlyResetStore{Store: fixture.store}
						file := &earlyResetCredentialFile{FileStore: rig.file, scopeKeys: map[string]bool{}}
						for _, admission := range before.CredentialAdmissions {
							file.scopeKeys[admission.StoreKey] = true
						}
						if timing == "before_cleanup" {
							file.afterSnapshot = advance
						} else {
							file.afterDelete = advance
						}
						activations := &earlyResetActivations{}
						if entry == "preflight_rejected" {
							activations.preflightErr = errors.New("preflight rejected")
						}
						service := newEarlyResetService(t, rig, store, file, candidate, activations)
						if err := executeEarlyResetEntry(ctx, service, before.OperationID, entry); !errors.Is(err, channelonboarding.ErrRevisionConflict) {
							t.Fatalf("stale reset = %v", err)
						}
						if winner.OperationID == "" || !reflect.DeepEqual(rig.parent(t, before.OperationID), winner) {
							t.Fatal("stale reset overwrote the winning transition")
						}
						wantDeletes := 0
						if timing == "during_cleanup" {
							wantDeletes = 1
						}
						if file.calls != wantDeletes {
							t.Fatalf("deleted after losing authority: deletes=%d want=%d", file.calls, wantDeletes)
						}
						for i, admission := range before.CredentialAdmissions {
							if i == 0 && timing == "during_cleanup" && entry == "preflight_rejected" {
								continue
							}
							if _, found, err := rig.file.Get(ctx, admission.StoreKey); err != nil || !found {
								t.Fatalf("stale cleanup removed retained occurrence %s: %t %v", admission.Role, found, err)
							}
						}
						if activations.refreshes != 0 || activations.publications != 0 || activations.promotions != 0 {
							t.Fatalf("losing reset granted effects: %#v", activations)
						}
					})
				}
			}
		}
	}
}

func TestChannelOnboardingEarlyResetPreservesInheritedAndObservedEvidence(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		fixture := openChannelOnboardingConfirmationFixture(t, backend)
		for _, entry := range []string{"credential_stale", "preflight_rejected", "local_reconciliation"} {
			t.Run(backend+"/"+entry, func(t *testing.T) {
				rig := newBoundHandoffRig(t, fixture)
				ctx := context.Background()
				begun := rig.start(t, channelonboarding.VerbConnect, "original-token", false)
				bound := rig.confirm(t, begun, "account-a")
				if err := rig.file.Set(ctx, bound.ProviderCredential.Key, "bound-rotation"); err != nil {
					t.Fatal(err)
				}
				if err := rig.service.ReconcileLocal(ctx); err != nil {
					t.Fatal(err)
				}
				preparing := rig.parent(t, begun.Operation.OperationID)
				if preparing.Phase != channelonboarding.PhasePreparing || preparing.BindingRevision != bound.BindingRevision || bound.BindingRevision < 1 {
					t.Fatalf("inherited reconnect obligation = %#v", preparing)
				}
				result := getEarlyResetCandidate(t, rig, preparing)
				var signingKey string
				for _, reservation := range preparing.CredentialReservations {
					if reservation.Role == result.SigningCredentialRole {
						signingKey = reservation.StoreKey
					}
				}
				if signingKey == "" {
					t.Fatal("missing signing reservation")
				}
				if err := rig.file.Set(ctx, signingKey, "observed-signing-value"); err != nil {
					t.Fatal(err)
				}
				stop := errors.New("stop after credential writes")
				rig.testBarrier = func(at channelonboarding.TestLifecycleBoundary) error {
					if at == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
						return stop
					}
					return nil
				}
				if _, err := rig.service.Retry(ctx, channelonboarding.RetryInput{OperationID: preparing.OperationID, ProviderCredential: "fresh-token"}); !errors.Is(err, stop) {
					t.Fatalf("inherited credential boundary = %v", err)
				}
				rig.testBarrier = nil
				before := checkpointEarlyWrittenAdmissions(t, rig, preparing)
				observed := false
				for _, admission := range before.CredentialAdmissions {
					observed = observed || admission.Kind == channelonboarding.CredentialAdmissionObserved
				}
				if !observed {
					t.Fatal("test did not admit an observed signing value")
				}
				if entry != "preflight_rejected" {
					if err := rig.file.Set(ctx, before.CredentialAdmissions[0].StoreKey, "successor-value"); err != nil {
						t.Fatal(err)
					}
				}
				activations := &earlyResetActivations{}
				if entry == "preflight_rejected" {
					activations.preflightErr = errors.New("preflight rejected")
				}
				service := newEarlyResetService(t, rig, fixture.store, rig.file, result, activations)
				err := executeEarlyResetEntry(ctx, service, before.OperationID, entry)
				if entry == "local_reconciliation" && err != nil || entry != "local_reconciliation" && err == nil {
					t.Fatalf("early reset = %v", err)
				}
				reset := rig.parent(t, before.OperationID)
				if reset.Phase != channelonboarding.PhasePreparing || reset.BindingRevision != bound.BindingRevision || !reflect.DeepEqual(reset.CredentialReservations, before.CredentialReservations) || reset.RequestHash != before.RequestHash || reset.SlotKey != before.SlotKey {
					t.Fatalf("reset discarded inherited responsibility: %#v", reset)
				}
				if value, found, err := rig.file.Get(ctx, signingKey); err != nil || !found || value != "observed-signing-value" {
					t.Fatalf("reset removed observed value: %q %t %v", value, found, err)
				}
				activations.preflightErr = nil
				fresh, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: reset.OperationID, ProviderCredential: "next-token"})
				if err != nil || fresh.IdentityOperation == nil || fresh.IdentityOperation.Kind != operatorchannel.OperationReconnect || fresh.IdentityOperation.ExpectedBindingRevision != bound.BindingRevision {
					t.Fatalf("inherited retry ceased being reconnect: %#v %v", fresh.IdentityOperation, err)
				}
			})
		}
	}
}

func checkpointEarlyOnboardingAdmissions(t *testing.T, rig *boundHandoffRig) (channelonboarding.Operation, channelonboarding.Candidate) {
	t.Helper()
	ctx := context.Background()
	known, err := rig.fixture.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	ids := map[string]bool{}
	for _, op := range known {
		ids[op.OperationID] = true
	}
	stop := errors.New("stop after credential writes")
	rig.testBarrier = func(boundary channelonboarding.TestLifecycleBoundary) error {
		if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
			return stop
		}
		return nil
	}
	if _, err := rig.service.Start(ctx, channelonboarding.StartInput{Verb: channelonboarding.VerbConnect, Selection: channelonboarding.CandidateSelection{Provider: "telegram"}, ProviderCredential: "original-token"}); !errors.Is(err, stop) {
		t.Fatalf("credential write boundary = %v", err)
	}
	rig.testBarrier = nil
	ops, err := rig.fixture.store.ListChannelOnboardingOperations(ctx)
	if err != nil {
		t.Fatal(err)
	}
	var op channelonboarding.Operation
	for _, row := range ops {
		if !ids[row.OperationID] {
			if op.OperationID != "" {
				t.Fatal("unexpected additional responsibility")
			}
			op = row
		}
	}
	if op.OperationID == "" || op.Phase != channelonboarding.PhasePreparing {
		t.Fatalf("preparing responsibility = %#v", op)
	}
	candidate := getEarlyResetCandidate(t, rig, op)
	return checkpointEarlyWrittenAdmissions(t, rig, op), candidate
}

func getEarlyResetCandidate(t *testing.T, rig *boundHandoffRig, op channelonboarding.Operation) channelonboarding.Candidate {
	t.Helper()
	result, err := rig.service.Get(context.Background(), op.OperationID)
	if err != nil || result.Candidate == nil {
		t.Fatalf("candidate = %#v %v", result, err)
	}
	return *result.Candidate
}

func checkpointEarlyWrittenAdmissions(t *testing.T, rig *boundHandoffRig, op channelonboarding.Operation) channelonboarding.Operation {
	t.Helper()
	ctx := context.Background()
	writer, err := channelonboarding.NewCredentialWriter(rig.file)
	if err != nil {
		t.Fatal(err)
	}
	var admissions []channelonboarding.CredentialAdmission
	for _, reservation := range op.CredentialReservations {
		key := reservation.StoreKey + ".operation." + operatorchannel.Hash("channel-onboarding-credential-occurrence-v1", op.OperationID, reservation.Role)
		receipt := operatorchannel.Hash("channel-onboarding-credential-receipt-v1", op.OperationID, reservation.Role)
		written, found, err := writer.ObserveWritten(ctx, key, receipt)
		if err != nil {
			t.Fatalf("actual written admission = %#v %t %v", written, found, err)
		}
		if found {
			admissions = append(admissions, channelonboarding.CredentialAdmission{Role: reservation.Role, StoreKey: key, Kind: channelonboarding.CredentialAdmissionWritten, Receipt: receipt, ValueSeal: written.ValueSeal})
		} else {
			observed, present, err := writer.ObserveOptional(ctx, reservation.StoreKey)
			if err != nil || !present {
				t.Fatalf("actual observed admission = %#v %t %v", observed, present, err)
			}
			admissions = append(admissions, channelonboarding.CredentialAdmission{Role: reservation.Role, StoreKey: reservation.StoreKey, Kind: channelonboarding.CredentialAdmissionObserved, ValueSeal: observed.ValueSeal})
		}
	}
	op, err = rig.fixture.store.AdvanceChannelOnboarding(ctx, channelonboarding.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: channelonboarding.PhaseCredentialsAdmitted, CredentialAdmissions: admissions, ReplaceCredentialAdmissions: true, Now: rig.now})
	if err != nil {
		t.Fatal(err)
	}
	return op
}

func executeEarlyResetEntry(ctx context.Context, service *channelonboarding.Service, operationID, entry string) error {
	if entry == "local_reconciliation" {
		return service.ReconcileLocal(ctx)
	}
	_, err := service.Retry(ctx, channelonboarding.RetryInput{OperationID: operationID})
	return err
}

func newEarlyResetService(t *testing.T, rig *boundHandoffRig, store channelonboarding.Store, file runtimecredentials.Store, candidate channelonboarding.Candidate, activations *earlyResetActivations) *channelonboarding.Service {
	t.Helper()
	writer, err := channelonboarding.NewCredentialWriter(file)
	if err != nil {
		t.Fatal(err)
	}
	catalog, err := channelonboarding.NewCandidateCatalog([]channelonboarding.Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	service, err := channelonboarding.NewService(channelonboarding.ServiceOptions{Store: store, Identities: rig.identities, Credentials: writer, Catalog: func() (*channelonboarding.CandidateCatalog, error) { return catalog, nil }, Activations: activations, Confirmation: handoffConfirmation{}, Readiness: retiredOnboardingReadiness{}, Now: func() time.Time { return rig.now }})
	if err != nil {
		t.Fatal(err)
	}
	return service
}

type earlyResetCredentialFile struct {
	*runtimecredentials.FileStore
	failAt        int
	calls         int
	err           error
	afterDelete   func()
	afterSnapshot func()
	scopeKeys     map[string]bool
}

func (s *earlyResetCredentialFile) Snapshot(ctx context.Context, key string) (runtimecredentials.AtomicSnapshot, error) {
	snapshot, err := s.FileStore.Snapshot(ctx, key)
	if err == nil && s.scopeKeys[key] && s.afterSnapshot != nil {
		after := s.afterSnapshot
		s.afterSnapshot = nil
		after()
	}
	return snapshot, err
}

func (s *earlyResetCredentialFile) DeleteWithReceiptAndSeal(ctx context.Context, key, receipt string, seal runtimecredentials.ValueSeal) (bool, error) {
	if s.scopeKeys != nil && !s.scopeKeys[key] {
		return s.FileStore.DeleteWithReceiptAndSeal(ctx, key, receipt, seal)
	}
	s.calls++
	if s.calls == s.failAt {
		return false, s.err
	}
	deleted, err := s.FileStore.DeleteWithReceiptAndSeal(ctx, key, receipt, seal)
	if s.afterDelete != nil {
		after := s.afterDelete
		s.afterDelete = nil
		after()
	}
	return deleted, err
}

type earlyResetStore struct {
	channelonboarding.Store
	commitFault string
	err         error
}

func (s *earlyResetStore) AdvanceChannelOnboarding(ctx context.Context, req channelonboarding.AdvanceRequest) (channelonboarding.Operation, error) {
	if req.Phase == channelonboarding.PhasePreparing && req.ReplaceCredentialAdmissions && s.commitFault != "" {
		fault := s.commitFault
		s.commitFault = ""
		if fault == "before_reset_commit" {
			return channelonboarding.Operation{}, s.err
		}
		_, err := s.Store.AdvanceChannelOnboarding(ctx, req)
		return channelonboarding.Operation{}, errors.Join(err, s.err)
	}
	return s.Store.AdvanceChannelOnboarding(ctx, req)
}

type earlyResetActivations struct {
	retiredOnboardingActivations
	preflightErr error
	refreshes    int
	publications int
	promotions   int
}

func (a *earlyResetActivations) PreflightChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.Candidate) error {
	return a.preflightErr
}

func (a *earlyResetActivations) RefreshChannelActivationCandidates(context.Context) error {
	a.refreshes++
	return nil
}

func (a *earlyResetActivations) PublishChannelActivation(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	a.publications++
	return fmt.Errorf("unexpected publication")
}

func (a *earlyResetActivations) PromoteChannelRegistration(context.Context, channelonboarding.Operation, channelonboarding.ConnectedChannelActivation) error {
	a.promotions++
	return fmt.Errorf("unexpected promotion")
}
