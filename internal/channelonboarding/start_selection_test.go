package channelonboarding

import (
	"context"
	"errors"
	"fmt"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/operatorchannel"
	"github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestOnboardingSelectionHasNoCredentialOrExecutionEffects(t *testing.T) {
	service, store, activations, identities, values, candidate := newStartSelectionFixture(t)
	input := StartInput{Verb: VerbReconnect, Selection: CandidateSelection{Provider: candidate.Provider}, SaveProof: true}
	selected, err := service.Select(context.Background(), input)
	if err != nil {
		t.Fatal(err)
	}
	role, err := selected.ProviderCredentialRequirement()
	if err != nil || role != candidate.ProviderCredentialRole || selected.SelectionFence == "" {
		t.Fatal("selection lost the canonical requirement", selected, err)
	}
	if store.op.OperationID != "" || activations.preflights != 0 || activations.refreshes != 0 || identities.beginCalls != 0 {
		t.Fatal("non-executing selection admitted work", store.op, activations, identities)
	}
	old, present, err := values.Get(context.Background(), credentialReservations(candidate)[0].StoreKey)
	if err != nil || !present || old != "old-usable-token" {
		t.Fatal("selection mutated the retained credential", present, err)
	}
	cut := errors.New("stop after replacement admission")
	service.testBarrier = func(boundary TestLifecycleBoundary, _ string) error {
		if boundary == TestAfterCredentialWriteBeforeCheckpoint {
			return cut
		}
		return nil
	}
	input.ExpectedSelection, input.ProviderCredential = selected.SelectionFence, "replacement-token"
	if _, err := service.Start(context.Background(), input); !errors.Is(err, cut) {
		t.Fatal("replacement did not reach its original credential owner", err)
	}
	reservation := store.op.CredentialReservations[0]
	key := operationCredentialStoreKey(reservation.StoreKey, store.op.OperationID, reservation.Role)
	value, present, err := values.Get(context.Background(), key)
	if err != nil || !present || value != "replacement-token" || activations.preflights != 0 || identities.beginCalls != 0 {
		t.Fatal("replacement adopted old bytes or executed before admission", present, err, activations, identities)
	}
	old, present, err = values.Get(context.Background(), reservation.StoreKey)
	if err != nil || !present || old != "old-usable-token" {
		t.Fatal("replacement overwrote the predecessor", present, err)
	}
	// Deployment accounts reuse the operation receipt key, never the shared
	// credential fallback available to an explicitly selected runtime slot.
	candidate = Candidate{DeploymentRegistration: true, Provider: candidate.Provider, Interface: candidate.Interface,
		Posture: ActivationWebhookRegistration, Ceremony: CeremonyAccountLink, ProviderCredentialRole: candidate.ProviderCredentialRole,
		SigningCredentialRole: candidate.SigningCredentialRole}
	if err := candidate.ValidateDeclaration(); err != nil {
		t.Fatal(err)
	}
	deployment := Operation{DeploymentRegistration: true, OperationID: uuid.NewString(), CredentialReservations: credentialReservations(candidate)}
	if err := values.Set(context.Background(), deployment.CredentialReservations[0].StoreKey, "other-account-token"); err != nil {
		t.Fatal(err)
	}
	service.deployment = true
	generated := 0
	service.secret = func() (string, error) {
		generated++
		return fmt.Sprintf("private-signing-secret-%d", generated), nil
	}
	if err := values.Set(context.Background(), deployment.CredentialReservations[1].StoreKey, "another-account-signing-secret"); err != nil {
		t.Fatal(err)
	}
	var required *CredentialRequiredError
	if _, err := service.admitCredentials(context.Background(), deployment, candidate, "", true); !errors.As(err, &required) {
		t.Fatal("deployment registration adopted the shared credential", err)
	}
	admissions, err := service.admitCredentials(context.Background(), deployment, candidate, "deployment-token", true)
	if err != nil || admissions[0].StoreKey != operationCredentialStoreKey(deployment.CredentialReservations[0].StoreKey, deployment.OperationID, candidate.ProviderCredentialRole) {
		t.Fatal("deployment registration lost its exact operation receipt", admissions, err)
	}
	if generated != 1 || len(admissions) != 2 || admissions[1].Kind != CredentialAdmissionWritten ||
		admissions[1].StoreKey != operationCredentialStoreKey(deployment.CredentialReservations[1].StoreKey, deployment.OperationID, candidate.SigningCredentialRole) {
		t.Fatal("fresh signing admission escaped its original registration operation")
	}
	deployment.CredentialAdmissions = admissions
	if _, err := ProviderCredentialAdmission(deployment, candidate.SigningCredentialRole); err == nil {
		t.Fatal("signing role selected as provider identity evidence")
	}
	proof, err := providerIdentityAuthority(deployment, candidate)
	if err != nil || proof.Credential.Key != admissions[0].StoreKey || proof.Credential.Seal != admissions[0].ValueSeal ||
		proof.Credential.Key == admissions[1].StoreKey {
		t.Fatal("signing admission expanded the provider identity proof", err)
	}
	replayed, err := service.admitCredentials(context.Background(), deployment, candidate, "different-retry-bytes", true)
	if err != nil || generated != 1 || len(replayed) != 2 || replayed[1] != admissions[1] {
		t.Fatal("credential checkpoint recovery generated or rotated a second signing key", err)
	}
	second := deployment
	second.OperationID, second.CredentialAdmissions = uuid.NewString(), nil
	other, err := service.admitCredentials(context.Background(), second, candidate, "second-account-token", true)
	if err != nil || generated != 2 || len(other) != 2 || other[1].StoreKey == admissions[1].StoreKey || other[1].ValueSeal == admissions[1].ValueSeal {
		t.Fatal("another account reused the first signing admission", err)
	}
	if err := values.Set(context.Background(), admissions[1].StoreKey, "foreign-replacement"); err != nil {
		t.Fatal(err)
	}
	if _, err := service.admitCredentials(context.Background(), deployment, candidate, "deployment-token", true); err == nil || generated != 2 {
		t.Fatal("foreign signing replacement was regenerated or adopted", err)
	}
}

func TestOnboardingSelectedRequirementRefusesChangesBeforeReservation(t *testing.T) {
	for _, cell := range []string{"context", "target", "role", "verb", "proof", "cancel", "ambiguous", "credential_input"} {
		t.Run(cell, func(t *testing.T) {
			service, store, activations, identities, _, candidate := newStartSelectionFixture(t)
			input := StartInput{Verb: VerbReconnect, Selection: CandidateSelection{Provider: candidate.Provider}, SaveProof: true}
			selected, err := service.Select(context.Background(), input)
			if err != nil {
				t.Fatal(err)
			}
			input.ExpectedSelection, input.ProviderCredential = selected.SelectionFence, "replacement-token"
			ctx := context.Background()
			switch cell {
			case "context":
				candidate.Coordinate.ContextPublicationGeneration++
			case "target":
				candidate.Target.FlowPath = "changed-flow"
				candidate.Target.Selector = "ingress:changed-flow:telegram"
			case "role":
				candidate.ProviderCredentialRole = "changed-role"
			case "verb":
				input.Verb = VerbRebind
			case "proof":
				input.SaveProof = false
			case "cancel":
				var cancel context.CancelFunc
				ctx, cancel = context.WithCancel(ctx)
				cancel()
			}
			candidates := []Candidate{candidate}
			if cell == "ambiguous" {
				other := candidate
				other.Coordinate.BundleHash = "bundle-v2:sha256:" + strings.Repeat("b", 64)
				candidates = append(candidates, other)
			}
			catalog, err := NewCandidateCatalog(candidates)
			if err != nil {
				t.Fatal(err)
			}
			service.catalog = func() (*CandidateCatalog, error) { return catalog, nil }
			if cell == "credential_input" {
				_, err = service.Select(ctx, input)
			} else {
				_, err = service.Start(ctx, input)
			}
			if err == nil || store.op.OperationID != "" || activations.preflights != 0 || identities.beginCalls != 0 {
				t.Fatal("changed selection gained reservation or effects", err, store.op, activations, identities)
			}
		})
	}
}

func TestOnboardingRequiredCredentialKeyConsumesCurrentAdmission(t *testing.T) {
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	prior := testSucceededOperation(candidate, time.Now().UTC())
	writer := testCredentialWriter(t)
	admissions := writeTestOperationCredentials(t, writer, prior, "prior")
	var provider CredentialAdmission
	for _, admission := range admissions {
		if admission.Role == candidate.ProviderCredentialRole {
			provider = admission
			if removed, err := writer.Release(context.Background(), admission); err != nil || !removed {
				t.Fatal(removed, err)
			}
		}
	}
	store := &cancellationTestStore{activation: testCurrentActivation(prior, admissions, time.Now().UTC())}
	service := &Service{store: store, credentials: writer}
	op := prior
	op.OperationID, op.Phase, op.CredentialAdmissions = uuid.NewString(), PhasePreparing, nil
	_, err := service.admitCredentials(context.Background(), op, candidate, "", true)
	var required *CredentialRequiredError
	if !errors.As(err, &required) || required.Role != provider.Role || required.StoreKey != provider.StoreKey {
		t.Fatal("missing current admission lost its exact effective key", required, err)
	}
	for _, reservation := range op.CredentialReservations {
		if reservation.Role == required.Role && reservation.StoreKey == required.StoreKey {
			t.Fatal("fixture failed to exercise distinct canonical keys")
		}
		if reservation.Role == required.Role {
			if err := writer.values.Set(context.Background(), reservation.StoreKey, "unsafe-base-fallback"); err != nil {
				t.Fatal(err)
			}
		}
	}
	_, err = service.admitCredentials(context.Background(), op, candidate, "", true)
	if !errors.As(err, &required) || required.StoreKey != provider.StoreKey {
		t.Fatal("missing current admission adopted base-key fallback", err)
	}
}

func newStartSelectionFixture(t *testing.T) (*Service, *cancellationTestStore, *cancellationTestActivations, *cancellationTestIdentities, *credentials.FileStore, Candidate) {
	t.Helper()
	candidate := testCandidate(strings.Repeat("a", 64), "support")
	catalog, err := NewCandidateCatalog([]Candidate{candidate})
	if err != nil {
		t.Fatal(err)
	}
	values, err := credentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := values.Set(context.Background(), credentialReservations(candidate)[0].StoreKey, "old-usable-token"); err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(values)
	if err != nil {
		t.Fatal(err)
	}
	store := &cancellationTestStore{}
	activations := &cancellationTestActivations{}
	identities := &cancellationTestIdentities{binding: operatorchannel.Binding{Status: operatorchannel.BindingCurrent, Revision: 1}}
	service, err := NewService(ServiceOptions{Store: store, Identities: identities, Credentials: writer,
		Catalog: func() (*CandidateCatalog, error) { return catalog, nil }, Activations: activations,
		Confirmation: cancellationTestConfirmation{}, Readiness: cancellationTestReadiness{}, Now: time.Now})
	if err != nil {
		t.Fatal(err)
	}
	return service, store, activations, identities, values, candidate
}
