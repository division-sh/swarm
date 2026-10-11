package channelonboarding

import (
	"bytes"
	"context"
	"path/filepath"
	"reflect"
	"testing"
	"time"

	domain "github.com/division-sh/swarm/internal/channelonboarding"
	"github.com/division-sh/swarm/internal/operatorchannel"
	render "github.com/division-sh/swarm/internal/runtime/channeldelivery"
	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
	"github.com/google/uuid"
)

func TestActivationAdmissionSerializationPreservesFrozenEvidence(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Set(context.Background(), "channel.token", "fixture-provider-secret"); err != nil {
		t.Fatal(err)
	}
	evidence, err := runtimecredentials.SealCurrentValue(context.Background(), store, "channel.token")
	if err != nil {
		t.Fatal(err)
	}
	observed := domain.CredentialAdmission{Role: "bot_token", StoreKey: "channel.token",
		Kind: domain.CredentialAdmissionObserved, ValueSeal: evidence.Seal}
	written := observed
	written.Kind, written.Receipt = domain.CredentialAdmissionWritten, uuid.NewString()
	account := operatorchannel.SessionAccountAdmission{Provider: "whatsapp", ConnectionID: uuid.NewString(),
		AccountRef: "15551234567@s.whatsapp.net", AdmissionID: uuid.NewString(), Revision: 1}
	for _, test := range []struct {
		name       string
		admissions []domain.CredentialAdmission
		account    operatorchannel.SessionAccountAdmission
	}{
		{name: "observed_credential_session_null", admissions: []domain.CredentialAdmission{observed}},
		{name: "written_credential_session_null", admissions: []domain.CredentialAdmission{written}},
		{name: "session", account: account},
	} {
		t.Run(test.name, func(t *testing.T) {
			frozen := append([]domain.CredentialAdmission(nil), test.admissions...)
			wantCredentials, err := marshalCredentialAdmissions(test.admissions)
			if err != nil {
				t.Fatal(err)
			}
			wantSession, err := marshalSessionAccount(test.account)
			if err != nil {
				t.Fatal(err)
			}
			credentials, session, err := marshalActivationAdmissions(test.admissions, test.account)
			if err != nil || !bytes.Equal(credentials, wantCredentials) || session != wantSession {
				t.Fatalf("serialized activation admissions differ: %v", err)
			}
			if !reflect.DeepEqual(test.admissions, frozen) {
				t.Fatal("serialization changed frozen credential evidence")
			}
		})
	}
}

func TestActivationPublicationInsertConflictPreservesFrozenEvidenceBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			r, principal, _ := publicationLockFixture(t, backend)
			signal := &render.ReconcileSignal{}
			subscription, err := signal.Subscribe(context.Background())
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(subscription.Close)
			switch selected := r.(type) {
			case postgresRunner:
				err = selected.owner.BindChannelReconciliation(signal)
			case sqliteRunner:
				err = selected.owner.BindChannelReconciliation(signal)
			default:
				t.Fatal("publication fixture has no original selected owner")
			}
			if err != nil {
				t.Fatal(err)
			}
			ctx, now := context.Background(), time.Now().UTC()
			predecessor := preparePublication(t, r, principal, domain.VerbConnect, now)
			op, previous, err := publishActivation(ctx, r, publicationRequest(predecessor, now))
			if err != nil {
				t.Fatal(err)
			}
			for _, phase := range []domain.Phase{domain.PhasePromotingRegistration, domain.PhaseRetiringPredecessor, domain.PhaseDeliveringConfirmation, domain.PhaseSucceeded} {
				op, err = advance(ctx, r, domain.AdvanceRequest{OperationID: op.OperationID, ExpectedRevision: op.Revision, Phase: phase, Now: now})
				if err != nil {
					t.Fatal(err)
				}
			}
			successor := preparePublication(t, r, principal, domain.VerbReconnect, now.Add(time.Minute))
			before, valid := signal.Mark()
			if !valid {
				t.Fatal("initial publication has no acknowledged notification")
			}
			request := publicationRequest(successor, now.Add(time.Minute))
			request.ActivationID = previous.ActivationID
			if _, _, err := publishActivation(ctx, r, request); err == nil {
				t.Fatal("duplicate activation INSERT unexpectedly committed")
			}
			retained, err := getActivation(ctx, r, previous.SlotKey)
			if err != nil || !reflect.DeepEqual(retained, previous) {
				t.Fatalf("failed INSERT changed predecessor activation/evidence: %v", err)
			}
			pending, err := getOperation(ctx, r, successor.OperationID)
			if err != nil || !reflect.DeepEqual(pending, successor) {
				t.Fatalf("failed INSERT changed pending operation/evidence: %v", err)
			}
			after, valid := signal.Mark()
			if !valid || after != before {
				t.Fatal("rolled-back publication emitted an acknowledged notification")
			}
			request.ActivationID = uuid.NewString()
			_, current, err := publishActivation(ctx, r, request)
			if err != nil || !reflect.DeepEqual(current.CredentialAdmissions, successor.CredentialAdmissions) || current.SessionAccount != successor.SessionAccount {
				t.Fatalf("fresh publication lost frozen evidence after rollback: %v", err)
			}
			published, valid := signal.Mark()
			if !valid || published == before {
				t.Fatal("acknowledged publication omitted its notification")
			}
		})
	}
}

func TestActivationAdmissionSerializationRetainsRefusalOrder(t *testing.T) {
	invalidCredentials := []domain.CredentialAdmission{{}}
	invalidSession := operatorchannel.SessionAccountAdmission{Provider: "whatsapp"}
	for _, test := range []struct {
		name       string
		admissions []domain.CredentialAdmission
		account    operatorchannel.SessionAccountAdmission
	}{
		{name: "credential", admissions: invalidCredentials},
		{name: "session", account: invalidSession},
		{name: "credential_before_session", admissions: invalidCredentials, account: invalidSession},
	} {
		t.Run(test.name, func(t *testing.T) {
			_, wantErr := marshalCredentialAdmissions(test.admissions)
			if wantErr == nil {
				_, wantErr = marshalSessionAccount(test.account)
			}
			credentials, session, err := marshalActivationAdmissions(test.admissions, test.account)
			if err == nil || wantErr == nil || err.Error() != wantErr.Error() || credentials != nil || session != nil {
				t.Fatalf("activation refusal lost original precedence or exposed partial evidence: %v", err)
			}
		})
	}
}
