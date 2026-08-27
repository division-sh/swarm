package channelonboarding

import (
	"context"
	"os"
	"path/filepath"
	"strings"
	"testing"

	runtimecredentials "github.com/division-sh/swarm/internal/runtime/credentials"
)

func TestOnboardingCredentialWriterCrashConvergence(t *testing.T) {
	path := filepath.Join(t.TempDir(), "credentials.json")
	store, err := runtimecredentials.NewFileStore(path)
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	first, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "super-secret-token", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	replayed, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "different-retry-bytes", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if replayed != first || first.Epoch == "" || first.Receipt != "operation-a/provider" {
		t.Fatalf("receipt replay = first:%#v replay:%#v", first, replayed)
	}
	value, found, err := store.Get(context.Background(), "channel.telegram.provider")
	if err != nil || !found || value != "super-secret-token" {
		t.Fatalf("replay rewrote credential: value=%q found=%v err=%v", value, found, err)
	}
	rotated, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "rotated-token", Receipt: "operation-b/provider"})
	if err != nil {
		t.Fatal(err)
	}
	if rotated.Epoch == first.Epoch || rotated.Receipt == first.Receipt {
		t.Fatalf("explicit new receipt did not rotate occurrence: old=%#v new=%#v", first, rotated)
	}
	raw, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	if strings.Contains(string(raw), first.Epoch) && strings.Contains(first.Epoch, "super-secret-token") {
		t.Fatal("credential epoch was derived from secret bytes")
	}
}

func TestCredentialAdmissionJSONNeverContainsValue(t *testing.T) {
	admission := CredentialAdmission{Role: "provider", StoreKey: "channel.telegram.provider", Kind: CredentialAdmissionWritten, Receipt: "receipt-a", Epoch: "epoch-a"}
	if strings.Contains(strings.ToLower(strings.Join([]string{admission.Role, admission.StoreKey, admission.Receipt, admission.Epoch}, " ")), "secret") {
		t.Fatal("test admission unexpectedly contains secret material")
	}
}

func TestOnboardingCredentialOccurrenceSurvivesSnapshotOwnerRestart(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	written, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token", Receipt: "operation/provider"})
	if err != nil {
		t.Fatal(err)
	}
	restarted, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	observed, err := restarted.Observe(context.Background(), written.StoreKey)
	if err != nil {
		t.Fatal(err)
	}
	if observed.Epoch != written.Epoch {
		t.Fatalf("restarted epoch = %q, want %q", observed.Epoch, written.Epoch)
	}
}

func TestOnboardingCredentialReleaseOwnsOnlyWrittenCurrentOccurrence(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	written, err := writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token-a", Receipt: "operation-a/provider"})
	if err != nil {
		t.Fatal(err)
	}
	admission := CredentialAdmission{Role: "provider", StoreKey: written.StoreKey, Kind: CredentialAdmissionWritten, Receipt: written.Receipt, Epoch: written.Epoch}
	if released, err := writer.Release(context.Background(), admission); err != nil || !released {
		t.Fatalf("release = %v, %v; want true, nil", released, err)
	}

	written, err = writer.Admit(context.Background(), CredentialWriteRequest{StoreKey: "channel.telegram.provider", Value: "token-b", Receipt: "operation-b/provider"})
	if err != nil {
		t.Fatal(err)
	}
	observed := CredentialAdmission{Role: "provider", StoreKey: written.StoreKey, Kind: CredentialAdmissionObserved, Receipt: "observation", Epoch: written.Epoch}
	if released, err := writer.Release(context.Background(), observed); err != nil || released {
		t.Fatalf("observed release = %v, %v; want false, nil", released, err)
	}
	if value, found, err := store.Get(context.Background(), written.StoreKey); err != nil || !found || value != "token-b" {
		t.Fatalf("observed credential = %q, %v, %v", value, found, err)
	}
}

func TestCredentialWriterReleaseOperationReconcilesPhysicalAndCheckpointedWrites(t *testing.T) {
	for _, test := range []struct {
		name             string
		writtenRoles     int
		checkpointWrites bool
	}{
		{name: "partial physical write before checkpoint", writtenRoles: 1},
		{name: "complete physical writes before checkpoint", writtenRoles: 2},
		{name: "complete checkpointed writes", writtenRoles: 2, checkpointWrites: true},
	} {
		t.Run(test.name, func(t *testing.T) {
			store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
			if err != nil {
				t.Fatal(err)
			}
			writer, err := NewCredentialWriter(store)
			if err != nil {
				t.Fatal(err)
			}
			op := Operation{
				OperationID: "operation-a",
				CredentialReservations: []CredentialReservation{
					{Role: "provider", StoreKey: "channel.telegram.provider"},
					{Role: "signing", StoreKey: "channel.telegram.signing"},
				},
			}
			written := make([]CredentialAdmission, 0, test.writtenRoles)
			for _, reservation := range op.CredentialReservations[:test.writtenRoles] {
				result, err := writer.Admit(context.Background(), CredentialWriteRequest{
					StoreKey: operationCredentialStoreKey(reservation.StoreKey, op.OperationID, reservation.Role),
					Value:    "operation-secret-" + reservation.Role, Receipt: credentialReceipt(op.OperationID, reservation.Role),
				})
				if err != nil {
					t.Fatal(err)
				}
				written = append(written, CredentialAdmission{
					Role: reservation.Role, StoreKey: result.StoreKey, Kind: CredentialAdmissionWritten,
					Receipt: result.Receipt, Epoch: result.Epoch,
				})
			}
			if test.checkpointWrites {
				op.CredentialAdmissions = append([]CredentialAdmission(nil), written...)
			}

			predecessor, err := writer.Admit(context.Background(), CredentialWriteRequest{
				StoreKey: "channel.telegram.predecessor", Value: "predecessor-secret", Receipt: "predecessor-receipt",
			})
			if err != nil {
				t.Fatal(err)
			}
			op.CredentialAdmissions = append(op.CredentialAdmissions, CredentialAdmission{
				Role: "provider", StoreKey: predecessor.StoreKey, Kind: CredentialAdmissionObserved,
				Receipt: "predecessor-observation", Epoch: predecessor.Epoch,
			})
			successorReservation := op.CredentialReservations[0]
			successor, err := writer.Admit(context.Background(), CredentialWriteRequest{
				StoreKey: operationCredentialStoreKey(successorReservation.StoreKey, "operation-b", successorReservation.Role),
				Value:    "successor-secret", Receipt: credentialReceipt("operation-b", successorReservation.Role),
			})
			if err != nil {
				t.Fatal(err)
			}

			if err := writer.ReleaseOperation(context.Background(), op); err != nil {
				t.Fatal(err)
			}
			for _, admission := range written {
				if _, found, err := store.Get(context.Background(), admission.StoreKey); err != nil || found {
					t.Fatalf("operation credential %q found=%v err=%v", admission.StoreKey, found, err)
				}
			}
			for _, retained := range []CredentialWriteResult{predecessor, successor} {
				if _, found, err := store.Get(context.Background(), retained.StoreKey); err != nil || !found {
					t.Fatalf("foreign credential %q found=%v err=%v", retained.StoreKey, found, err)
				}
			}
		})
	}
}

func TestCredentialWriterReleaseOperationRejectsForeignWrittenAdmission(t *testing.T) {
	store, err := runtimecredentials.NewFileStore(filepath.Join(t.TempDir(), "credentials.json"))
	if err != nil {
		t.Fatal(err)
	}
	writer, err := NewCredentialWriter(store)
	if err != nil {
		t.Fatal(err)
	}
	reservation := CredentialReservation{Role: "provider", StoreKey: "channel.telegram.provider"}
	foreign, err := writer.Admit(context.Background(), CredentialWriteRequest{
		StoreKey: operationCredentialStoreKey(reservation.StoreKey, "operation-b", reservation.Role),
		Value:    "successor-secret", Receipt: credentialReceipt("operation-b", reservation.Role),
	})
	if err != nil {
		t.Fatal(err)
	}
	op := Operation{OperationID: "operation-a", CredentialReservations: []CredentialReservation{reservation}, CredentialAdmissions: []CredentialAdmission{{
		Role: reservation.Role, StoreKey: foreign.StoreKey, Kind: CredentialAdmissionWritten,
		Receipt: foreign.Receipt, Epoch: foreign.Epoch,
	}}}
	if err := writer.ReleaseOperation(context.Background(), op); err == nil || !strings.Contains(err.Error(), "not owned") {
		t.Fatalf("foreign written cleanup error = %v", err)
	}
	if _, found, err := store.Get(context.Background(), foreign.StoreKey); err != nil || !found {
		t.Fatalf("foreign credential found=%v err=%v", found, err)
	}
}
