package runtimepersistence

import (
	"context"
	"errors"
	"reflect"
	"testing"

	"github.com/division-sh/swarm/internal/channelonboarding"
)

func TestChannelOnboardingReplacementSelectionBeforeEffectsBothStores(t *testing.T) {
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			rig := newBoundHandoffRig(t, openChannelOnboardingConfirmationFixture(t, backend))
			begun := rig.start(t, channelonboarding.VerbConnect, "old-usable-token", false)
			rig.confirm(t, begun, "same-operator")
			rig.complete(t, begun.Operation.OperationID)
			ctx := context.Background()
			before, err := rig.fixture.store.ListChannelOnboardingOperations(ctx)
			if err != nil {
				t.Fatal(err)
			}
			keysBefore, err := rig.file.List(ctx)
			if err != nil {
				t.Fatal(err)
			}
			input := channelonboarding.StartInput{Verb: channelonboarding.VerbReconnect,
				Selection: channelonboarding.CandidateSelection{Provider: "telegram"}}
			selected, err := rig.service.Select(ctx, input)
			if err != nil {
				t.Fatal(err)
			}
			after, err := rig.fixture.store.ListChannelOnboardingOperations(ctx)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("selection changed a live responsibility", err)
			}
			keysAfter, err := rig.file.List(ctx)
			if err != nil || !reflect.DeepEqual(keysBefore, keysAfter) {
				t.Fatal("selection touched credentials", err)
			}
			input.ExpectedSelection, input.ProviderCredential = selected.SelectionFence, "replacement-token"
			stale := input
			stale.SaveProof = true
			if _, err := rig.service.Start(ctx, stale); !errors.Is(err, channelonboarding.ErrConflict) {
				t.Fatal("changed selection gained admission", err)
			}
			after, err = rig.fixture.store.ListChannelOnboardingOperations(ctx)
			if err != nil || !reflect.DeepEqual(before, after) {
				t.Fatal("stale selection reserved work", err)
			}
			var cutOperation string
			rig.testBarrier = func(boundary channelonboarding.TestLifecycleBoundary) error {
				if boundary == channelonboarding.TestAfterCredentialWriteBeforeCheckpoint {
					return errHandoffPublicationStop
				}
				return nil
			}
			if _, err := rig.service.Start(ctx, input); !errors.Is(err, errHandoffPublicationStop) {
				t.Fatal("replacement missed original credential admission cut", err)
			}
			after, err = rig.fixture.store.ListChannelOnboardingOperations(ctx)
			if err != nil || len(after) != len(before)+1 {
				t.Fatal("replacement did not reserve exactly once", err)
			}
			for _, op := range after {
				if op.OperationID == begun.Operation.OperationID {
					if !reflect.DeepEqual(op, before[0]) {
						t.Fatal("replacement changed predecessor evidence")
					}
				} else {
					cutOperation = op.OperationID
					if op.Phase != channelonboarding.PhasePreparing || op.IdentityOperationID != "" || op.ActivationRevision != 0 {
						t.Fatal("replacement launched before credential checkpoint", op.OperationID, op.Phase)
					}
				}
			}
			if cutOperation == "" {
				t.Fatal("replacement responsibility missing")
			}
			keysAfter, err = rig.file.List(ctx)
			if err != nil || len(keysAfter) != len(keysBefore)+1 {
				t.Fatal("replacement did not create exactly its own credential occurrence", err)
			}
			for _, key := range keysAfter {
				old := false
				for _, previous := range keysBefore {
					old = old || previous == key
				}
				if old {
					continue
				}
				value, found, err := rig.file.Get(ctx, key)
				if err != nil || !found || value != "replacement-token" {
					t.Fatal("new credential occurrence adopted old bytes", err)
				}
			}
			for _, admission := range before[0].CredentialAdmissions {
				if admission.Role != "bot_token" {
					continue
				}
				value, found, err := rig.file.Get(ctx, admission.StoreKey)
				if err != nil || !found || value != "old-usable-token" {
					t.Fatal("replacement overwrote predecessor credential evidence", err)
				}
			}
		})
	}
}
