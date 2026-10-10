package storetest

import (
	"context"
	"errors"
	"testing"

	"github.com/google/uuid"
)

func TestSessionPublicationFixtureRequiresOriginalSelectedOwner(t *testing.T) {
	for _, selected := range []any{nil, struct{}{}} {
		if lock, err := HoldSessionPublicationOnboardingLock(context.Background(), selected, uuid.NewString()); err == nil || lock != nil {
			t.Fatalf("foreign lock owner admitted: %v %v", lock, err)
		}
		if err := SetSessionPublicationInsertFault(context.Background(), selected, true); err == nil {
			t.Fatal("foreign fault owner admitted")
		}
	}
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			var selected any
			if backend == "sqlite" {
				selected = StartSQLiteRuntimeStore(t)
			} else {
				selected = StartPostgresRuntimeStore(t)
			}
			for _, id := range []string{"", uuid.NewString()} {
				if lock, err := HoldSessionPublicationOnboardingLock(context.Background(), selected, id); err == nil || lock != nil {
					t.Fatalf("absent lock owner admitted: %v %v", lock, err)
				}
			}
			ctx, cancel := context.WithCancel(context.Background())
			cancel()
			if lock, err := HoldSessionPublicationOnboardingLock(ctx, selected, uuid.NewString()); !errors.Is(err, context.Canceled) || lock != nil {
				t.Fatalf("canceled lock owner admitted: %v %v", lock, err)
			}
			if err := SetSessionPublicationInsertFault(context.Background(), selected, true); err != nil {
				t.Fatal(err)
			}
			if err := SetSessionPublicationInsertFault(context.Background(), selected, false); err != nil {
				t.Fatal(err)
			}
		})
	}
}
