package testutil

import (
	"context"
	"testing"
)

func TestEmptyPostgresStoreRemainsUnbootstrapped(t *testing.T) {
	location, store := StartEmptyPostgresStore(t)
	if location == "" {
		t.Fatal("empty sandbox lost its location")
	}
	if count, err := store.PublicTableCount(context.Background()); err != nil || count != 0 {
		t.Fatalf("empty construction bootstrapped the schema: count=%d err=%v", count, err)
	}
	if err := store.ResetPublicSchema(context.Background()); err != nil {
		t.Fatal(err)
	}
	if count, err := store.PublicTableCount(context.Background()); err != nil || count != 0 {
		t.Fatalf("empty reset bootstrapped the schema: count=%d err=%v", count, err)
	}
	var foreign EmptyPostgresStore
	if _, err := foreign.PublicTableCount(context.Background()); err == nil {
		t.Fatal("zero fixture supplied empty-store evidence")
	}
	if err := foreign.ResetPublicSchema(context.Background()); err == nil {
		t.Fatal("zero fixture reset a store")
	}
}
