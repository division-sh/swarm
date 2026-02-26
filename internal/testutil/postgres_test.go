package testutil

import (
	"context"
	"testing"
	"time"
)

func TestStartPostgres_Smoke(t *testing.T) {
	_, db, cleanup := StartPostgres(t)
	defer cleanup()
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Second)
	defer cancel()
	var one int
	if err := db.QueryRowContext(ctx, `SELECT 1`).Scan(&one); err != nil {
		t.Fatalf("query: %v", err)
	}
	if one != 1 {
		t.Fatalf("expected 1, got %d", one)
	}
}

