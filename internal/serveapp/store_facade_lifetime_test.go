package serveapp

import (
	"context"
	"database/sql"
	"testing"
	"time"

	worklifetime "github.com/division-sh/swarm/internal/runtime/core/worklifetime"
)

func TestSelectedStoreOwnerRequiresExactProcessJoinBeforeActivatedClose(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/runtime.db")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	owner := newSelectedStoreOwner(storeBundle{SQLDB: db}.facade())
	process := worklifetime.NewProcess()
	if err := owner.Activate(process); err != nil {
		t.Fatalf("activate selected store: %v", err)
	}
	if err := owner.CloseUnactivated(); err == nil {
		t.Fatal("CloseUnactivated succeeded after selected-store activation")
	}
	if err := owner.CloseActivated(nil); err == nil {
		t.Fatal("CloseActivated succeeded without a process join receipt")
	}

	foreign := worklifetime.NewProcess()
	foreignReceipt, err := foreign.Join(context.Background())
	if err != nil {
		t.Fatalf("join foreign process: %v", err)
	}
	if err := owner.CloseActivated(foreignReceipt); err == nil {
		t.Fatal("CloseActivated accepted a foreign process join receipt")
	}

	lease, err := process.Begin(context.Background())
	if err != nil {
		t.Fatalf("begin delayed store work: %v", err)
	}
	joinCtx, cancel := context.WithTimeout(context.Background(), 10*time.Millisecond)
	defer cancel()
	if _, err := process.Join(joinCtx); err == nil {
		t.Fatal("process join succeeded while delayed store work remained active")
	}
	if err := db.PingContext(context.Background()); err != nil {
		t.Fatalf("selected store closed before process join: %v", err)
	}
	if err := lease.Done(); err != nil {
		t.Fatalf("settle delayed store work: %v", err)
	}
	receipt, err := process.Join(context.Background())
	if err != nil {
		t.Fatalf("join selected-store process: %v", err)
	}
	if err := owner.CloseActivated(receipt); err != nil {
		t.Fatalf("close activated selected store: %v", err)
	}
	if err := db.PingContext(context.Background()); err == nil {
		t.Fatal("selected store remained usable after exact joined close")
	}
}

func TestSelectedStoreOwnerClosesUnactivatedConstruction(t *testing.T) {
	db, err := sql.Open("sqlite", t.TempDir()+"/runtime.db")
	if err != nil {
		t.Fatalf("open sqlite: %v", err)
	}
	owner := newSelectedStoreOwner(storeBundle{SQLDB: db}.facade())
	if err := owner.CloseUnactivated(); err != nil {
		t.Fatalf("close unactivated selected store: %v", err)
	}
	if err := db.PingContext(context.Background()); err == nil {
		t.Fatal("unactivated selected store remained usable after close")
	}
}
