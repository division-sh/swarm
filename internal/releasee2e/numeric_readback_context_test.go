package releasee2e

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"sync/atomic"
	"testing"
	"time"
)

func TestNumericPostSettlementReadbackContextOwnership(t *testing.T) {
	if goldenRunDeadline != 90*time.Second || numericPostSettlementReadbackDeadline != 60*time.Second {
		t.Fatal("numeric proof changed the approved active/readback bounds")
	}
	active, activeCancel := context.WithTimeout(context.Background(), goldenRunDeadline)
	defer activeCancel()
	started := time.Now()
	readback, cancel, err := newNumericPostSettlementReadbackContext(active)
	if err != nil {
		t.Fatal(err)
	}
	defer cancel()
	deadline, ok := readback.Deadline()
	if !ok || deadline.Before(started.Add(numericPostSettlementReadbackDeadline)) || deadline.After(time.Now().Add(numericPostSettlementReadbackDeadline)) {
		t.Fatalf("readback did not receive its exact bounded deadline: %v %v", deadline, ok)
	}
	activeCancel()
	if err := readback.Err(); err != nil {
		t.Fatalf("settled readback still inherits the completed active lifetime: %v", err)
	}
	if _, _, err := newNumericPostSettlementReadbackContext(active); !errors.Is(err, context.Canceled) {
		t.Fatalf("cancelled active work was given a fresh readback budget: %v", err)
	}
	expiredActive, expireActive := context.WithDeadline(context.Background(), time.Unix(1, 0))
	defer expireActive()
	if _, _, err := newNumericPostSettlementReadbackContext(expiredActive); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("timed-out active work was given a fresh readback budget: %v", err)
	}

	var requests atomic.Int32
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		requests.Add(1)
		w.Header().Set("Content-Type", "application/json")
		_, _ = w.Write([]byte(`{"jsonrpc":"2.0","id":"run.fan_out.list","result":{}}`))
	}))
	defer server.Close()
	expiredReadback, expireReadback := context.WithDeadline(readback, time.Unix(1, 0))
	defer expireReadback()
	rpc := &releaseRPCClient{endpoint: server.URL, client: server.Client()}
	if _, err := readNumericFeed(expiredReadback, rpc, "00000000-0000-4000-8000-000000000001"); !errors.Is(err, context.DeadlineExceeded) {
		t.Fatalf("expired readback did not fail the actual public consumer: %v", err)
	}
	if got := requests.Load(); got != 0 {
		t.Fatalf("expired readback dispatched %d public requests", got)
	}
}
