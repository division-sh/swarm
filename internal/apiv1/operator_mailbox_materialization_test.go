package apiv1

import (
	"context"
	"testing"

	storepkg "github.com/division-sh/swarm/internal/store"
	"github.com/division-sh/swarm/internal/store/storetest"
)

// The authored mailbox_write materializer/idempotency test is retired by #2307.
// These helpers remain shared by independent mailbox API tests. Real supported
// notify_human/ask_human producers are exercised by the served HITL proofs.
func newSQLiteMailboxMaterializationAPIStore(t *testing.T, ctx context.Context) *storepkg.SQLiteRuntimeStore {
	t.Helper()
	store := storetest.StartSQLiteRuntimeStoreWithContext(t, ctx)
	registerScopedAPITestCatalog(t, store, nil)
	return store
}

func requireTaggedNoticeProjection(t *testing.T, value any) map[string]any {
	t.Helper()
	projection := asMap(t, value)
	if projection["kind"] != "notice" {
		t.Fatalf("expected tagged notice, got %#v", projection)
	}
	return asMap(t, projection["notice"])
}
