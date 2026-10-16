package main

import (
	"strings"
	"testing"
)

func TestFamily145SourceChildOwnersRetainUnfilteredParentsAndNamespaceOrder(t *testing.T) {
	for _, tc := range []struct{ function, query, binding string }{
		{"CountSourceEventPublicationsTx", `SELECT COUNT(*) FROM events WHERE source_event_id=$1`, "sourceEventID).Scan(&count)"},
		{"ReadSourceEventPublicationIdentityTx", `SELECT event_id FROM events WHERE source_event_id=$1`, "sourceEventID).Scan(&id)"},
		{"ReadSourceNamedChildEventIDsTx", `SELECT CAST(event_id AS TEXT) FROM events WHERE source_event_id=$1 AND (event_name=$2 OR event_name LIKE $3) ORDER BY event_id`, `parentID, name, "%/"+name)`},
	} {
		physical := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", tc.function)
		queries := family145A2ClaimLiteralQueries(t, physical)
		if len(queries) != 1 || queries[0] != tc.query || strings.Count(physical, tc.binding) != 1 {
			t.Fatalf("%s changed original projection, parent/name scope, LIKE semantics, ordering or argument binding: %v", tc.function, queries)
		}
	}
	physical := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/template_storage_observation.go", "ReadSourceNamedChildEventIDsTx")
	for _, cut := range []string{"for rows.Next() {", "rows.Scan(&id)", "out = append(out, id)", "errors.Join(err, rows.Close())", "out = nil", "return out, rows.Err()"} {
		if strings.Count(physical, cut) != 1 {
			t.Fatalf("child discovery lost ordered scan or safe row/close failure handling: %s", cut)
		}
	}
}
