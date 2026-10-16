package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

func family145SourceChildrenAssertions(source string, protocolHelper bool) ([]string, error) {
	assertions, err := family145DescriptorBusinessAssertions(source)
	if !protocolHelper || err != nil {
		return assertions, err
	}
	var business []string
	for _, assertion := range assertions {
		if strings.Contains(assertion, " => t.Fatalf(") {
			business = append(business, assertion)
		}
	}
	return business, nil
}

func family145SourceChildrenProofError(function, before, after string) error {
	helper := function == "a2MultiUntilChildren"
	want, err := family145SourceChildrenAssertions(before, helper)
	if err != nil {
		return err
	}
	got, err := family145SourceChildrenAssertions(after, helper)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("source-child read changed original multiplicity/identity/business/refusal assertions: %s: %v", function, err)
	}
	cuts := []string{
		"publications, err := storetest.CountSourceEventPublications(ctx, selected.events, triggerID)",
		"outputID, err := storetest.ReadSourceEventPublicationIdentity(ctx, selected.events, triggerID)",
	}
	if helper {
		cuts = []string{
			"ids, err := storetest.ReadSourceNamedChildEventIDs(ctx, selected.events, parentID, name)",
			"if err != nil {\n\t\tt.Fatal(err)\n\t}",
			"for _, id := range ids {",
			"prepared, found, err := selected.events.LoadPreparedPublishEvent(ctx, id)",
			"children = append(children, prepared.Event.Event())",
			"return children",
		}
		for _, retired := range []string{"selected.db", "QueryContext", "rows.Next", "rows.Scan", "rows.Err", "rows.Close"} {
			if strings.Contains(after, retired) {
				return fmt.Errorf("source-child discovery retained escaped row protocol: %s", retired)
			}
		}
	}
	cursor := -1
	for _, cut := range cuts {
		at := strings.Index(after, cut)
		if at < 0 || at <= cursor || strings.Count(after, cut) != 1 {
			return fmt.Errorf("source-child read lost original scope, error, owner, ordering or destination: %s", cut)
		}
		cursor = at
	}
	return nil
}

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
