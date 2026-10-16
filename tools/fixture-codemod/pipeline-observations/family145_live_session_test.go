package main

import (
	"encoding/json"
	"reflect"
	"strings"
	"testing"
)

func TestFamily145LiveSessionReadsKeepAllFiveOriginalPhysicalPredicatesAndBindings(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string]string{}
	for _, row := range rows {
		original[row.Function] = row.Before
	}
	for _, port := range []struct {
		file, owner, predecessor, argument, scan string
		query                                    int
	}{
		{"delivery/read_projections.go", "ReadPostgresAgentOneDeliverySessionStorageTx", "TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession", "eventID", "&out.Status, &out.ActiveSessionID", 0},
		{"delivery/read_projections.go", "ReadPostgresAgentOneDeliverySessionStorageTx", "TestCLISessionFailureDoesNotRotateFromStderrProse", "eventID", "&out.Status, &out.ActiveSessionID", 0},
		{"llmpersistence/session_storage_observation.go", "ReadPostgresSessionFlowStorageTx", "TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession", "sessionID", "&out.Status, &out.FlowInstance", 1},
		{"llmpersistence/session_storage_observation.go", "CountPostgresAgentOneSupportLiveSessionsTx", "TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession", "runID", "&count", 2},
		{"llmpersistence/session_storage_observation.go", "CountPostgresAgentOneSessionsTx", "TestCLISessionFailureDoesNotRotateFromStderrProse", "", "&count", 1},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/"+port.file, port.owner)
		old, got := family145ConnectorSQL(t, "package proof\n"+original[port.predecessor]), family145ConnectorSQL(t, "package proof\n"+owner)
		if port.query >= len(old) || len(got) != 1 || !reflect.DeepEqual(got, old[port.query:port.query+1]) {
			t.Fatalf("session statement changed join/NULL/version/name/status/flow/run/memory scope: %s", port.owner)
		}
		assertFamily145LiveSessionQueryBindings(t, owner, port.argument, port.scan)
	}
}

func assertFamily145LiveSessionQueryBindings(t *testing.T, owner, argument, scan string) {
	t.Helper()
	calls := sourceOutcomePreparationCalls(owner, "QueryRowContext")
	if len(calls) != 1 || (argument != "" && !strings.HasSuffix(calls[0], ", "+argument+")")) || !strings.Contains(owner, ".Scan("+scan+")") {
		t.Fatalf("session query lost its exact variadic binding or tuple mapping: %s", owner)
	}
	if argument != "" {
		hostile := strings.Replace(owner, ", "+argument+").Scan(", ").Scan(", 1)
		changed := sourceOutcomePreparationCalls(hostile, "QueryRowContext")
		if len(changed) != 1 || strings.HasSuffix(changed[0], ", "+argument+")") {
			t.Fatal("omitted SQL parameter counterexample did not lose the required binding")
		}
	}
}

func TestFamily145LiveSessionBridgesKeepBoundedOriginalSelectedReadAuthority(t *testing.T) {
	for _, port := range []struct{ bridge, owner, identity string }{
		{"ReadPostgresAgentOneDeliverySessionStorage", "delivery.ReadPostgresAgentOneDeliverySessionStorageTx(ctx, tx, eventID)", ", eventID"},
		{"ReadPostgresSessionFlowStorage", "llmpersistence.ReadPostgresSessionFlowStorageTx(ctx, tx, sessionID)", ", sessionID"},
		{"CountPostgresAgentOneSupportLiveSessions", "llmpersistence.CountPostgresAgentOneSupportLiveSessionsTx(ctx, tx, runID)", ", runID"},
		{"CountPostgresAgentOneSessions", "llmpersistence.CountPostgresAgentOneSessionsTx", ""},
	} {
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/test_live_session_storage.go", port.bridge+"ForTest")
		for _, cut := range []string{"validatePostgresPublicationIdentities(selected" + port.identity + ")", "readFanOutSemanticStorage(ctx, selected, nil,", port.owner} {
			if !strings.Contains(bridge, cut) {
				t.Fatalf("live-session bridge lost native coordinator, identity or fixed row owner: %s/%s", port.bridge, cut)
			}
		}
		public := family145ConversationForkOwnerSource(t, "storetest/live_session_storage.go", port.bridge)
		if !strings.Contains(public, "private."+port.bridge+"ForTest(ctx, selected"+port.identity+")") || strings.Contains(public, "sql.") {
			t.Fatalf("live-session public port gained raw handle/query/callback authority: %s", port.bridge)
		}
	}
}
