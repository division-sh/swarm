package main

import (
	"encoding/json"
	"fmt"
	"reflect"
	"strings"
	"testing"
)

var family145LiveSessionCuts = map[string][]string{
	"TestReusedLiveSessionKeepsDeliveryFrontierBoundToCanonicalSession": {
		"storetest.StartPostgresRuntimeStore(t)", "registerDifferentTestAuthorActivityCatalog(t, pg, \"review.requested\")",
		"requireCanonicalConversationSurface(t, ctx, pg)", "requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)",
		"storetest.RequireRun(t, ctx, pg,", "seedConformanceRunningAgent(t, ctx, pg, runID, \"agent-1\")",
		"newScopedTestEventBus(t, pg,", "pg.SetSessionLockTTL(30 * time.Second)",
		"conformanceProviderCredentialResolver(t, \"ANTHROPIC_API_KEY\", \"test-key\")",
		"http.DefaultTransport = previousTransport", "liveTestCompletionController(pg, pg, pg, discardCompletionSpendProjection{})",
		"managedConformanceExecutionContext(t, base, \"reused-live-session\")", "runtimedelivery.WithClaim(base, claim)",
		"storetest.ClaimDelivery(ctx, pg, event1, route)", "conversation.RunManaged(newTurnContext(event1),",
		"pg.SettleSuccess(newTurnContext(event1), firstClaim.Claim, nil, 0, runtimedelivery.NotApplicableHandlerRuleSelection())",
		"storetest.ClaimDelivery(ctx, pg, event2, route)", "conversation.RunManaged(newTurnContext(event2),",
		"storetest.ReadPostgresAgentOneDeliverySessionStorage(ctx, pg, event2.ID())",
		"deliveryStatus, activeSessionID := delivery.Status, delivery.ActiveSessionID",
		"storetest.ReadPostgresSessionFlowStorage(ctx, pg, session.ID)",
		"sessionStatus, sessionFlow := liveSession.Status, liveSession.FlowInstance",
		"storetest.CountPostgresAgentOneSupportLiveSessions(ctx, pg, runID)",
		"pg.ListAgentDeliveryLifecycleFacts(ctx, []agentidentity.Identity{agentIdentity})",
	},
	"TestCLISessionFailureDoesNotRotateFromStderrProse": {
		"storetest.StartPostgresRuntimeStore(t)", "registerDifferentTestAuthorActivityCatalog(t, pg, \"review.requested\")",
		"requireCanonicalConversationSurface(t, ctx, pg)", "requireCanonicalDeliveryLifecycleSurface(t, ctx, pg)",
		"storetest.RequireRun(t, ctx, pg,", "seedConformanceRunningAgent(t, ctx, pg, runID, \"agent-1\")",
		"os.WriteFile(fakeDocker, []byte(script), 0o755)", "t.Setenv(\"SWARM_FAKE_DOCKER_STATE\", dockerState)",
		"t.Setenv(\"CLAUDE_CODE_OAUTH_TOKEN\", \"stale-oauth-token\")", "pg.SetSessionLockTTL(30 * time.Second)",
		"runtimesessions.Registry(pg)", "newScopedTestEventBus(t, pg,",
		"conformanceProviderCredentialResolver(t, \"CLAUDE_CODE_OAUTH_TOKEN\", \"oauth-token\")",
		"liveTestCompletionController(pg, pg, pg, discardCompletionSpendProjection{})",
		"managedConformanceExecutionContext(t, base, \"cli-session-failure\")", "runtimedelivery.WithClaim(base, deliveryClaim)",
		"runtime.StartSession(newTurnContext(evt), \"agent-1\", conversation.SystemPrompt, nil)",
		"storetest.ClaimDelivery(ctx, pg, evt,", "conversation.RunManaged(newTurnContext(evt),",
		"runtimefailures.As(err)", "storetest.ReadPostgresAgentOneDeliverySessionStorage(ctx, pg, evt.ID())",
		"deliveryStatus, activeSessionID := delivery.Status, delivery.ActiveSessionID",
		"storetest.CountPostgresAgentOneSessions(ctx, pg)",
	},
}

func family145LiveSessionProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(want, got) {
		return fmt.Errorf("live-session changed an original business assertion or its order")
	}
	for _, name := range []string{"PersistedProjection", "CommitSemanticEventWithRoutes", "WithLifecycleToken", "WithExecution", "WithActor", "RunManaged", "SettleSuccess"} {
		if !reflect.DeepEqual(sourceOutcomePreparationCalls(before, name), sourceOutcomePreparationCalls(after, name)) {
			return fmt.Errorf("live-session changed original input, authority or managed execution: %s", name)
		}
	}
	for _, cut := range family145LiveSessionCuts[function] {
		if !strings.Contains(after, cut) {
			return fmt.Errorf("live-session lost the exact native owner, provider or execution cut: %s", cut)
		}
	}
	if strings.LastIndex(after, "conversation.RunManaged(") > strings.Index(after, "storetest.ReadPostgresAgentOneDeliverySessionStorage(") {
		return fmt.Errorf("session observation moved before the actual managed turn")
	}
	reads := []string{"ReadPostgresAgentOneDeliverySessionStorage", "ReadPostgresSessionFlowStorage", "CountPostgresAgentOneSupportLiveSessions"}
	if function == "TestCLISessionFailureDoesNotRotateFromStderrProse" {
		reads = []string{"ReadPostgresAgentOneDeliverySessionStorage", "CountPostgresAgentOneSessions"}
	}
	previous := -1
	for _, name := range reads {
		call := "storetest." + name + "("
		at := strings.Index(after, call)
		if at <= previous || strings.Count(after, call) != 1 {
			return fmt.Errorf("live-session moved, duplicated or dropped an original independent read: %s", name)
		}
		previous = at
	}
	return family145DescriptorReadinessEscape([]byte("package proof\n" + after))
}

func TestFamily145LiveSessionWholeGroupPreservesInputsClaimsProviderFailuresAndAssertions(t *testing.T) {
	var rows []struct{ Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	found := 0
	for _, row := range rows {
		if _, ok := family145LiveSessionCuts[row.Function]; !ok {
			continue
		}
		found++
		if err := family145LiveSessionProofError(row.Function, row.Before, row.After); err != nil {
			t.Fatalf("%s: %v", row.Function, err)
		}
		for _, cut := range family145LiveSessionCuts[row.Function] {
			if family145LiveSessionProofError(row.Function, row.Before, strings.ReplaceAll(row.After, cut, "foreignCut(")) == nil {
				t.Fatalf("lost live-session owner/cut accepted: %s", cut)
			}
		}
		assertFamily145LiveSessionHostileInputsAndAssertions(t, row.Function, row.Before, row.After)
	}
	if found != 2 {
		t.Fatalf("live-session whole caller inventory=%d want2", found)
	}
}

func assertFamily145LiveSessionHostileInputsAndAssertions(t *testing.T, function, before, after string) {
	t.Helper()
	assertions, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		t.Fatal(err)
	}
	for _, assertion := range assertions {
		hostile, err := family145BusAssertionCounterexample(after, strings.SplitN(assertion, " => ", 2)[0])
		if err != nil {
			t.Fatal(err)
		}
		if family145LiveSessionProofError(function, before, hostile) == nil {
			t.Fatalf("lost original session identity/failure/frontier assertion accepted: %s", assertion)
		}
	}
	for _, name := range []string{"PersistedProjection", "CommitSemanticEventWithRoutes", "RunManaged"} {
		calls := sourceOutcomePreparationCalls(after, name)
		if len(calls) == 0 {
			t.Fatalf("missing actual live-session input/execution inventory: %s", name)
		}
		if family145LiveSessionProofError(function, before, strings.Replace(after, calls[0], "foreignCall()", 1)) == nil {
			t.Fatalf("changed original live-session event/authority/managed input accepted: %s", name)
		}
	}
}

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
