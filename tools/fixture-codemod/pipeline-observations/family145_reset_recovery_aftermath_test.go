package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"reflect"
	"strings"
	"testing"
)

func TestFamily145ResetRecoveryAftermathStoragePreservesEveryPhysicalQuery(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	original := map[string]string{}
	for _, row := range rows {
		original[row.Function] = row.Before
	}
	for _, port := range []struct {
		file, owner, predecessor string
		query                    int
	}{
		{"eventrecord/template_storage_observation.go", "ReadPostgresRecoveryFailedPayloadStorageTx", "TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset", 1},
		{"eventrecord/template_storage_observation.go", "ReadPostgresResetPayloadStorageTx", "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", 1},
		{"llmpersistence/session_storage_observation.go", "ReadPostgresSessionTerminationStorageTx", "TestResetOrphanedSessionAftermathSurface_RoundTripsThroughObservabilityReader", 0},
	} {
		owner := family145ConversationForkOwnerSource(t, "internal/backend/"+port.file, port.owner)
		old, got := family145ConnectorSQL(t, "package proof\n"+original[port.predecessor]), family145ConnectorSQL(t, "package proof\n"+owner)
		if len(old) != 2 || len(got) != 1 || !reflect.DeepEqual(got, old[port.query:port.query+1]) {
			t.Fatalf("aftermath physical statement changed columns, NULL behavior, scope or order: %s", port.owner)
		}
	}
	count := family145ConversationForkOwnerSource(t, "internal/backend/eventrecord/run_name_cardinality.go", "CountGlobalNamedEventStorageTx")
	want := family145ConnectorSQL(t, "package proof\n"+original["TestStartupRecoveryFailurePlatformEventSurface_PreservesRecoveryFailedWithoutPlatformReset"])[0]
	got := family145ConnectorSQL(t, "package proof\n"+count)
	if len(got) != 1 || strings.Replace(got[0], "$1", "'platform.reset'", 1) != want {
		t.Fatal("existing exact event-name count changed original whole-store physical scope")
	}
	family145ResetCountBridge(t, "internal/runtimepersistence/test_event_support.go", "CountEventNameStorageForTest", "eventrecord.CountGlobalNamedEventStorageTx", "eventName")
	old, err := os.ReadFile("../../../internal/store/internal/runtimepersistence/test_api_event_storage.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145ResetEventCountRetiredLocation(old); err != nil {
		t.Fatal(err)
	}
	if err := family145ResetEventCountRetiredLocation(append(append([]byte(nil), old...), []byte("\nfunc CountEventNameStorageForTest() {}\n")...)); err == nil {
		t.Fatal("moved event-name count survived at its retired location")
	}
}

func family145ResetEventCountRetiredLocation(raw []byte) error {
	file, err := parser.ParseFile(token.NewFileSet(), "test_api_event_storage.go", raw, parser.AllErrors)
	if err != nil {
		return err
	}
	for _, declaration := range file.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && fn.Name.Name == "CountEventNameStorageForTest" {
			return fmt.Errorf("moved event-name count survived at the old owner location")
		}
	}
	return nil
}

func TestFamily145ResetRecoveryAftermathBridgesKeepOriginalCoordinatorAndNoPublicQueryAuthority(t *testing.T) {
	for _, port := range []struct{ file, bridge, owner, identity string }{
		{"test_event_support.go", "ReadPostgresRecoveryFailedPayloadStorageForTest", "eventrecord.ReadPostgresRecoveryFailedPayloadStorageTx", ""},
		{"test_event_support.go", "ReadPostgresResetPayloadStorageForTest", "eventrecord.ReadPostgresResetPayloadStorageTx", ""},
		{"test_session_termination_storage.go", "ReadPostgresSessionTerminationStorageForTest", "llmpersistence.ReadPostgresSessionTerminationStorageTx(ctx, tx, sessionID)", ", sessionID"},
	} {
		bridge := family145ConversationForkOwnerSource(t, "internal/runtimepersistence/"+port.file, port.bridge)
		for _, cut := range []string{"validatePostgresPublicationIdentities(selected" + port.identity + ")", "readFanOutSemanticStorage(ctx, selected, nil,", port.owner} {
			if !strings.Contains(bridge, cut) {
				t.Fatalf("aftermath bridge lost original native reader or bounded identity: %s/%s", port.bridge, cut)
			}
		}
		public := family145ConversationForkOwnerSource(t, "storetest/reset_recovery_aftermath_storage.go", strings.TrimSuffix(port.bridge, "ForTest"))
		if !strings.Contains(public, "private."+port.bridge+"(ctx, selected"+port.identity+")") || strings.Contains(public, "sql.") {
			t.Fatalf("aftermath public port gained raw query/callback authority: %s", port.bridge)
		}
	}
}
