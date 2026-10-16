package main

import (
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

var family145OperatorChannelCalls = map[string][]string{
	"TestInboundGatewaySignedTelegramOperatorChannelClaimSelectedStoreParity": {"storetest.StartPostgresRuntimeStore(t)", "storetest.StartSQLiteRuntimeStore(t)", "runOperatorChannelInboundSupportedSurface(t, selected,"},
	"runOperatorChannelInboundSupportedSurface":                               {"seedSQLiteInboundGatewayRuntime(t, ctx, owner,", "seedPostgresInboundGatewayRuntime(t, ctx, owner,", "storetest.CountOperatorChannelActionIntentStorage(ctx, selected)", "storetest.ReadOperatorChannelTextIntentFactStorage(ctx, selected, \"7320\")", "publishOperatorChannelTelegramUpdate(t, gateway, bus, inboundTarget, ctx,", "after.Total.ReadCommits-before.Total.ReadCommits != 1", "after.Total.WriteCommits != before.Total.WriteCommits"},
}

func family145OperatorChannelProofError(function, before, after string) error {
	if err := family145ConnectorProofError(function, before, after); err != nil {
		return err
	}
	for _, call := range family145OperatorChannelCalls[function] {
		if !strings.Contains(after, call) {
			return fmt.Errorf("operator channel lost original owner/scope/lifetime: %s", call)
		}
	}
	return nil
}

func TestFamily145OperatorChannelReadOwnersKeepExactPhysicalScope(t *testing.T) {
	var rows []struct{ Function, Before string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	var original []string
	for _, row := range rows {
		if row.Function == "runOperatorChannelInboundSupportedSurface" {
			original = family145ConnectorSQL(t, "package proof\n"+row.Before)
		}
	}
	data, err := os.ReadFile(filepath.Join("..", "..", "..", "internal", "store", "internal", "backend", "operatorchannel", "storage_observation.go"))
	if err != nil {
		t.Fatal(err)
	}
	file, err := parser.ParseFile(token.NewFileSet(), "owner.go", data, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	seen := 0
	for _, decl := range file.Decls {
		fn, ok := decl.(*ast.FuncDecl)
		if !ok {
			continue
		}
		queries := family145ConnectorSQL(t, "package proof\n"+family145NodeString(fn))
		index := 0
		if fn.Name.Name == "ReadTextIntentFactStorageTx" {
			index = 1
			if !strings.Contains(family145NodeString(fn), "providerEventID).Scan(&fact)") {
				t.Fatal("text owner lost its exact bound provider event")
			}
		}
		if len(original) != 2 || len(queries) != 1 || queries[0] != strings.Replace(original[index], "'7320'", "$1", 1) {
			t.Fatalf("physical operator-channel scope changed: %s: %v vs %v", fn.Name.Name, queries, original)
		}
		seen++
	}
	if seen != 2 {
		t.Fatalf("missing original physical read owners: %d", seen)
	}
}
