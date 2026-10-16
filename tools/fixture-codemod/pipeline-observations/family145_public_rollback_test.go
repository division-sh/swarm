package main

import (
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/publicrollback"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

//go:embed family145-public-rollback-preimages.json
var publicRollbackBytes []byte

func publicRollbackPins(t *testing.T) []publicrollback.Pin {
	t.Helper()
	pins, err := publicrollback.Decode(publicRollbackBytes)
	if err != nil {
		t.Fatal(err)
	}
	return pins
}

func restorePublicRollbackHistory(t *testing.T, rows []map[string]any, recipes bool) []map[string]any {
	pins, err := publicrollback.Decode(publicRollbackBytes)
	if err != nil {
		panic(err)
	}
	for _, row := range rows {
		for _, pin := range pins {
			if row["File"] != pin.File || row["Function"] != pin.Function {
				continue
			}
			if recipes && row["After"] == pin.Before && row["Successor"] == pin.After {
				delete(row, "Successor")
			} else if !recipes && row["After"] == pin.After {
				row["After"] = pin.Before
			}
		}
	}
	return rows
}

func cloneRollbackHistory(t *testing.T, rows []map[string]any) []map[string]any {
	t.Helper()
	raw, err := json.Marshal(rows)
	if err != nil {
		t.Fatal(err)
	}
	var copy []map[string]any
	if err := json.Unmarshal(raw, &copy); err != nil {
		t.Fatal(err)
	}
	return copy
}

func TestFamily145PublicRollbackNewOwnersHaveExactPhysicalPredicates(t *testing.T) {
	for _, owner := range [][3]string{
		{"internal/store/internal/backend/pipelinepersistence/owner_operations.go", "CountTemplateEventReplayScopesTx", "func CountTemplateEventReplayScopesTx(ctx context.Context, tx *sql.Tx, event string) (int,error) { var count int;err:=tx.QueryRowContext(ctx,`SELECT COUNT(*) FROM committed_replay_scopes WHERE event_id=$1`,event).Scan(&count);return count,err }"},
		{"internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "CountTemplateEventReplayScopesForTest", `func CountTemplateEventReplayScopesForTest(ctx context.Context, selected any, event string)(int,error){return readFanOutSemanticStorage(ctx,selected,[]string{event},func(ctx context.Context,tx *sql.Tx)(int,error){return pipelinepersistence.CountTemplateEventReplayScopesTx(ctx,tx,event)})}`},
	} {
		raw, err := os.ReadFile(filepath.Join("../../..", owner[0]))
		if err != nil || requestKeyDeclaration(t, raw, owner[1]) != requestKeyDeclaration(t, []byte("package proof\n"+owner[2]), owner[1]) {
			t.Fatal("rollback query/identity/native snapshot contract changed:", owner[1], err)
		}
	}
	path := "internal/store/internal/backend/pipelinepersistence/template_storage_observation.go"
	old, err := exec.Command("git", "show", publicrollback.Predecessor+":"+path).Output()
	current, readErr := os.ReadFile("../../../internal/store/internal/backend/pipelinepersistence/owner_operations.go")
	if err != nil || readErr != nil || requestKeyDeclaration(t, old, "CountTemplateSubscribedReplayScopeTx") != requestKeyDeclaration(t, current, "CountTemplateSubscribedReplayScopeTx") {
		t.Fatal("subscribed-only owner was silently broadened:", err, readErr)
	}
	oldLocation, err := os.ReadFile(filepath.Join("../../..", path))
	if err != nil {
		t.Fatal(err)
	}
	parsed, err := parser.ParseFile(token.NewFileSet(), path, oldLocation, parser.AllErrors)
	if err != nil {
		t.Fatal(err)
	}
	for _, declaration := range parsed.Decls {
		if fn, ok := declaration.(*ast.FuncDecl); ok && (fn.Name.Name == "CountTemplateEventReplayScopesTx" || fn.Name.Name == "CountTemplateSubscribedReplayScopeTx") {
			t.Fatal("moved replay-scope reader survived at the old owner location:", fn.Name.Name)
		}
	}
}
