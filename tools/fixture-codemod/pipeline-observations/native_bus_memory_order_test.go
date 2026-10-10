package main

import (
	"go/parser"
	"go/token"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestNativeBusMemoryOrderRetiresOnlyTheFakeSQLKey(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-bus-memory-persistence-order")
	before := strings.Replace(row.Before, "Intercept(ctx context.Context,", "Intercept(_ context.Context,", 1)
	before = strings.Replace(before, "\tif tx, ok := runtimepipelinefixture.SQLTx(ctx); ok && tx != nil {\n\t\ti.t.Fatal(\"non-transactional interceptor unexpectedly ran with sql tx\")\n\t}\n", "", 1)
	if before != row.After {
		t.Fatal("actual persistence-before-interception event-type assertions changed")
	}
	path := filepath.Join("..", "..", "..", row.File)
	body, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	set := token.NewFileSet()
	file, err := parser.ParseFile(set, path, body, 0)
	if err != nil {
		t.Fatal(err)
	}
	fn, err := uniqueRecipeFunction(file, row)
	if err != nil {
		t.Fatal(err)
	}
	actual := string(body[set.Position(fn.Pos()).Offset:set.Position(fn.End()).Offset])
	if actual != row.After || strings.Contains(actual, "runtimepipelinefixture") {
		t.Fatal("memory component regained the non-authoritative transaction protocol")
	}
}
