package main

import (
	"bytes"
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/selectedreadrole"
)

func nativeEntryConformanceRecipeInputs(t *testing.T) ([]byte, []recipe) {
	t.Helper()
	current, err := os.ReadFile(filepath.Join("../../..", selectedreadrole.NativeEntry2542File))
	if err != nil {
		t.Fatal(err)
	}
	var all []recipe
	if err := json.Unmarshal(recipeBytes, &all); err != nil {
		t.Fatal(err)
	}
	var rows []recipe
	for _, row := range all {
		if row.File == selectedreadrole.NativeEntry2542File {
			rows = append(rows, row)
		}
	}
	if len(rows) != 15 {
		t.Fatalf("conformance recipe inventory=%d want15", len(rows))
	}
	return current, rows
}
