package main

import (
	"crypto/sha256"
	"fmt"
	"os"
	"strings"
	"testing"
)

func TestCurrentEventObservationRelocationRetainsEveryExactDeclaration(t *testing.T) {
	raw, err := os.ReadFile("../../../" + owner)
	if err != nil {
		t.Fatal(err)
	}
	ledger, err := os.ReadFile("../../../.github/audit-artifacts/issue-2542-family145-event-owner-relocation.tsv")
	if err != nil {
		t.Fatal(err)
	}
	var names []string
	for _, input := range sources {
		names = append(names, input.names...)
	}
	_, actual := relocate(raw, names)
	rows := strings.Split(strings.TrimSpace(string(ledger)), "\n")
	if len(rows) != len(names) || len(names) != 23 {
		t.Fatalf("relocation inventory changed: %d/%d", len(rows), len(names))
	}
	for _, row := range rows {
		columns := strings.Split(row, "\t")
		if len(columns) != 3 || fmt.Sprintf("%x", sha256.Sum256([]byte(actual[columns[1]]))) != columns[2] {
			t.Fatalf("relocation changed an exact declaration: %s", row)
		}
	}
}

func TestFiniteRelocationRejectsMissingAndAmbiguousDeclarations(t *testing.T) {
	for _, source := range []string{"package fixture\nfunc Other() {}", "package fixture\nfunc Read() {}\nfunc Read() {}"} {
		t.Run(source, func(t *testing.T) {
			defer func() {
				if recover() == nil {
					t.Fatal("unreviewed declaration inventory accepted")
				}
			}()
			relocate([]byte(source), []string{"Read"})
		})
	}
}

func TestFiniteRelocationRejectsEditedInputAndSideEffectImports(t *testing.T) {
	t.Run("edited input", func(t *testing.T) {
		file := t.TempDir() + "/source.go"
		if err := os.WriteFile(file, []byte("package changed\n"), 0600); err != nil {
			t.Fatal(err)
		}
		defer func() {
			if recover() == nil {
				t.Fatal("unreviewed source accepted")
			}
		}()
		pinned(file, ownerBefore)
	})
	t.Run("side effect import", func(t *testing.T) {
		defer func() {
			if recover() == nil {
				t.Fatal("unreviewed import semantics accepted")
			}
		}()
		pruneImports([]byte("package fixture\nimport _ \"fmt\"\nfunc Kept() {}"))
	})
}
