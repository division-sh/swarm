package main

import (
	"encoding/json"
	"strings"
	"testing"
)

func TestNativeSelectedConstructionDropsOnlyUnusedDatabaseAuthority(t *testing.T) {
	var rows []recipe
	if err := json.Unmarshal(recipeBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		if row.Family != "native-selected-construction-raw-argument-retirement" {
			continue
		}
		matched++
		row = historicalMechanicalRecipe(t, row)
		switch row.Function {
		case "selectedContractExecutionOwnerForCatalogTest":
			if strings.Count(row.Before, "db *sql.DB, ") != 1 || strings.Replace(row.Before, "db *sql.DB, ", "", 1) != row.After {
				t.Fatal("selected native dependency assembly or joined context retirement changed")
			}
		case "selectedContractExecutionOwnerForCatalogHarness":
			if strings.Count(row.Before, "selectedContractExecutionOwnerForCatalogTest(t, h.db, h.pg,") != 1 ||
				strings.Replace(row.Before, "selectedContractExecutionOwnerForCatalogTest(t, h.db, h.pg,", "selectedContractExecutionOwnerForCatalogTest(t, h.pg,", 1) != row.After {
				t.Fatal("owner reuse/binding, optional lifecycle collaborator or SQLite dependency assembly changed")
			}
		default:
			t.Fatalf("unreviewed construction recipe: %s", row.Function)
		}
	}
	if matched != 2 {
		t.Fatalf("construction recipes=%d, want2", matched)
	}
}
