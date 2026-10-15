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

func family145GateCarrierShapeError(source string) error {
	file, err := parser.ParseFile(token.NewFileSet(), "gate.go", source, parser.AllErrors)
	if err != nil {
		return err
	}
	want := map[string]string{
		"name": "string", "events": "gateRecoverySelectedStore", "lifecycle": "runtimerunlifecycle.CandidateStore",
		"cards": "gateRecoveryDecisionStore", "persistence": "runtimepipeline.WorkflowPersistence", "trace": "gateRecoveryTraceStore",
	}
	var found int
	ast.Inspect(file, func(node ast.Node) bool {
		spec, ok := node.(*ast.TypeSpec)
		if !ok || spec.Name.Name != "gateRecoveryStoreCase" {
			return true
		}
		found++
		shape, ok := spec.Type.(*ast.StructType)
		if !ok {
			err = fmt.Errorf("gate carrier is no longer a closed role projection")
			return false
		}
		if len(shape.Fields.List) != len(want) {
			err = fmt.Errorf("gate carrier changed its exact field inventory")
			return false
		}
		got := map[string]string{}
		for _, field := range shape.Fields.List {
			if len(field.Names) != 1 {
				err = fmt.Errorf("gate carrier contains an embedded or combined escape")
				return false
			}
			got[field.Names[0].Name] = family145NodeString(field.Type)
		}
		if !reflect.DeepEqual(got, want) {
			err = fmt.Errorf("gate carrier acquired a raw pool, dialect or foreign role: %v", got)
		}
		return false
	})
	if found != 1 || err != nil {
		return fmt.Errorf("closed gate carrier inventory=%d/1: %v", found, err)
	}
	return nil
}

func family145GateCarrierConstructorProofError(function, before, after string) error {
	want, err := family145DescriptorBusinessAssertions(before)
	if err != nil {
		return err
	}
	got, err := family145DescriptorBusinessAssertions(after)
	if err != nil || !reflect.DeepEqual(want, got) {
		return fmt.Errorf("gate native opener lost exact trace-owner assertion: %v", err)
	}
	backend := strings.TrimSuffix(strings.TrimPrefix(function, "open"), "GateRecoveryStore")
	for _, cut := range []string{
		"selected := storetest.Start" + backend + "RuntimeStore(t)",
		"persistence := runtimepipeline.NewWorkflowPersistence(selected)",
		"name: \"" + strings.ToLower(backend) + "\", events: selected, cards: selected",
		"lifecycle: selected, persistence: persistence, trace: selected",
		"return result",
	} {
		if strings.Count(after, cut) != 1 {
			return fmt.Errorf("gate opener changed exact native/role ownership: %s", cut)
		}
	}
	for _, escape := range []string{"AdmitPostgres", "Database(", "StartPostgres(t)", "postgres:", "db:"} {
		if strings.Contains(after, escape) {
			return fmt.Errorf("gate opener retains raw/dialect construction: %s", escape)
		}
	}
	return nil
}

func family145GateDialectQualificationProofError(before, after string) error {
	for _, pair := range [][2]string{
		{"!f.p.selected.postgres", `backend.name != "postgres"`},
		{"corruption.duplicate && f.p.selected.postgres", `corruption.duplicate && backend.name == "postgres"`},
	} {
		if strings.Count(before, pair[0]) != 1 || strings.Count(after, pair[1]) != 1 {
			return fmt.Errorf("exact PG JSONB behavior qualification changed: %v", pair)
		}
		after = strings.Replace(after, pair[1], pair[0], 1)
	}
	if after != before {
		return fmt.Errorf("dialect-carrier removal changed original corruption/refusal assertions or setup")
	}
	return nil
}

func TestFamily145GateCarrierRetirementKeepsEveryRoleAndDialectQualification(t *testing.T) {
	data, err := os.ReadFile("../../../internal/runtime/pipeline/workflow_gate_recovery_external_test.go")
	if err != nil {
		t.Fatal(err)
	}
	if err := family145GateCarrierShapeError(string(data)); err != nil {
		t.Fatal(err)
	}
	for _, field := range []string{"db *sql.DB", "postgres bool", "escape any", "events *sql.DB"} {
		hostile := strings.Replace(string(data), "type gateRecoveryStoreCase struct {", "type gateRecoveryStoreCase struct {\n"+field, 1)
		if family145GateCarrierShapeError(hostile) == nil {
			t.Fatalf("raw/dialect/foreign/duplicate carrier capability passed: %s", field)
		}
	}
	var rows []struct{ File, Function, Before, After string }
	if err := json.Unmarshal(family145TransitionBytes, &rows); err != nil {
		t.Fatal(err)
	}
	matched := 0
	for _, row := range rows {
		switch row.Function {
		case "openSQLiteGateRecoveryStore", "openPostgresGateRecoveryStore":
			matched++
			if err := family145GateCarrierConstructorProofError(row.Function, row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			for _, hostile := range []string{strings.Replace(row.After, "trace: selected", "trace: foreign", 1), strings.Replace(row.After, "StartPostgresRuntimeStore", "AdmitPostgresRuntimeStore", 1), strings.Replace(row.After, "StartSQLiteRuntimeStore", "AdmitSQLiteRuntimeStore", 1)} {
				if hostile != row.After && family145GateCarrierConstructorProofError(row.Function, row.Before, hostile) == nil {
					t.Fatal("foreign role or supplied-pool construction passed")
				}
			}
		case "TestA2MapFanOutRawBusinessSourceReaderRefusesOnBothStores":
			matched++
			if err := family145GateDialectQualificationProofError(row.Before, row.After); err != nil {
				t.Fatal(err)
			}
			if family145GateDialectQualificationProofError(row.Before, strings.Replace(row.After, `backend.name == "postgres"`, "false", 1)) == nil {
				t.Fatal("lost PostgreSQL duplicate-key assertion passed")
			}
		case "selected":
			if row.File != "internal/runtime/pipeline/issue2564_publication_handoff_external_test.go" {
				continue
			}
			matched++
			original := strings.Replace(row.Before, `name: f.backend, postgres: f.backend == "postgres",`, "name: f.backend,", 1)
			if original != row.After {
				t.Fatal("indirect native handoff projection lost its original roles/owner")
			}
		}
	}
	if matched != 4 {
		t.Fatalf("carrier/opener/indirect/dialect caller inventory=%d/4", matched)
	}
}
