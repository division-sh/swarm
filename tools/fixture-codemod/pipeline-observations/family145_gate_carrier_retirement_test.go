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
