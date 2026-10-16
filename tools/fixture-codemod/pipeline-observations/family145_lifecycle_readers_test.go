package main

import (
	_ "embed"
	"go/ast"
	"strings"
	"testing"
)

func family145LifecycleReadersInverse(after string) string {
	return strings.NewReplacer(
		"requireLifecycleEventCount(t, rt.ReceiverStateReader, ", "requireLifecycleEventCount(t, rt, ",
		"readLifecycleLoop(t, rt.Endpoint, ", "readLifecycleLoop(t, rt, ",
	).Replace(after)
}

func family145LifecycleReadersRecordCall(t *testing.T, function string, call *ast.CallExpr, counts map[string]int) {
	t.Helper()
	name := family145NodeString(call.Fun)
	switch name {
	case "requireLifecycleEventCount":
		if len(call.Args) != 5 || family145NodeString(call.Args[1]) != "rt.ReceiverStateReader" {
			t.Fatal("lifecycle count retained raw or foreign authority")
		}
		counts["lifecycle-event-count/"+function]++
	case "readLifecycleLoop":
		if len(call.Args) != 4 || family145NodeString(call.Args[1]) != "rt.Endpoint" {
			t.Fatal("public loop read retained raw or foreign authority")
		}
		counts["lifecycle-public-loop/"+function]++
	}
}
