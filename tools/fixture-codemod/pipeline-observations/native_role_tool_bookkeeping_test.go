package main

import (
	"strings"
	"testing"
)

func nativeRoleToolBookkeepingWorkload(t *testing.T, source string) string {
	t.Helper()
	source = strings.ReplaceAll(source, "inject sqlite role-scoped hostile bookkeeping:", "inject sqlite hostile bookkeeping:")
	return nativeToolBookkeepingWorkload(t, source)
}

func TestNativeRoleToolBookkeepingPreservesEntireCurrentEntityWorkload(t *testing.T) {
	row := nativeMissingHeaderRecipe(t, "native-role-tool-private-bookkeeping")
	if nativeRoleToolBookkeepingWorkload(t, row.Before) != nativeRoleToolBookkeepingWorkload(t, row.After) {
		t.Fatal("role-scoped current entity, hostile storage cut or generated-tool assertions changed")
	}
	for _, change := range [][2]string{
		{"sqliteStore, entityToolTestRunID, entityID", "sqliteStore, otherRun, entityID"},
		{"\"evt-current\"", "\"other-current\""},
		{"changed != 1", "changed != 0"},
		{"brief[\"summary\"]", "brief[\"wrong\"]"},
	} {
		changed := strings.Replace(row.After, change[0], change[1], 1)
		if changed == row.After || nativeRoleToolBookkeepingWorkload(t, row.Before) == nativeRoleToolBookkeepingWorkload(t, changed) {
			t.Fatalf("changed role-scoped storage/workload admitted: %s", change[0])
		}
	}
}
