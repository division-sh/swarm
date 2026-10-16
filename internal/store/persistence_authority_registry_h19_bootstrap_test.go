package store_test

import (
	"bytes"
	"fmt"
	"os"
	"path/filepath"
	"slices"
	"sort"
	"strings"
	"testing"
)

// PR2698/6205327685 decision9: only these two obsolete full identities may
// disappear at initial admission. The immutable 4813-key origin never changes.
var registryH19Retirements = [2]string{
	"unclassified\traw-operation\traw-sql\tinternal/store/internal/backend/delivery/read_projections.go\tReadRepeatedFanOutSnapshotTx\tcall:database/sql.(*database/sql.Row).Scan#1\tfunc=func(dest ...any) error|recv=*database/sql.Row|arg=*int64|arg=*int64|arg=*int64|arg=*int64|arg=*int64|arg=*int64|arg=*int64|arg=*int64|results=(error)",
	"unclassified\traw-operation\traw-sql\tinternal/store/internal/runtimepersistence/test_served_delivery_observation.go\tReadRepeatedFanOutSnapshotForTest\tcall:github.com/division-sh/swarm/internal/store/internal/backend/delivery.ReadRepeatedFanOutSnapshotTx#1\tfunc=func(ctx context.Context, tx *database/sql.Tx, run string) ([8]int64, error)|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|arg=string|results=([8]int64,error)",
}

var registryH19SourcePins = [3][3]string{
	{"internal/store/internal/backend/delivery/read_projections.go", "ReadRepeatedFanOutSnapshotTx", "d913a51ded4e165efe424ea2801b5c549c40999aa9b3f9a3b0204279754c3478"},
	{"internal/store/internal/runtimepersistence/test_served_delivery_observation.go", "ReadRepeatedFanOutSnapshotForTest", "e9a5a7b6ba0964692ad26a1b0e3887c49cfd4c88d64d3ebe926c50191b7da6b1"},
	{"internal/store/internal/backend/runforkrevision/postgres.go", "CountRunFactRevisionRowsForTest", "197079c3599de1fcf55e40f6aaae10f5f468935a0004e96d16d075a7eda9ccd2"},
}

func registryRetainedH19Initial(origin []byte) ([]byte, error) {
	rows, err := validateRegistryRetainedOrigin(registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision}, origin)
	if err != nil {
		return nil, err
	}
	for _, row := range registryH19Retirements {
		if !rows[row] {
			return nil, fmt.Errorf("exact H19 predecessor identity is absent")
		}
		delete(rows, row)
	}
	var retained []string
	for row := range rows {
		retained = append(retained, row)
	}
	sort.Strings(retained)
	return []byte(strings.Join(retained, "\n") + "\n"), nil
}

func registryRetainedH19Bootstrap(root string, origin, active []byte) error {
	exact, err := registryRetainedH19Initial(origin)
	if err != nil {
		return err
	}
	if !bytes.Equal(active, exact) {
		return fmt.Errorf("first retained registry must equal origin or its exact two-key H19 retirement")
	}
	for _, pin := range registryH19SourcePins {
		declarations, err := debtServedDebugDeclarations(filepath.Join(root, pin[0]))
		if err != nil || debtBaselineChecksum([]byte(declarations[pin[1]])) != pin[2] {
			return fmt.Errorf("H19 bootstrap source differs: %s/%s got=%s: %v", pin[0], pin[1], debtBaselineChecksum([]byte(declarations[pin[1]])), err)
		}
	}
	roles, err := os.ReadFile(filepath.Join(root, "internal/store/testdata/persistence_authority_findings.tsv"))
	if err != nil {
		return err
	}
	for index, row := range registryH19Retirements {
		role, replacement := "private-domain-adapter", strings.Replace(row, "|arg=*int64|results=", "|results=", 1)
		if index == 1 {
			role, replacement = "private-runtime-adapter", strings.ReplaceAll(row, "[8]int64", "[7]int64")
		}
		replacement = strings.Replace(replacement, "unclassified\t", role+"\t", 1)
		if strings.Count(string(roles), replacement+"\n") != 1 || strings.Contains(string(roles), row+"\n") {
			return fmt.Errorf("H19 bootstrap requires the exact reviewed replacement role and signature")
		}
	}
	const revisionRole = "private-runtime-adapter\traw-operation\traw-sql\tinternal/store/internal/runtimepersistence/test_served_delivery_observation.go\tReadRepeatedFanOutSnapshotForTest\tcall:github.com/division-sh/swarm/internal/store/internal/backend/runforkrevision.CountRunFactRevisionRowsForTest#1\tfunc=func(ctx context.Context, tx *database/sql.Tx, runID string) (int, error)|recv=invalid type|arg=context.Context|arg=*database/sql.Tx|arg=string|results=(int,error)\n"
	if strings.Count(string(roles), revisionRole) != 1 {
		return fmt.Errorf("H19 bootstrap requires the exact revision-owner consumer role")
	}
	return nil
}

func TestPersistenceAuthorityRetainedRegistryH19BootstrapIsExact(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	admission, err := loadRegistryRetainedAdmission(root, registryRetainedAuthorization{registryRetainedOrigin, registryRetainedDecision})
	if err != nil {
		t.Fatal(err)
	}
	var rows []string
	for row := range admission.Origin {
		rows = append(rows, row)
	}
	sort.Strings(rows)
	origin := []byte(strings.Join(rows, "\n") + "\n")
	exact, err := registryRetainedH19Initial(origin)
	if err != nil {
		t.Fatal(err)
	}
	for _, pin := range registryH19SourcePins {
		declarations, err := debtServedDebugDeclarations(filepath.Join(root, pin[0]))
		if err != nil {
			t.Fatal(err)
		}
		t.Logf("exact source %s/%s sha256=%s", pin[0], pin[1], debtBaselineChecksum([]byte(declarations[pin[1]])))
	}
	if active, err := parseRegistryRetainedRows(exact); err != nil || len(active) != 4811 {
		t.Fatalf("exact initial retirement=%d error=%v", len(active), err)
	}
	if err := registryRetainedH19Bootstrap(root, origin, exact); err != nil {
		t.Fatal(err)
	}
	kept := bytes.SplitN(exact, []byte("\n"), 2)[0]
	for _, mutant := range [][]byte{
		bytes.Replace(origin, []byte(registryH19Retirements[0]+"\n"), nil, 1),
		bytes.Replace(exact, append(slices.Clone(kept), '\n'), nil, 1),
		append(slices.Clone(exact), []byte("unclassified\tforeign\n")...),
		bytes.Replace(exact, kept, []byte(registryH19Retirements[0]), 1),
	} {
		if err := registryRetainedH19Bootstrap(root, origin, mutant); err == nil {
			t.Fatal("one-key, third deletion, addition or substitution gained bootstrap authority")
		}
	}
	copy := t.TempDir()
	for _, pin := range registryH19SourcePins {
		raw, err := os.ReadFile(filepath.Join(root, pin[0]))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, pin[0], string(raw))
	}
	const roles = "internal/store/testdata/persistence_authority_findings.tsv"
	raw, err := os.ReadFile(filepath.Join(root, roles))
	if err != nil {
		t.Fatal(err)
	}
	debtWriteModuleSource(t, copy, roles, string(raw))
	if err := registryRetainedH19Bootstrap(copy, origin, exact); err != nil {
		t.Fatal(err)
	}
	for _, pin := range registryH19SourcePins {
		original, err := os.ReadFile(filepath.Join(copy, pin[0]))
		if err != nil {
			t.Fatal(err)
		}
		declarations, err := debtServedDebugDeclarations(filepath.Join(copy, pin[0]))
		if err != nil {
			t.Fatal(err)
		}
		debtWriteModuleSource(t, copy, pin[0], "package witness\n"+strings.Replace(declarations[pin[1]], "return", "foreignReturn", 1))
		if err := registryRetainedH19Bootstrap(copy, origin, exact); err == nil {
			t.Fatal("changed current H19 source gained bootstrap authority after a warm pass:", pin[1])
		}
		debtWriteModuleSource(t, copy, pin[0], string(original))
	}
	debtWriteModuleSource(t, copy, roles, strings.Replace(string(raw), "private-domain-adapter\traw-operation\traw-sql\t"+registryH19SourcePins[0][0]+"\t"+registryH19SourcePins[0][1]+"\t", "unclassified\traw-operation\traw-sql\t"+registryH19SourcePins[0][0]+"\t"+registryH19SourcePins[0][1]+"\t", 1))
	if err := registryRetainedH19Bootstrap(copy, origin, exact); err == nil {
		t.Fatal("changed H19 replacement role gained bootstrap authority")
	}
}
