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

// #2700/6251310878 and 6251311357: one obsolete retained signature, not
// retirement of the INSERT, its parameter/caller authority, or any numeric debt.
const registryEffectClassSourceAfter = "58952785806bc6733461eb6a5665d76e4e5cbee0"
const registryEffectClassOwner = "internal/store/internal/backend/activityjournal/fork_history_fixture.go"
const registryEffectClassCaller = "internal/store/internal/runtimepersistence/test_activity_fork_history_fixture.go"
const registryEffectClassBeforeSHA = "60417323c5d8698774fe6c85337e784ca1f117b383b28a167af4763b2cb237e0"
const registryEffectClassAfterSHA = "fa424e0ba2d6a6fbe9ca83019299abf206949ccb6bb5a99c49db6dc4c1f352d6"
const registryEffectClassCallerSHA = "cf84282d0cd73ec95ddfe130d88a4c411f46ecf1374b6c23381562d6079f3683"

const registryEffectClassOld = "unclassified\traw-operation\traw-sql\t" + registryEffectClassOwner + "\tInsertHistoricalActivityForkAttemptTx\tcall:database/sql.(*database/sql.Tx).ExecContext#1\t" +
	"func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|arg=string|arg=github.com/division-sh/swarm/internal/runtime/effects.ExecutionMode|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=any|arg=string|arg=string|arg=string|arg=time.Time|arg=time.Time|arg=time.Time|results=(database/sql.Result,error)"

const registryEffectClassCurrent = "private-backend\traw-operation\traw-sql\t" + registryEffectClassOwner + "\tInsertHistoricalActivityForkAttemptTx\tcall:database/sql.(*database/sql.Tx).ExecContext#1\t" +
	"func=func(ctx context.Context, query string, args ...any) (database/sql.Result, error)|recv=*database/sql.Tx|arg=context.Context|arg=string|arg=string|arg=string|arg=github.com/division-sh/swarm/internal/runtime/effects.ExecutionMode|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=int|arg=string|arg=string|arg=string|arg=string|arg=string|arg=string|arg=any|arg=string|arg=string|arg=string|arg=time.Time|arg=time.Time|arg=time.Time|results=(database/sql.Result,error)"

func registryEffectClassContinuation(root string, previous []byte) ([]byte, error) {
	return registryEffectClassContinuationAt(root, root, previous)
}

func registryEffectClassContinuationAt(gitRoot, actualRoot string, previous []byte) ([]byte, error) {
	if err := registryEffectClassSources(gitRoot, actualRoot); err != nil {
		return nil, err
	}
	roles, err := os.ReadFile(filepath.Join(actualRoot, "internal/store/testdata/persistence_authority_findings.tsv"))
	if err != nil {
		return nil, err
	}
	if err := registryEffectClassRoles(roles); err != nil {
		return nil, err
	}
	rows, err := parseRegistryRetainedRows(previous)
	if err != nil {
		return nil, err
	}
	if !rows[registryEffectClassOld] {
		return nil, fmt.Errorf("effect-class continuation lacks its exact retained predecessor")
	}
	delete(rows, registryEffectClassOld)
	var retained []string
	for row := range rows {
		retained = append(retained, row)
	}
	sort.Strings(retained)
	return []byte(strings.Join(retained, "\n") + "\n"), nil
}

func registryEffectClassSources(gitRoot, actualRoot string) error {
	for _, file := range []struct{ path, before, after string }{
		{registryEffectClassOwner, registryEffectClassBeforeSHA, registryEffectClassAfterSHA},
		{registryEffectClassCaller, registryEffectClassCallerSHA, registryEffectClassCallerSHA},
	} {
		if _, err := debtRepair2698SignedSource(gitRoot, registryRetainedOrigin, file.path, file.before); err != nil {
			return err
		}
		after, err := debtRepair2698SignedSource(gitRoot, registryEffectClassSourceAfter, file.path, file.after)
		if err != nil {
			return err
		}
		actual, err := os.ReadFile(filepath.Join(actualRoot, file.path))
		if err != nil || !bytes.Equal(actual, after) {
			return fmt.Errorf("effect-class continuation differs from complete actual owner/caller source: %s: %v", file.path, err)
		}
	}
	return nil
}

func registryEffectClassRoles(raw []byte) error {
	oldKey := strings.TrimPrefix(registryEffectClassOld, "unclassified\t")
	primitive := "call:database/sql.(*database/sql.Tx).ExecContext#"
	count := 0
	for _, line := range strings.Split(string(raw), "\n") {
		parts := strings.Split(line, "\t")
		if len(parts) != 7 {
			continue
		}
		if strings.Join(parts[1:], "\t") == oldKey {
			return fmt.Errorf("obsolete effect-class full identity remains in actual registry")
		}
		if parts[3] == registryEffectClassOwner && parts[4] == "InsertHistoricalActivityForkAttemptTx" && strings.HasPrefix(parts[5], primitive) {
			if line != registryEffectClassCurrent {
				return fmt.Errorf("effect-class replacement role/signature differs")
			}
			count++
		}
	}
	if count != 1 {
		return fmt.Errorf("effect-class replacement primitive multiplicity=%d, want one", count)
	}
	return nil
}

func proveRegistryEffectClassContinuation(t *testing.T, root string, origin, previous []byte) {
	t.Helper()
	before := slices.Clone(previous)
	exact, err := registryEffectClassContinuation(root, previous)
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(previous, before) {
		t.Fatal("continuation mutated the prior packet")
	}
	priorRows, err := parseRegistryRetainedRows(previous)
	if err != nil {
		t.Fatal(err)
	}
	actualRows, err := parseRegistryRetainedRows(exact)
	if err != nil {
		t.Fatal(err)
	}
	if len(priorRows) != 4704 || len(actualRows) != 4703 || actualRows[registryEffectClassOld] || actualRows[strings.Replace(registryEffectClassCurrent, "private-backend\t", "unclassified\t", 1)] {
		t.Fatal("effect-class continuation changed more than the one obsolete key or substituted its successor")
	}
	for row := range priorRows {
		if row != registryEffectClassOld && !actualRows[row] {
			t.Fatal("unrelated retained identity disappeared")
		}
	}
	if err := registryRetainedH19Bootstrap(root, origin, exact); err != nil {
		t.Fatal(err)
	}
	for _, mutant := range [][]byte{
		bytes.Replace(exact, []byte(strings.Split(string(exact), "\n")[0]+"\n"), nil, 1),
		append(slices.Clone(exact), []byte(registryEffectClassOld+"\n")...),
		append(slices.Clone(exact), []byte(strings.Replace(registryEffectClassCurrent, "private-backend\t", "unclassified\t", 1)+"\n")...),
	} {
		if err := registryRetainedH19Bootstrap(root, origin, mutant); err == nil {
			t.Fatal("extra deletion, resurrection or substitution gained continuation admission")
		}
	}
	admission, actual, registered, strict := registryRetainedControl()
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, strict); err != nil {
		t.Fatal(err)
	}
	admission.Active[registryEffectClassOld] = true
	admission.Origin[registryEffectClassOld] = true
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, strict); err == nil {
		t.Fatal("the obsolete retained identity resurrected through unchanged strict admission")
	}
	admission.Previous[registryEffectClassOld] = true
	if err := validateRegistryRetainedAdmission(admission, actual, registered, nil, nil, strict); err == nil {
		t.Fatal("the inherited obsolete signature remained admitted without an actual finding")
	}
	proveRegistryEffectClassSourceAndRoleControls(t, root, previous)
}

func proveRegistryEffectClassSourceAndRoleControls(t *testing.T, root string, previous []byte) {
	t.Helper()
	copy := t.TempDir()
	paths := []string{registryEffectClassOwner, registryEffectClassCaller, "internal/store/testdata/persistence_authority_findings.tsv"}
	original := make(map[string][]byte)
	for _, path := range paths {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		original[path] = raw
		debtWriteModuleSource(t, copy, path, string(raw))
	}
	check := func(want bool) {
		t.Helper()
		_, err := registryEffectClassContinuationAt(root, copy, previous)
		if (err == nil) != want {
			t.Fatalf("actual-copy continuation accept=%t err=%v", want, err)
		}
	}
	check(true)
	if _, err := registryEffectClassContinuationAt(copy, copy, previous); err == nil {
		t.Fatal("copied files without the authenticated signed source pair gained continuation admission")
	}
	for _, path := range paths[:2] {
		for _, mutation := range []string{"missing", "changed", "extra-call", "argument", "stale"} {
			if mutation == "missing" {
				if err := os.Remove(filepath.Join(copy, path)); err != nil {
					t.Fatal(err)
				}
			} else {
				raw := append(slices.Clone(original[path]), []byte("\n// unrelated source change\n")...)
				if mutation == "extra-call" {
					if path == registryEffectClassOwner {
						raw = bytes.Replace(original[path], []byte("_, err = tx.ExecContext(ctx, query,"), []byte("tx.ExecContext(ctx, query)\n\t_, err = tx.ExecContext(ctx, query,"), 1)
					} else {
						raw = bytes.Replace(original[path], []byte("return activityjournal.InsertHistoricalActivityForkAttemptTx(ctx, tx, postgres, record)"), []byte("activityjournal.InsertHistoricalActivityForkAttemptTx(ctx, tx, postgres, record)\nreturn activityjournal.InsertHistoricalActivityForkAttemptTx(ctx, tx, postgres, record)"), 1)
					}
				}
				if mutation == "argument" {
					if path == registryEffectClassOwner {
						raw = bytes.Replace(original[path], []byte("record.ActivityID, record.Tool, record.Attempt"), []byte("record.ActivityID, record.Tool, record.Tool, record.Attempt"), 1)
					} else {
						raw = bytes.Replace(original[path], []byte("tx, postgres, record)"), []byte("tx, !postgres, record)"), 1)
					}
				}
				if mutation == "stale" {
					prior, err := debtGit(root, "show", registryRetainedOrigin+":"+path)
					if err != nil {
						t.Fatal(err)
					}
					raw = prior
					if bytes.Equal(raw, original[path]) {
						continue
					}
				}
				debtWriteModuleSource(t, copy, path, string(raw))
			}
			check(false)
			debtWriteModuleSource(t, copy, path, string(original[path]))
			check(true) // No warm or copied result can hide the next disk change.
		}
	}
	roles := paths[2]
	for _, changed := range []string{
		strings.Replace(string(original[roles]), registryEffectClassCurrent+"\n", "", 1),
		strings.Replace(string(original[roles]), registryEffectClassCurrent, strings.Replace(registryEffectClassCurrent, "private-backend\t", "unclassified\t", 1), 1),
		strings.Replace(string(original[roles]), registryEffectClassCurrent, registryEffectClassCurrent+" changed", 1),
		strings.Replace(string(original[roles]), registryEffectClassCurrent, strings.Replace(registryEffectClassCurrent, registryEffectClassOwner, "internal/store/internal/backend/activityjournal/foreign.go", 1), 1),
		string(original[roles]) + registryEffectClassCurrent + "\n",
		string(original[roles]) + strings.Replace(registryEffectClassCurrent, "ExecContext#1", "ExecContext#2", 1) + "\n",
		string(original[roles]) + registryEffectClassOld + "\n",
	} {
		debtWriteModuleSource(t, copy, roles, changed)
		check(false)
		debtWriteModuleSource(t, copy, roles, string(original[roles]))
		check(true)
	}
}
