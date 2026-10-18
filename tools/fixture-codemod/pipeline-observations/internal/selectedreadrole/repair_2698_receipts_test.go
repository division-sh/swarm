package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
)

func TestRepair2698SourceCompositionPreservesBothSignedCuts(t *testing.T) {
	root := filepath.Clean("../../../../..")
	assertCompletionRebase2698Inverse(t, root)
	t.Run("schedule-original-reader", func(t *testing.T) { assertRepair2698ScheduleReaderInverse(t, root) })
	pins, err := Repair2698Sources()
	if err != nil {
		t.Fatal(err)
	}
	previousPath := ""
	for _, pin := range pins {
		t.Run(pin.File, func(t *testing.T) {
			if pin.File <= previousPath || !filepath.IsLocal(pin.File) || filepath.ToSlash(filepath.Clean(pin.File)) != pin.File {
				t.Fatal("source packet is not a unique canonical sorted inventory")
			}
			previousPath = pin.File
			current, err := os.ReadFile(filepath.Join(root, pin.File))
			if err != nil {
				t.Fatal(err)
			}
			endpoint, err := Repair2698SuccessorHistoricalSource(root, pin.File, current)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(endpoint)) != pin.AfterSHA256 {
				t.Fatal("actual source differs from the finite repaired endpoint:", err)
			}
			for _, cut := range []struct{ ref, digest string }{{Repair2698Before, pin.BeforeSHA256}, {Repair2698After, pin.AfterSHA256}} {
				raw, err := exec.Command("git", "-C", root, "show", cut.ref+":"+pin.File).Output()
				if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != cut.digest {
					t.Fatal("source packet differs from its signed cut:", cut.ref, err)
				}
			}
			previous, err := Repair2698HistoricalSource(root, pin.File, current)
			if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != pin.BeforeSHA256 {
				t.Fatal("complete historical source was not restored:", err)
			}
			again, err := Repair2698HistoricalSource(root, pin.File, previous)
			if err != nil || !bytes.Equal(again, previous) {
				t.Fatal("native repair was applied more than once:", err)
			}
			for _, source := range [][]byte{append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("func "), []byte("func foreign_"), 1)} {
				if bytes.Equal(source, current) {
					t.Fatal("hostile source did not change")
				}
				got, err := Repair2698HistoricalSource(root, pin.File, source)
				if err == nil && !bytes.Equal(got, source) {
					t.Fatal("warm source cache certified changed current bytes")
				}
			}
			got, err := Repair2698HistoricalSource(root, pin.File+".foreign", current)
			if err != nil || !bytes.Equal(got, current) {
				t.Fatal("foreign source path received an inverse:", err)
			}
		})
	}
}

func assertCompletionRebase2698Inverse(t *testing.T, root string) {
	t.Helper()
	before, after, err := CompletionRebase2698Images(root)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := CompletionRebase2698HistoricalSource(root, CompletionRebase2698Path, after)
	if err != nil || !bytes.Equal(previous, before) {
		t.Fatal("completion inverse lost its complete predecessor:", err)
	}
	once, err := CompletionRebase2698HistoricalSource(root, CompletionRebase2698Path, previous)
	if err != nil || !bytes.Equal(once, previous) {
		t.Fatal("completion inverse applied twice:", err)
	}
	foreign, err := CompletionRebase2698HistoricalSource(root, CompletionRebase2698Path+".foreign", after)
	if err != nil || !bytes.Equal(foreign, after) {
		t.Fatal("foreign completion path acquired an inverse:", err)
	}
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	const packet = "tools/fixture-codemod/pipeline-observations/testdata/capability_completion_successor_2707.json"
	for _, path := range []string{CompletionRebase2698Path, OperatorRebase2698Path, "tools/fixture-codemod/pipeline-observations/recipes.json", packet} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			t.Fatal(err)
		}
		writeReadRoleSource(t, copy, path, string(raw))
	}
	writeReadRoleSource(t, copy, ".git", "gitdir: "+strings.TrimSpace(string(gitdir))+"\n")
	if _, _, err := CompletionRebase2698Images(copy); err != nil {
		t.Fatal("exact copied completion source refused:", err)
	}
	for _, changed := range [][]byte{before, append(bytes.Clone(after), '\n'), append(bytes.Clone(after), after...)} {
		writeReadRoleSource(t, copy, CompletionRebase2698Path, string(changed))
		if _, err := CompletionRebase2698HistoricalSource(copy, CompletionRebase2698Path, after); err == nil {
			t.Fatal("warm completion inverse accepted stale/changed/duplicate/supplied source")
		}
	}
	if err := os.Remove(filepath.Join(copy, CompletionRebase2698Path)); err != nil {
		t.Fatal(err)
	}
	if _, err := CompletionRebase2698HistoricalSource(copy, CompletionRebase2698Path, after); err == nil {
		t.Fatal("pristine gitdir concealed missing completion source")
	}
	writeReadRoleSource(t, copy, CompletionRebase2698Path, string(after))
	assertOperatorRebase2698Inverse(t, copy)
	assertRebase2698LedgerHostility(t, copy)
	writeReadRoleSource(t, copy, packet, "{}\n")
	if _, _, err := CompletionRebase2698Images(copy); err == nil {
		t.Fatal("changed completion packet accepted")
	}
}

func assertOperatorRebase2698Inverse(t *testing.T, root string) {
	t.Helper()
	before, after, err := OperatorRebase2698Images(root)
	if err != nil {
		t.Fatal(err)
	}
	previous, err := operatorRebase2698HistoricalSource(root, OperatorRebase2698Path, after)
	if err != nil || !bytes.Equal(previous, before) {
		t.Fatal("operator inverse lost its complete signed predecessor:", err)
	}
	once, err := operatorRebase2698HistoricalSource(root, OperatorRebase2698Path, previous)
	if err != nil || !bytes.Equal(once, previous) {
		t.Fatal("operator inverse consumed its predecessor twice:", err)
	}
	foreign, err := operatorRebase2698HistoricalSource(root, OperatorRebase2698Path+".foreign", after)
	if err != nil || !bytes.Equal(foreign, after) {
		t.Fatal("foreign operator source acquired an inverse:", err)
	}
	old, err := Body(before, OperatorRebase2698Prior)
	if err != nil {
		t.Fatal(err)
	}
	current, err := Body(after, OperatorRebase2698Current)
	if err != nil {
		t.Fatal(err)
	}
	const beforeRows = "\t\t\t\tbeforeRows, err := storetest.ReadFiniteRunStartStorageCounts(ctx, fixture.primary)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n"
	const call = "\t\t\t\tresponse := rpcCall(t, handler, body)\n"
	for _, changed := range [][]byte{
		before,
		append(bytes.Clone(after), '\n'),
		bytes.Replace(after, []byte(beforeRows), nil, 1),
		bytes.Replace(after, []byte("beforeRows != afterRows"), []byte("false"), 1),
		bytes.Replace(bytes.Replace(after, []byte(beforeRows), nil, 1), []byte(call), []byte(call+beforeRows), 1),
		bytes.Replace(after, []byte(OperatorRebase2698Current), []byte(OperatorRebase2698Prior), 1),
		bytes.Replace(after, []byte(OperatorRebase2698Current), []byte("TestForeignRebaseOperator"), 1),
		append(bytes.Clone(after), []byte("\n"+old+"\n")...),
		append(bytes.Clone(after), []byte("\n"+current+"\n")...),
		bytes.Replace(after, []byte("apiCompletions != 0"), []byte("apiCompletions < 0"), 1),
	} {
		if bytes.Equal(changed, after) {
			t.Fatal("operator hostile did not mutate its current target")
		}
		writeReadRoleSource(t, root, OperatorRebase2698Path, string(changed))
		if _, err := operatorRebase2698HistoricalSource(root, OperatorRebase2698Path, after); err == nil {
			t.Fatal("warm operator inverse accepted lost/moved/changed checks, rename ambiguity or supplied source")
		}
	}
	if err := os.Remove(filepath.Join(root, OperatorRebase2698Path)); err != nil {
		t.Fatal(err)
	}
	if _, err := operatorRebase2698HistoricalSource(root, OperatorRebase2698Path, after); err == nil {
		t.Fatal("pristine gitdir concealed missing operator source")
	}
	writeReadRoleSource(t, root, OperatorRebase2698Path, string(after))
}

func assertRebase2698LedgerHostility(t *testing.T, root string) {
	t.Helper()
	const path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	actual, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	prior, err := CompletionRebase2698HistoricalRecipes(root, actual)
	if err != nil || bytes.Equal(prior, actual) {
		t.Fatal("exact three-edge ledger did not recover its signed predecessor:", err)
	}
	once, err := CompletionRebase2698HistoricalRecipes(root, prior)
	if err != nil || !bytes.Equal(once, prior) {
		t.Fatal("ledger inverse consumed a stale/reversed endpoint twice:", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(actual, &rows); err != nil {
		t.Fatal(err)
	}
	for _, mutation := range []func([]map[string]any) []map[string]any{
		func(rows []map[string]any) []map[string]any { rows[0]["Before"] = "foreign"; return rows },
		func(rows []map[string]any) []map[string]any { return rows[1:] },
		func(rows []map[string]any) []map[string]any { return append(rows, rows[0]) },
		func(rows []map[string]any) []map[string]any { rows[0], rows[1] = rows[1], rows[0]; return rows },
		func(rows []map[string]any) []map[string]any {
			for _, row := range rows {
				if row["File"] == OperatorRebase2698Path || row["File"] == CompletionRebase2698Path {
					delete(row, "Successor")
				}
			}
			return rows
		},
	} {
		var changed []map[string]any
		if err := json.Unmarshal(actual, &changed); err != nil {
			t.Fatal(err)
		}
		mutant := mutation(changed)
		raw, err := json.Marshal(mutant)
		if err != nil || reflect.DeepEqual(mutant, rows) {
			t.Fatal("ledger hostile did not alter its target:", err)
		}
		writeReadRoleSource(t, root, path, string(raw))
		if _, err := CompletionRebase2698HistoricalRecipes(root, raw); err == nil {
			t.Fatal("warm/copy ledger accepted altered, missing, duplicate, reordered or reverse rows")
		}
		if _, err := CatchUp2542HistoricalRecipes(root, actual, false); err == nil {
			t.Fatal("supplied pristine ledger concealed changed actual recipe input")
		}
	}
	writeReadRoleSource(t, root, path, string(actual))
	if _, err := CatchUp2542HistoricalRecipes(root, actual, false); err != nil {
		t.Fatal("restored complete ledger refused:", err)
	}
}

func writeReadRoleSource(t *testing.T, root, path, source string) {
	t.Helper()
	full := filepath.Join(root, path)
	if err := os.MkdirAll(filepath.Dir(full), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(full, []byte(source), 0600); err != nil {
		t.Fatal(err)
	}
}

func assertRepair2698ScheduleReaderInverse(t *testing.T, root string) {
	t.Helper()
	current, err := os.ReadFile(filepath.Join(root, repair2698SchedulePath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(current)) != repair2698ScheduleAfterSHA {
		t.Fatal("schedule fixture is not its exact repaired endpoint:", err)
	}
	before, err := Repair2698HistoricalSource(root, repair2698SchedulePath, current)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(before)) != repair2698ScheduleBeforeSHA {
		t.Fatal("schedule fixture lost its signed predecessor:", err)
	}
	for _, hostile := range [][]byte{before, append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("admit(\"7e0\")"), []byte("admit(\"8e0\")"), 1)} {
		got, err := Repair2698HistoricalSource(root, repair2698SchedulePath, hostile)
		if err == nil && !bytes.Equal(got, hostile) {
			t.Fatal("stale, repeated or changed assertion source received a schedule inverse")
		}
	}
	if got, err := Repair2698HistoricalSource(root, repair2698SchedulePath+".foreign", current); err != nil || !bytes.Equal(got, current) {
		t.Fatal("foreign path received a schedule inverse:", err)
	}
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, repair2698SchedulePath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, current, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := Repair2698HistoricalSource(copy, repair2698SchedulePath, current); err != nil || !bytes.Equal(got, before) {
		t.Fatal("exact copied schedule source refused:", err)
	}
	if err := os.WriteFile(path, append(bytes.Clone(current), '\n'), 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair2698HistoricalSource(copy, repair2698SchedulePath, current); err == nil {
		t.Fatal("pristine gitdir hid a changed copied schedule fixture")
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair2698HistoricalSource(copy, repair2698SchedulePath, current); err == nil {
		t.Fatal("warm source view hid a missing copied schedule fixture")
	}
}

func TestRepair2698FixtureNameContinuationIsExactAndOnceOnly(t *testing.T) {
	root := filepath.Clean("../../../../..")
	packet, err := readRepair2698FixtureNamePacket(root)
	if err != nil {
		t.Fatal(err)
	}
	for _, file := range []Repair2698Source{packet.Files[0], packet.Guard} {
		current, err := os.ReadFile(filepath.Join(root, file.File))
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(current)) != file.AfterSHA256 {
			t.Fatal("actual source/table differs from exact rename endpoint:", file.File, err)
		}
	}
	current, err := os.ReadFile(filepath.Join(root, Repair2698FixtureNamePath))
	if err != nil {
		t.Fatal(err)
	}
	previous, err := Repair2698FixtureNameHistoricalSource(root, Repair2698FixtureNamePath, current)
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != packet.Files[0].BeforeSHA256 {
		t.Fatal("exact rename inverse refused:", err)
	}
	for _, hostile := range [][]byte{previous, append(bytes.Clone(current), '\n'), bytes.Replace(current, []byte("ctx == nil"), []byte("ctx != nil"), 1), append(bytes.Clone(current), []byte("\nfunc extraRawCaller(){}\n")...)} {
		got, err := Repair2698HistoricalSource(root, Repair2698FixtureNamePath, hostile)
		if err == nil && !bytes.Equal(got, hostile) {
			t.Fatal("stale/changed-body/extra-call source acquired a combined inverse")
		}
	}
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	path := filepath.Join(copy, Repair2698FixtureNamePath)
	if err := os.MkdirAll(filepath.Dir(path), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(path, current, 0600); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair2698FixtureNameHistoricalSource(copy, Repair2698FixtureNamePath, current); err != nil {
		t.Fatal("exact copied source refused:", err)
	}
	if err := os.Remove(path); err != nil {
		t.Fatal(err)
	}
	if _, err := Repair2698FixtureNameHistoricalSource(copy, Repair2698FixtureNamePath, current); err == nil {
		t.Fatal("warm cache certified missing copied source against pristine gitdir")
	}
}

func TestRepair2698FunctionReadsCopiedSourceNotGitdirCheckout(t *testing.T) {
	root := filepath.Clean("../../../../..")
	const path = "internal/runtime/node_delivery_startup_recovery_test.go"
	const function = "TestPipelineCoordinatorRecoveryContinuesAfterCommittedDeadLetterParity"
	copy := t.TempDir()
	gitdir, err := exec.Command("git", "-C", root, "rev-parse", "--absolute-git-dir").Output()
	if err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, ".git"), []byte("gitdir: "+strings.TrimSpace(string(gitdir))+"\n"), 0600); err != nil {
		t.Fatal(err)
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		t.Fatal(err)
	}
	if err := os.MkdirAll(filepath.Dir(filepath.Join(copy, path)), 0700); err != nil {
		t.Fatal(err)
	}
	if err := os.WriteFile(filepath.Join(copy, path), raw, 0600); err != nil {
		t.Fatal(err)
	}
	actual, err := Body(raw, function)
	if err != nil {
		t.Fatal(err)
	}
	// The earlier packet is embedded; actual source always comes from this copy.
	prior, err := repair2698HistoricalFunction(copy, path, function, actual)
	if err != nil || prior == actual {
		t.Fatal("exact copied current source has no predecessor:", err)
	}
	hostile := bytes.Replace(raw, []byte("func "+function+"("), []byte("func "+function+"Foreign("), 1)
	if bytes.Equal(hostile, raw) {
		t.Fatal("copied-source mutation did not apply")
	}
	if err := os.WriteFile(filepath.Join(copy, path), hostile, 0600); err != nil {
		t.Fatal(err)
	}
	if got, err := repair2698HistoricalFunction(copy, path, function, actual); err == nil && got != actual {
		t.Fatal("pristine gitdir checkout hid changed copied source")
	}
	if err := os.Remove(filepath.Join(copy, path)); err != nil {
		t.Fatal(err)
	}
	if _, err := repair2698HistoricalFunction(copy, path, function, actual); err == nil {
		t.Fatal("warm source cache hid a missing actual sibling")
	}
}

func TestRepair2698PacketCacheCannotHideChangedMetadataOrCallerMutation(t *testing.T) {
	pins, err := Repair2698Sources()
	if err != nil {
		t.Fatal(err)
	}
	pins[0].AfterSHA256 = "foreign"
	if current, err := Repair2698Sources(); err != nil || current[0].AfterSHA256 == "foreign" {
		t.Fatal("a caller mutated the authenticated immutable packet:", err)
	}
	original := repair2698Sources
	t.Cleanup(func() { repair2698Sources = original })
	repair2698Sources = append(bytes.Clone(original), '\n')
	if _, err := Repair2698Sources(); err == nil {
		t.Fatal("warm immutable cache hid changed packet bytes")
	}
}
