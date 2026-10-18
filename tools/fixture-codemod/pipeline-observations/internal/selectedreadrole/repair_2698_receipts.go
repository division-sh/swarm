package selectedreadrole

import (
	"bytes"
	"crypto/sha256"
	_ "embed"
	"encoding/json"
	"fmt"
	"go/format"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
)

// #2698/6247420399: this view is only for historical comparisons.
const CompletionRebase2698Path = "internal/serveapp/fork_receiver_ownership_test.go"
const CompletionRebase2698Function = "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores"
const CompletionRebase2698Before = "af47497e27bcc0cca593fb56f5767ff68f201ce1"
const CompletionRebase2698After = "297b495eb876585c317e3ff59ce51e26b3bc02c3"
const CompletionRebase2698BeforeSHA = "c22bdc13e1a0f3b4a3c8beb2f8ccfcc138439ca9c7f2eea7618f7b152d06129a"
const CompletionRebase2698AfterSHA = "264fbaa1318ff6c6f9858489a714c319e91f8e70057ba5a6fa9e998ee7c567d4"
const OperatorRebase2698Path = "internal/apiv1/operator_deployment_run_start_test.go"
const OperatorRebase2698Feed = "TestDeploymentRunStartFeedOnlyAcrossSelectedStores"
const OperatorRebase2698Prior = "TestRunStartRejectsDeclaredFeedWithoutOutputPinBeforeMutation"
const OperatorRebase2698Current = "TestRunStartRejectsDeclaredFeedWithoutInputPinBeforeMutation"
const operatorRebase2698BeforeSHA = "289d76066fd81ff411710827269800d6f1319f0a30e91bd918afe48a22260852"
const operatorRebase2698AfterSHA = "6622171eb6599be90d287f6930bc82234587188526a7d3b9c5048ef38c0ec6ca"

// #2698/6247777206: only these two complete signed declarations may compose.
func OperatorRebase2698Images(root string) ([]byte, []byte, error) {
	before, err := exec.Command("git", "-C", root, "show", CompletionRebase2698Before+":"+OperatorRebase2698Path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(before)) != operatorRebase2698BeforeSHA {
		return nil, nil, fmt.Errorf("operator rebase lost its complete signed predecessor: %v", err)
	}
	after, err := exec.Command("git", "-C", root, "show", CompletionRebase2698After+":"+OperatorRebase2698Path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(after)) != operatorRebase2698AfterSHA {
		return nil, nil, fmt.Errorf("operator rebase lost its complete signed endpoint: %v", err)
	}
	actual, err := os.ReadFile(filepath.Join(root, OperatorRebase2698Path))
	if err != nil || !bytes.Equal(actual, after) {
		return nil, nil, fmt.Errorf("operator rebase actual source changed: %v", err)
	}
	feed, feedErr := Body(after, OperatorRebase2698Feed)
	refusal, refusalErr := Body(after, OperatorRebase2698Current)
	if _, err := Body(after, OperatorRebase2698Prior); err == nil || feedErr != nil || refusalErr != nil {
		return nil, nil, fmt.Errorf("operator rebase lost its unique renamed declarations")
	}
	restored, err := operatorRebase2698Inverse(after, feed, refusal)
	if err != nil || !bytes.Equal(restored, before) {
		return nil, nil, fmt.Errorf("operator rebase changed outside its finite declarations: %v", err)
	}
	return before, after, nil
}

func operatorRebase2698Inverse(after []byte, feed, refusal string) ([]byte, error) {
	for _, edit := range []struct{ body, current, prior string }{
		{feed, "event plus feed admits exact root input", "event plus feed admits exact output pin"},
		{refusal, OperatorRebase2698Current, OperatorRebase2698Prior},
		{refusal, "\t\t\t\tbeforeRows, err := storetest.ReadFiniteRunStartStorageCounts(ctx, fixture.primary)\n\t\t\t\tif err != nil {\n\t\t\t\t\tt.Fatal(err)\n\t\t\t\t}\n", ""},
		{refusal, "no exact declared root input pin", "no exact declared output pin"},
		{refusal, "rejection did not come from compiled root-input admission", "rejection did not come from compiled output-pin admission"},
		{refusal, "\t\t\t\tafterRows, err := storetest.ReadFiniteRunStartStorageCounts(ctx, fixture.primary)\n\t\t\t\tif err != nil || beforeRows != afterRows {\n\t\t\t\t\tt.Fatalf(\"failed admission changed durable rows or API completions: before=%+v after=%+v err=%v\", beforeRows, afterRows, err)\n\t\t\t\t}\n", ""},
		{refusal, "prepare non-input-pinned resource", "prepare non-output-pinned resource"},
	} {
		cut := []byte(edit.current)
		if bytes.Count([]byte(edit.body), cut) != 1 || bytes.Count(after, cut) != 1 {
			return nil, fmt.Errorf("operator rebase lost a unique declared edit: %s", edit.current)
		}
		after = bytes.Replace(after, cut, []byte(edit.prior), 1)
	}
	return after, nil
}

func operatorRebase2698HistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path != OperatorRebase2698Path || fmt.Sprintf("%x", sha256.Sum256(current)) != operatorRebase2698AfterSHA {
		return current, nil
	}
	before, after, err := OperatorRebase2698Images(root)
	if err != nil || !bytes.Equal(current, after) {
		return nil, fmt.Errorf("operator inverse does not describe actual copied source: %v", err)
	}
	return bytes.Clone(before), nil
}

// One view for all old ledger consumers; every unlisted row stays byte-semantic
// identical to the signed frozen ledger, including historical fields and order.
func CompletionRebase2698HistoricalRecipes(root string, current []byte) ([]byte, error) {
	const path = "tools/fixture-codemod/pipeline-observations/recipes.json"
	actual, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return nil, err
	}
	var rows, supplied []map[string]any
	if json.Unmarshal(actual, &rows) != nil || json.Unmarshal(current, &supplied) != nil {
		return nil, fmt.Errorf("completion rebase ledger is not valid JSON")
	}
	if !reflect.DeepEqual(rows, supplied) {
		return current, nil // A historical/intermediate ledger acquires no inverse.
	}
	prior, err := exec.Command("git", "-C", root, "show", CompletionRebase2698Before+":"+path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(prior)) != "b264aa83aa40d870752ba1dd2ce8cf7b0a05f12185fe4a33c37e7922a8119d2f" {
		return nil, fmt.Errorf("completion rebase lost its complete signed ledger: %v", err)
	}
	var expected []map[string]any
	if err := json.Unmarshal(prior, &expected); err != nil {
		return nil, err
	}
	if err := completionRebase2698RecipeEdges(root, expected); err != nil {
		return nil, err
	}
	if !reflect.DeepEqual(expected, rows) {
		for index := range expected {
			if index < len(rows) && !reflect.DeepEqual(expected[index], rows[index]) {
				return nil, fmt.Errorf("completion rebase ledger changed outside its exact three Successors: row %d %v/%v", index, expected[index]["File"], expected[index]["Function"])
			}
		}
		return nil, fmt.Errorf("completion rebase ledger changed outside its exact three Successors")
	}
	return prior, nil
}

func completionRebase2698RecipeEdges(root string, rows []map[string]any) error {
	before, after, err := CompletionRebase2698Images(root)
	if err != nil {
		return err
	}
	operatorBefore, operatorAfter, err := OperatorRebase2698Images(root)
	if err != nil {
		return err
	}
	for _, edge := range []struct {
		path, old, current, family string
		before, after              []byte
	}{
		{CompletionRebase2698Path, CompletionRebase2698Function, CompletionRebase2698Function, "native-served-delivery-quiescence-caller", before, after},
		{OperatorRebase2698Path, OperatorRebase2698Feed, OperatorRebase2698Feed, "native-durable-resource-carrier-family145", operatorBefore, operatorAfter},
		{OperatorRebase2698Path, OperatorRebase2698Prior, OperatorRebase2698Current, "native-durable-resource-carrier-family145", operatorBefore, operatorAfter},
	} {
		old, oldErr := Body(edge.before, edge.old)
		current, currentErr := Body(edge.after, edge.current)
		if oldErr != nil || currentErr != nil {
			return fmt.Errorf("completion rebase lost a unique recipe declaration")
		}
		matched := 0
		for _, row := range rows {
			if row["File"] != edge.path || row["Function"] != edge.old {
				continue
			}
			matched++
			endpoint := row["Successor"]
			if endpoint == nil || endpoint == "" {
				endpoint = row["After"]
			}
			if row["Family"] != edge.family || endpoint != old {
				return fmt.Errorf("completion rebase changed a signed recipe endpoint: %s", edge.old)
			}
			successor := current
			if edge.path == CompletionRebase2698Path {
				successor, err = completionRebase2698SignedRecipe(root, row, current)
				if err != nil {
					return err
				}
			}
			row["Successor"] = successor
		}
		if matched != 1 {
			return fmt.Errorf("completion rebase lost a unique recipe row: %s", edge.old)
		}
	}
	return nil
}

func completionRebase2698SignedRecipe(root string, original map[string]any, current string) (string, error) {
	raw, err := exec.Command("git", "-C", root, "show", CompletionRebase2698After+":tools/fixture-codemod/pipeline-observations/recipes.json").Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(raw)) != "0430b0c467f39bc3afade666a8a47e2c877da1526fb310b6513fcb75eb72eff8" {
		return "", fmt.Errorf("completion rebase lost its signed common-descendant ledger: %v", err)
	}
	var rows []map[string]any
	if err := json.Unmarshal(raw, &rows); err != nil {
		return "", err
	}
	matched, successor := 0, ""
	for _, row := range rows {
		if row["File"] != CompletionRebase2698Path || row["Function"] != CompletionRebase2698Function {
			continue
		}
		matched++
		successor, _ = row["Successor"].(string)
		body, err := Body([]byte("package proof\n"+successor), CompletionRebase2698Function)
		row["Successor"] = original["Successor"]
		if err != nil || body != current || !reflect.DeepEqual(row, original) {
			return "", fmt.Errorf("completion rebase changed its exact signed source/recipe pair")
		}
	}
	if matched != 1 {
		return "", fmt.Errorf("completion rebase lost its unique signed recipe")
	}
	return successor, nil
}

func CompletionRebase2698Images(root string) ([]byte, []byte, error) {
	const packetPath = "tools/fixture-codemod/pipeline-observations/testdata/capability_completion_successor_2707.json"
	packet, err := os.ReadFile(filepath.Join(root, packetPath))
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(packet)) != "4b531e52419e81158f1faed4ad8c3c63f367d30d13a3ddb79fd62302a61bb9ff" {
		return nil, nil, fmt.Errorf("completion rebase lost its immutable #2707 packet: %v", err)
	}
	before, err := exec.Command("git", "-C", root, "show", CompletionRebase2698Before+":"+CompletionRebase2698Path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(before)) != CompletionRebase2698BeforeSHA {
		return nil, nil, fmt.Errorf("completion rebase lost its complete signed predecessor: %v", err)
	}
	after, err := exec.Command("git", "-C", root, "show", CompletionRebase2698After+":"+CompletionRebase2698Path).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(after)) != CompletionRebase2698AfterSHA {
		return nil, nil, fmt.Errorf("completion rebase lost its complete signed endpoint: %v", err)
	}
	actual, err := os.ReadFile(filepath.Join(root, CompletionRebase2698Path))
	if err != nil || !bytes.Equal(actual, after) {
		return nil, nil, fmt.Errorf("completion rebase actual source changed: %v", err)
	}
	body, err := Body(after, CompletionRebase2698Function)
	observer := []byte("\t\t\tcompletion := installReceiverCompletionBoundary(t)\n")
	join := []byte("\t\t\tctx, cancel := context.WithTimeout(t.Context(), servedEventPublishLifecycleProbeWaitTimeout)\n\t\t\tdefer cancel()\n\t\t\trequireReceiverCompletionBoundary(t, ctx, completion, rt.BundleHash, seed.RunID)\n")
	for _, cut := range [][]byte{observer, join} {
		if err != nil || bytes.Count([]byte(body), cut) != 1 || bytes.Count(after, cut) != 1 {
			return nil, nil, fmt.Errorf("completion rebase lost a unique named-declaration cut: %v", err)
		}
	}
	restored := bytes.Replace(bytes.Replace(after, observer, nil, 1), join, nil, 1)
	if !bytes.Equal(restored, before) {
		return nil, nil, fmt.Errorf("completion rebase changed outside its two reviewed additions")
	}
	return before, after, nil
}

func CompletionRebase2698HistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path != CompletionRebase2698Path || fmt.Sprintf("%x", sha256.Sum256(current)) != CompletionRebase2698AfterSHA {
		return current, nil
	}
	before, after, err := CompletionRebase2698Images(root)
	if err != nil || !bytes.Equal(current, after) {
		return nil, fmt.Errorf("completion inverse does not describe actual copied source: %v", err)
	}
	return bytes.Clone(before), nil
}

// The historical view is limited to the exact settled #2698 source repair.
// Neither the recipes nor any prior source/history packet is rewritten.
//
//go:embed testdata/repair_2698_sources.json
var repair2698Sources []byte

//go:embed testdata/repair_2698_fixture_name_successor.json
var repair2698FixtureNameSources []byte

//go:embed testdata/repair_2698_cleanup_successor.json
var repair2698CleanupSources []byte

const Repair2698Before = "e857a1e482a2e08ab63f2e8169b4472e608cfdc2"
const Repair2698After = "f5c57acafdae2cd0750f493c15ca667ca3d84e33"
const Repair2698SourceDigest = "d5deace20725d4bafb1a8a294634618daf9312a5b91eff0d7a14e8c1ce6d39e6"
const Repair2698FixtureNameAfter = "6a6c51b3924ff248e5a6b4c382f4631d3ff734c8"
const Repair2698FixtureNameDigest = "910a0a6e02aba0d8f3245454c0505ab237c6ee0da11f18a0f78b8a305a0b6976"
const Repair2698FixtureNamePath = "internal/store/internal/runtimepersistence/test_event_support.go"
const Repair2698CleanupAfter = "6d6c3eafaea02d3e0fef5e3183870174436adbc1"
const Repair2698CleanupDigest = "976911841d61b4088ff55ce29b0e499b5629b1e3b764b6c91169538493debd05"
const Repair2698CleanupPath = "internal/serveapp/main_runtime_test.go"

type repair2698CleanupPacket struct {
	BeforeCommit, AfterCommit, Authorization string
	Files                                    []Repair2698Source
}

func readRepair2698CleanupPacket(root string) (repair2698CleanupPacket, error) {
	var packet repair2698CleanupPacket
	if fmt.Sprintf("%x", sha256.Sum256(repair2698CleanupSources)) != Repair2698CleanupDigest {
		return packet, fmt.Errorf("cleanup continuation certificate changed")
	}
	decoder := json.NewDecoder(bytes.NewReader(repair2698CleanupSources))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&packet); err != nil {
		return packet, err
	}
	if decoder.Decode(new(any)) != io.EOF || packet.BeforeCommit != Repair2698After ||
		packet.AfterCommit != Repair2698CleanupAfter || len(packet.Files) != 1 ||
		packet.Files[0].File != Repair2698CleanupPath ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2698#issuecomment-6218416177" {
		return packet, fmt.Errorf("cleanup continuation lost its exact one-file boundary")
	}
	file := packet.Files[0]
	before, err := exec.Command("git", "-C", root, "show", packet.BeforeCommit+":"+file.File).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(before)) != file.BeforeSHA256 {
		return packet, fmt.Errorf("cleanup continuation lost its predecessor: %v", err)
	}
	after, err := exec.Command("git", "-C", root, "show", packet.AfterCommit+":"+file.File).Output()
	if err != nil || fmt.Sprintf("%x", sha256.Sum256(after)) != file.AfterSHA256 {
		return packet, fmt.Errorf("cleanup continuation lost its signed endpoint: %v", err)
	}
	old := []byte("\tif _, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {\n\t\tp.t.Errorf(\"timed out stopping Run during cleanup\\noutput:\\n%s\", p.outputString())\n\t}\n")
	next := []byte("\tif code, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {\n\t\tp.t.Errorf(\"timed out stopping Run during cleanup\\noutput:\\n%s\", p.outputString())\n\t} else if code != 0 {\n\t\tp.t.Errorf(\"Run exited during cleanup with code %d\\noutput:\\n%s\", code, p.outputString())\n\t}\n")
	if bytes.Count(before, old) != 1 || !bytes.Equal(after, bytes.Replace(before, old, next, 1)) {
		return packet, fmt.Errorf("cleanup continuation changed more than the accepted exit-status check")
	}
	return packet, nil
}

func repair2698CleanupHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != Repair2698CleanupPath {
		return actual, nil
	}
	packet, err := readRepair2698CleanupPacket(root)
	if err != nil {
		return nil, err
	}
	file := packet.Files[0]
	if fmt.Sprintf("%x", sha256.Sum256(actual)) != file.AfterSHA256 {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) {
		return nil, fmt.Errorf("cleanup inverse does not describe actual copied source: %v", err)
	}
	return exec.Command("git", "-C", root, "show", packet.BeforeCommit+":"+path).Output()
}

func Repair2698SuccessorHistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path == OperatorRebase2698Path {
		return operatorRebase2698HistoricalSource(root, path, current)
	}
	if path == CompletionRebase2698Path {
		return CompletionRebase2698HistoricalSource(root, path, current)
	}
	if path == repair2698SchedulePath {
		return repair2698ScheduleHistoricalSource(root, path, current)
	}
	if path == Repair2698CleanupPath {
		return repair2698CleanupHistoricalSource(root, path, current)
	}
	return Repair2698FixtureNameHistoricalSource(root, path, current)
}

type repair2698FixtureNamePacket struct {
	BeforeCommit, AfterCommit, Authorization string
	Files                                    []Repair2698Source
	Guard                                    Repair2698Source
}

func readRepair2698FixtureNamePacket(root string) (repair2698FixtureNamePacket, error) {
	var packet repair2698FixtureNamePacket
	if fmt.Sprintf("%x", sha256.Sum256(repair2698FixtureNameSources)) != Repair2698FixtureNameDigest {
		return packet, fmt.Errorf("fixture name continuation changed")
	}
	key := "repair2698/fixture-name/" + root + "/" + Repair2698FixtureNameDigest
	if cached, ok := sourceCache.Load(key); ok {
		packet = cached.(repair2698FixtureNamePacket)
		packet.Files = append([]Repair2698Source(nil), packet.Files...)
		return packet, nil
	}
	if err := json.Unmarshal(repair2698FixtureNameSources, &packet); err != nil {
		return packet, err
	}
	if packet.BeforeCommit != Repair2698After || packet.AfterCommit != Repair2698FixtureNameAfter ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/2542#issuecomment-6212534506" ||
		len(packet.Files) != 1 || packet.Files[0].File != Repair2698FixtureNamePath ||
		packet.Guard.File != "internal/store/internal/runtimepersistence/unretained_cleanup_boundary_guard_test.go" {
		return packet, fmt.Errorf("fixture name continuation lost its exact one-row boundary")
	}
	for _, file := range []Repair2698Source{packet.Files[0], packet.Guard} {
		old, err := exec.Command("git", "-C", root, "show", packet.BeforeCommit+":"+file.File).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(old)) != file.BeforeSHA256 {
			return packet, fmt.Errorf("fixture name continuation lost its predecessor: %s: %v", file.File, err)
		}
		current, err := exec.Command("git", "-C", root, "show", packet.AfterCommit+":"+file.File).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(current)) != file.AfterSHA256 {
			return packet, fmt.Errorf("fixture name continuation lost its signed endpoint: %s: %v", file.File, err)
		}
		if err := exactFixtureNameRename(old, current); err != nil {
			return packet, err
		}
	}
	copy := packet
	copy.Files = append([]Repair2698Source(nil), packet.Files...)
	sourceCache.Store(key, copy)
	return packet, nil
}

func exactFixtureNameRename(before, after []byte) error {
	old := []byte("validateUnretainedCleanupHistoryInputs")
	if bytes.Count(before, old) != 2 {
		return fmt.Errorf("fixture name continuation changed its two original identifiers")
	}
	expected, err := format.Source(bytes.ReplaceAll(before, old, []byte("validateUnretainedCleanupHistoryInputsForTest")))
	if err != nil || !bytes.Equal(after, expected) {
		return fmt.Errorf("fixture name continuation changed more than its two identifiers: %v", err)
	}
	return nil
}

func Repair2698FixtureNameHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != Repair2698FixtureNamePath {
		return actual, nil
	}
	packet, err := readRepair2698FixtureNamePacket(root)
	if err != nil {
		return nil, err
	}
	if fmt.Sprintf("%x", sha256.Sum256(actual)) != packet.Files[0].AfterSHA256 {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(actual, current) {
		return nil, fmt.Errorf("fixture name inverse does not describe actual copied source: %v", err)
	}
	return exec.Command("git", "-C", root, "show", packet.BeforeCommit+":"+path).Output()
}

type Repair2698Source struct{ File, BeforeSHA256, AfterSHA256 string }

func Repair2698Sources() ([]Repair2698Source, error) {
	pins, err := readRepair2698Sources()
	return append([]Repair2698Source(nil), pins...), err
}

func readRepair2698Sources() ([]Repair2698Source, error) {
	if fmt.Sprintf("%x", sha256.Sum256(repair2698Sources)) != Repair2698SourceDigest {
		return nil, fmt.Errorf("native repair source packet changed")
	}
	key := "repair2698/packet/" + Repair2698SourceDigest
	if cached, ok := sourceCache.Load(key); ok {
		return cached.([]Repair2698Source), nil
	}
	var packet struct {
		BeforeCommit, AfterCommit, Authorization string
		Files                                    []Repair2698Source
	}
	if err := json.Unmarshal(repair2698Sources, &packet); err != nil {
		return nil, err
	}
	if packet.BeforeCommit != Repair2698Before || packet.AfterCommit != Repair2698After || len(packet.Files) != 41 ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2698#issuecomment-6205327685" {
		return nil, fmt.Errorf("native repair lost its exact source inputs or authorization")
	}
	sourceCache.Store(key, packet.Files)
	return packet.Files, nil
}

// Authenticate the supplied actual bytes before reading immutable git history.
// Unknown and intermediate bytes receive no inverse, including in copied trees.
func Repair2698HistoricalSource(root, path string, current []byte) ([]byte, error) {
	actual := current
	current, err := Repair2698SuccessorHistoricalSource(root, path, current)
	if err != nil || (path == Repair2698FixtureNamePath || path == Repair2698CleanupPath) && bytes.Equal(actual, current) {
		return current, err // No stale intermediate acquires the combined inverse.
	}
	pins, err := readRepair2698Sources()
	if err != nil {
		return nil, err
	}
	digest := fmt.Sprintf("%x", sha256.Sum256(current))
	for _, pin := range pins {
		if pin.File != path || pin.AfterSHA256 != digest {
			continue
		}
		key := "repair2698/" + root + "/" + path + "/" + pin.BeforeSHA256
		if cached, ok := sourceCache.Load(key); ok {
			return bytes.Clone(cached.([]byte)), nil
		}
		previous, err := exec.Command("git", "-C", root, "show", Repair2698Before+":"+path).Output()
		if err != nil || fmt.Sprintf("%x", sha256.Sum256(previous)) != pin.BeforeSHA256 {
			return nil, fmt.Errorf("native repair lost its signed predecessor %s: %v", path, err)
		}
		sourceCache.Store(key, bytes.Clone(previous))
		return previous, nil
	}
	return current, nil
}

func repair2698HistoricalFunction(root, path, name, current string) (string, error) {
	pins, err := readRepair2698Sources()
	if err != nil {
		return "", err
	}
	listed := false
	for _, pin := range pins {
		listed = listed || pin.File == path
	}
	if !listed {
		return current, nil
	}
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	actual, err := Body(raw, name)
	if err != nil || actual != current {
		return current, nil // Historical/intermediate text is not a live certificate.
	}
	previous, err := Repair2698HistoricalSource(root, path, raw)
	if err != nil || bytes.Equal(previous, raw) {
		return current, err
	}
	// Compose through the unchanged earlier integration packet exactly once.
	previous, err = CatchUp2542HistoricalSource(root, path, previous)
	if err != nil {
		return "", err
	}
	body, err := Body(previous, name)
	if err != nil {
		return current, nil // A new native setup declaration has no old recipe.
	}
	return body, nil
}
