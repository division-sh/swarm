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
)

// #2698/6247420399: this view is only for historical comparisons.
const CompletionRebase2698Path = "internal/serveapp/fork_receiver_ownership_test.go"
const CompletionRebase2698Function = "TestSelectedForkReceiverAcquisitionCapabilityRefusalBothStores"
const CompletionRebase2698Before = "af47497e27bcc0cca593fb56f5767ff68f201ce1"
const CompletionRebase2698After = "297b495eb876585c317e3ff59ce51e26b3bc02c3"
const CompletionRebase2698BeforeSHA = "c22bdc13e1a0f3b4a3c8beb2f8ccfcc138439ca9c7f2eea7618f7b152d06129a"
const CompletionRebase2698AfterSHA = "264fbaa1318ff6c6f9858489a714c319e91f8e70057ba5a6fa9e998ee7c567d4"

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
