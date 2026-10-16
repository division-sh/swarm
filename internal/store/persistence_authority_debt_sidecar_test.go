package store_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"go/format"
	"io"
	"os"
	"path/filepath"
)

const debtCollectorSidecarPath = "internal/store/testdata/persistence_authority_debt_baseline.collector.json"
const debtCacheCollectorFrom = "b42ea974646e7b666459091174645baa501d91da16871813568db23858def7b7"

type debtCollectorMetadata struct {
	Version        int    `json:"version"`
	Collector      string `json:"collector"`
	BaselineSHA256 string `json:"baseline_sha256"`
	Predecessor    string `json:"predecessor"`
}

func debtBaselineChecksum(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

type debtRepair2698SourceFile struct{ File, BeforeSHA256, AfterSHA256 string }
type debtRepair2698SourcePacket struct {
	BeforeCommit, AfterCommit, Authorization string
	Files                                    []debtRepair2698SourceFile
}

type debtRepair2698FixtureNamePacket struct {
	BeforeCommit, AfterCommit, Authorization string
	Files                                    []debtRepair2698SourceFile
	Guard                                    debtRepair2698SourceFile
}

func debtRepair2698ReadFixtureNamePacket(root string) (debtRepair2698FixtureNamePacket, error) {
	var packet debtRepair2698FixtureNamePacket
	raw, err := os.ReadFile(filepath.Join(root, debtRepair2698FixtureNamePacketPath))
	if err != nil {
		return packet, err
	}
	if debtBaselineChecksum(raw) != debtRepair2698FixtureNameSHA {
		return packet, fmt.Errorf("fixture name source certificate changed")
	}
	key := "repair2698/fixture-name/" + root + "/" + debtRepair2698FixtureNameSHA
	if cached, ok := debtSelectedReadRolePinCache.Load(key); ok {
		packet = cached.(debtRepair2698FixtureNamePacket)
		packet.Files = append([]debtRepair2698SourceFile(nil), packet.Files...)
		return packet, nil
	}
	if err := json.Unmarshal(raw, &packet); err != nil {
		return packet, err
	}
	if packet.BeforeCommit != debtRepair2698After || packet.AfterCommit != debtRepair2698FixtureNameAfter ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/2542#issuecomment-6212534506" ||
		len(packet.Files) != 1 || packet.Files[0].File != debtRepair2698FixtureNameSource ||
		packet.Guard.File != "internal/store/internal/runtimepersistence/unretained_cleanup_boundary_guard_test.go" {
		return packet, fmt.Errorf("fixture name continuation lost its exact one-row boundary")
	}
	for _, file := range []debtRepair2698SourceFile{packet.Files[0], packet.Guard} {
		before, err := debtRepair2698SignedSource(root, packet.BeforeCommit, file.File, file.BeforeSHA256)
		if err != nil {
			return packet, err
		}
		after, err := debtRepair2698SignedSource(root, packet.AfterCommit, file.File, file.AfterSHA256)
		if err != nil {
			return packet, err
		}
		old := []byte("validateUnretainedCleanupHistoryInputs")
		expected, err := format.Source(bytes.ReplaceAll(before, old, []byte("validateUnretainedCleanupHistoryInputsForTest")))
		if err != nil || bytes.Count(before, old) != 2 || !bytes.Equal(after, expected) {
			return packet, fmt.Errorf("fixture name continuation changed more than two identifiers: %s: %v", file.File, err)
		}
	}
	copy := packet
	copy.Files = append([]debtRepair2698SourceFile(nil), packet.Files...)
	debtSelectedReadRolePinCache.Store(key, copy)
	return packet, nil
}

func debtRepair2698FixtureNameHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != debtRepair2698FixtureNameSource {
		return actual, nil
	}
	packet, err := debtRepair2698ReadFixtureNamePacket(root)
	if os.IsNotExist(err) {
		return actual, nil
	}
	if err != nil {
		return nil, err
	}
	file := packet.Files[0]
	if debtBaselineChecksum(actual) != file.AfterSHA256 {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) {
		return nil, fmt.Errorf("fixture name inverse does not describe actual copied source: %v", err)
	}
	return debtRepair2698SignedSource(root, packet.BeforeCommit, path, file.BeforeSHA256)
}

func debtRepair2698ReadCleanupPacket(root string) (debtRepair2698SourcePacket, error) {
	var packet debtRepair2698SourcePacket
	raw, err := os.ReadFile(filepath.Join(root, debtRepair2698CleanupPacketPath))
	if err != nil {
		return packet, err
	}
	if debtBaselineChecksum(raw) != debtRepair2698CleanupSHA {
		return packet, fmt.Errorf("cleanup source certificate changed")
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&packet); err != nil {
		return packet, err
	}
	if decoder.Decode(new(any)) != io.EOF || packet.BeforeCommit != debtRepair2698After ||
		packet.AfterCommit != debtRepair2698CleanupAfter || len(packet.Files) != 1 ||
		packet.Files[0].File != debtRepair2698CleanupSource ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2698#issuecomment-6218416177" {
		return packet, fmt.Errorf("cleanup continuation lost its exact one-file boundary")
	}
	file := packet.Files[0]
	before, err := debtRepair2698SignedSource(root, packet.BeforeCommit, file.File, file.BeforeSHA256)
	if err != nil {
		return packet, err
	}
	after, err := debtRepair2698SignedSource(root, packet.AfterCommit, file.File, file.AfterSHA256)
	if err != nil {
		return packet, err
	}
	old := []byte("\tif _, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {\n\t\tp.t.Errorf(\"timed out stopping Run during cleanup\\noutput:\\n%s\", p.outputString())\n\t}\n")
	next := []byte("\tif code, ok := p.waitForExit(serveRuntimeStopTimeout); !ok {\n\t\tp.t.Errorf(\"timed out stopping Run during cleanup\\noutput:\\n%s\", p.outputString())\n\t} else if code != 0 {\n\t\tp.t.Errorf(\"Run exited during cleanup with code %d\\noutput:\\n%s\", code, p.outputString())\n\t}\n")
	if bytes.Count(before, old) != 1 || !bytes.Equal(after, bytes.Replace(before, old, next, 1)) {
		return packet, fmt.Errorf("cleanup continuation changed more than the accepted exit-status check")
	}
	return packet, nil
}

func debtRepair2698CleanupHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != debtRepair2698CleanupSource {
		return actual, nil
	}
	packet, err := debtRepair2698ReadCleanupPacket(root)
	if os.IsNotExist(err) {
		return actual, nil // Immutable archives predate this continuation.
	}
	if err != nil {
		return nil, err
	}
	file := packet.Files[0]
	if debtBaselineChecksum(actual) != file.AfterSHA256 {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) {
		return nil, fmt.Errorf("cleanup inverse does not describe actual copied source: %v", err)
	}
	return debtRepair2698SignedSource(root, packet.BeforeCommit, path, file.BeforeSHA256)
}

func debtRepair2698SuccessorHistoricalSource(root, path string, current []byte) ([]byte, error) {
	if path == debtRepair2698SchedulePath {
		return debtRepair2698ScheduleHistoricalSource(root, path, current)
	}
	if path == debtRepair2698CleanupSource {
		return debtRepair2698CleanupHistoricalSource(root, path, current)
	}
	return debtRepair2698FixtureNameHistoricalSource(root, path, current)
}

func debtRepair2698ScheduleHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != debtRepair2698SchedulePath || debtBaselineChecksum(actual) != debtRepair2698ScheduleAfterSHA {
		return actual, nil
	}
	current, err := os.ReadFile(filepath.Join(root, path))
	if err != nil || !bytes.Equal(current, actual) {
		return nil, fmt.Errorf("schedule reader inverse does not describe actual copied source: %v", err)
	}
	return debtRepair2698SignedSource(root, debtRepair2698ScheduleBefore, path, debtRepair2698ScheduleBeforeSHA)
}

func debtRepair2698ReadPacket(root string) (debtRepair2698SourcePacket, error) {
	var packet debtRepair2698SourcePacket
	raw, err := os.ReadFile(filepath.Join(root, debtRepair2698SourcePath))
	if err != nil {
		return packet, err
	}
	if debtBaselineChecksum(raw) != debtRepair2698SourceSHA {
		return packet, fmt.Errorf("native repair source certificate changed")
	}
	key := "repair2698/packet/" + root + "/" + debtRepair2698SourceSHA
	if cached, ok := debtSelectedReadRolePinCache.Load(key); ok {
		packet = cached.(debtRepair2698SourcePacket)
		packet.Files = append([]debtRepair2698SourceFile(nil), packet.Files...)
		return packet, nil
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&packet); err != nil {
		return packet, err
	}
	if decoder.Decode(new(any)) != io.EOF || packet.BeforeCommit != debtRepair2698Before || packet.AfterCommit != debtRepair2698After ||
		packet.Authorization != "https://github.com/division-sh/swarm/pull/2698#issuecomment-6205327685" || len(packet.Files) != 41 {
		return packet, fmt.Errorf("native repair lost its exact immutable inputs")
	}
	previous := ""
	for _, file := range packet.Files {
		if !filepath.IsLocal(file.File) || filepath.ToSlash(filepath.Clean(file.File)) != file.File || file.File <= previous ||
			!authorityDebtHex(file.BeforeSHA256, 64) || !authorityDebtHex(file.AfterSHA256, 64) || file.BeforeSHA256 == file.AfterSHA256 {
			return packet, fmt.Errorf("native repair has a foreign, duplicate or non-forward source: %s", file.File)
		}
		for _, input := range [][2]string{{packet.BeforeCommit, file.BeforeSHA256}, {packet.AfterCommit, file.AfterSHA256}} {
			if _, err := debtRepair2698SignedSource(root, input[0], file.File, input[1]); err != nil {
				return packet, err
			}
		}
		previous = file.File
	}
	copy := packet
	copy.Files = append([]debtRepair2698SourceFile(nil), packet.Files...)
	debtSelectedReadRolePinCache.Store(key, copy)
	return packet, nil
}

func debtRepair2698SignedSource(root, ref, path, digest string) ([]byte, error) {
	key := "repair2698/signed/" + root + "/" + ref + "/" + path + "/" + digest
	if cached, ok := debtSelectedReadRolePinCache.Load(key); ok {
		return bytes.Clone(cached.([]byte)), nil
	}
	raw, err := debtGit(root, "show", ref+":"+path)
	if err != nil || debtBaselineChecksum(raw) != digest {
		return nil, fmt.Errorf("native repair lost its exact signed source %s:%s: %v", ref, path, err)
	}
	debtSelectedReadRolePinCache.Store(key, bytes.Clone(raw))
	return raw, nil
}

// Preservation only: callers of the live census never consume this view.
func debtRepair2698HistoricalSource(root, path string, current []byte) ([]byte, error) {
	actual := current
	current, err := debtRepair2698SuccessorHistoricalSource(root, path, current)
	if err != nil || (path == debtRepair2698FixtureNameSource || path == debtRepair2698CleanupSource) && bytes.Equal(actual, current) {
		return current, err
	}
	packet, err := debtRepair2698ReadPacket(root)
	if os.IsNotExist(err) {
		return current, nil // Immutable archives before this repair have no certificate.
	}
	if err != nil {
		return nil, err
	}
	for _, file := range packet.Files {
		if file.File == path && debtBaselineChecksum(current) == file.AfterSHA256 {
			return debtRepair2698SignedSource(root, packet.BeforeCommit, path, file.BeforeSHA256)
		}
	}
	return current, nil
}

func debtRepair2698SourcePolicy(root string, historical debtCatchUpSourcePacket) error {
	cleanup, err := debtRepair2698ReadCleanupPacket(root)
	if err != nil && !os.IsNotExist(err) {
		return err
	}
	if err == nil {
		file := cleanup.Files[0]
		actual, err := os.ReadFile(filepath.Join(root, file.File))
		if err != nil || debtBaselineChecksum(actual) != file.AfterSHA256 {
			return fmt.Errorf("cleanup actual source changed: %s: %v", file.File, err)
		}
	}
	name, err := debtRepair2698ReadFixtureNamePacket(root)
	if err != nil {
		return err
	}
	for _, file := range []debtRepair2698SourceFile{name.Files[0], name.Guard} {
		raw, err := os.ReadFile(filepath.Join(root, file.File))
		if err != nil || debtBaselineChecksum(raw) != file.AfterSHA256 {
			return fmt.Errorf("fixture name actual source/table changed: %s: %v", file.File, err)
		}
	}
	packet, err := debtRepair2698ReadPacket(root)
	if err != nil {
		return err
	}
	intersections := 0
	for _, file := range packet.Files {
		actual, err := os.ReadFile(filepath.Join(root, file.File))
		if err == nil {
			actual, err = debtRepair2698SuccessorHistoricalSource(root, file.File, actual)
		}
		if err != nil || debtBaselineChecksum(actual) != file.AfterSHA256 {
			return fmt.Errorf("native repair actual source changed: %s: %v", file.File, err)
		}
		for _, old := range historical.Files {
			if old.File == file.File {
				if old.ComposedSHA256 != file.BeforeSHA256 {
					return fmt.Errorf("native repair does not continue its original source: %s", file.File)
				}
				intersections++
			}
		}
	}
	if intersections != 18 {
		return fmt.Errorf("native repair changed its exact 18 source intersections: %d", intersections)
	}
	return nil
}

func debtRepair2698CollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRepair2698CollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Before, debtCatchUpNativeEntryCollectorTo)
	if err != nil {
		return false
	}
	if base == debtCatchUpNativeEntryCollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		// Reuse the existing exact joined-input proof, then the single new edge.
		intermediate := head
		intermediate.Collector = debtCatchUpNativeEntryCollectorTo
		if !debtCatchUpNativeEntryCollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtRepair2698UnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtCatchUpNativeEntryCollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Before, debtCatchUpNativeEntryCollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("native repair metadata lost its exact predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRepair2698CollectorTo
	return baseline, nil
}

// Decision 10 changes proof cost, not the collector's authority vocabulary.
// Keep the first repair's fingerprint and signed source cut as a distinct edge.
func debtRepair2698ProofCostCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRepair2698ProofCostCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ProofCostBefore, debtRepair2698CollectorTo)
	if err != nil {
		return false
	}
	if base == debtRepair2698CollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtRepair2698CollectorTo
		if !debtRepair2698CollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtRepair2698ProofCostUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRepair2698CollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ProofCostBefore, debtRepair2698CollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("proof-cost metadata lost its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRepair2698ProofCostCollectorTo
	return baseline, nil
}

// The cleanup continuation preserves history only; the exact prior debt frame
// and the current census still constrain every transition.
func debtRepair2698CleanupCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRepair2698CleanupCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698CleanupBefore, debtRepair2698ProofCostCollectorTo)
	if err != nil {
		return false
	}
	if base == debtRepair2698ProofCostCollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtRepair2698ProofCostCollectorTo
		if !debtRepair2698ProofCostCollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtRepair2698CleanupUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRepair2698ProofCostCollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698CleanupBefore, debtRepair2698ProofCostCollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("cleanup metadata lost its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRepair2698CleanupCollectorTo
	return baseline, nil
}

func debtRepair2698Round2CollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRepair2698Round2CollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Round2Before, debtRepair2698CleanupCollectorTo)
	if err != nil {
		return false
	}
	if base == debtRepair2698CleanupCollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtRepair2698CleanupCollectorTo
		if !debtRepair2698CleanupCollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtRepair2698Round2UnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRepair2698CleanupCollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698Round2Before, debtRepair2698CleanupCollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("round-2 metadata lost its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRepair2698Round2CollectorTo
	return baseline, nil
}

func debtRepair2698ScheduleCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtRepair2698ScheduleCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ScheduleBefore, debtRepair2698Round2CollectorTo)
	if err != nil {
		return false
	}
	if base == debtRepair2698Round2CollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		intermediate := head
		intermediate.Collector = debtRepair2698Round2CollectorTo
		if !debtRepair2698Round2CollectorTransition(base, intermediate.Collector, trusted, intermediate, proofs...) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtRepair2698ScheduleUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRepair2698Round2CollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtRepair2698ScheduleBefore, debtRepair2698Round2CollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("schedule reader metadata lost its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRepair2698ScheduleCollectorTo
	return baseline, nil
}

type debtCatchUpSourcePacket struct {
	Base, Master, Lane string
	Files              []debtCatchUpSourceFile
}

type debtCatchUpSourceFile struct{ File, ComposedSHA256, MasterSHA256, LaneSHA256 string }

func debtCatchUpReadPacket(root string) (debtCatchUpSourcePacket, error) {
	var packet debtCatchUpSourcePacket
	raw, err := os.ReadFile(filepath.Join(root, "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/catch_up_2542.json"))
	if err != nil {
		return packet, err
	}
	if debtBaselineChecksum(raw) != debtCatchUpHistoryPacketSHA {
		return packet, fmt.Errorf("joined source proof packet changed")
	}
	if cached, ok := debtSelectedReadRolePinCache.Load("catch-up/" + debtCatchUpHistoryPacketSHA); ok {
		packet = cached.(debtCatchUpSourcePacket)
		packet.Files = append([]debtCatchUpSourceFile(nil), packet.Files...)
		return packet, nil
	}
	if err := json.Unmarshal(raw, &packet); err != nil {
		return packet, err
	}
	if packet.Base != debtCatchUpBase || packet.Master != debtCatchUpMaster || packet.Lane != debtCatchUpLane {
		return packet, fmt.Errorf("joined source proof lost an immutable predecessor")
	}
	copy := packet
	copy.Files = append([]debtCatchUpSourceFile(nil), packet.Files...)
	debtSelectedReadRolePinCache.Store("catch-up/"+debtCatchUpHistoryPacketSHA, copy)
	return packet, nil
}

// Ruling6195697761: immutable predecessor evidence only. The independent
// composed-source control still checks every actual file before qualification.
func debtCatchUpLaneSource(root, path string, current []byte) ([]byte, error) {
	var err error
	current, err = debtRepair2698HistoricalSource(root, path, current)
	if err != nil {
		return nil, err
	}
	packet, err := debtCatchUpReadPacket(root)
	if os.IsNotExist(err) {
		return current, nil // Historical archives predate the joined proof packet.
	}
	if err != nil {
		return nil, err
	}
	for _, file := range packet.Files {
		if file.File != path || file.LaneSHA256 == "" || file.ComposedSHA256 == file.LaneSHA256 || debtBaselineChecksum(current) != file.ComposedSHA256 {
			continue
		}
		key := "catch-up/lane/" + root + "/" + path + "/" + file.LaneSHA256
		if cached, ok := debtSelectedReadRolePinCache.Load(key); ok {
			return bytes.Clone(cached.([]byte)), nil
		}
		previous, err := debtGit(root, "show", packet.Lane+":"+path)
		if err != nil || debtBaselineChecksum(previous) != file.LaneSHA256 {
			return nil, fmt.Errorf("joined source proof lost exact lane input %s: %v", path, err)
		}
		debtSelectedReadRolePinCache.Store(key, bytes.Clone(previous))
		return previous, nil
	}
	return current, nil
}

func debtCatchUpLaneDeclarations(root, path string) (map[string]string, error) {
	raw, err := os.ReadFile(filepath.Join(root, path))
	if err == nil {
		raw, err = debtCatchUpLaneSource(root, path, raw)
	}
	if err != nil {
		return nil, err
	}
	return debtServedSetupSourceDeclarations(raw)
}

func debtCatchUpLaneDeclaration(root, path, name, current string) (string, error) {
	packet, err := debtCatchUpReadPacket(root)
	if os.IsNotExist(err) {
		return current, nil
	}
	if err != nil {
		return "", err
	}
	listed := false
	for _, file := range packet.Files {
		listed = listed || file.File == path
	}
	if !listed {
		return current, nil
	}
	actual, err := debtServedSetupDeclarations(filepath.Join(root, path))
	if err != nil {
		return "", err
	}
	if actual[name] != current {
		return current, nil // An older intermediate stays under its own inverse.
	}
	previous, err := debtCatchUpLaneDeclarations(root, path)
	if err != nil {
		return "", err
	}
	if body, found := previous[name]; found {
		return body, nil
	}
	return current, nil
}

func debtCatchUpForkProofDonors(base, source map[string]authorityDebtSite) bool {
	rows := debtForkProofRows()
	rows[2].Multiplicity, rows[4].Multiplicity = 2, 2
	for _, old := range rows {
		if base[old.identity()] != old || source[old.identity()] != old {
			return false
		}
	}
	return true
}

func debtCatchUpSignedBaseline(root, ref, collector string) (authorityDebtBaseline, []byte, error) {
	raw, err := debtGit(root, "show", ref+":"+debtBaselinePath)
	if err != nil {
		return authorityDebtBaseline{}, nil, err
	}
	baseline, err := parseAuthorityDebtBaseline(raw)
	if err != nil || baseline.BootstrapSource != debtBootstrapSource || baseline.Collector != debtCacheCollectorFrom {
		return baseline, nil, fmt.Errorf("joined collector predecessor header changed: %v", err)
	}
	metadata, err := debtGit(root, "show", ref+":"+debtCollectorSidecarPath)
	if err != nil {
		return baseline, nil, err
	}
	if _, err := debtParseCollectorMetadata(metadata, raw, collector); err != nil {
		return baseline, nil, err
	}
	baseline.Collector = collector
	return baseline, metadata, nil
}

func debtCatchUpCollectorSource(root string) error {
	actual, err := debtCollectorDigest(root)
	if err != nil || (actual != debtCatchUpCollectorTo && actual != debtCatchUpFreshnessCollectorTo && actual != debtCatchUpNativeEntryCollectorTo && actual != debtRepair2698CollectorTo && actual != debtRepair2698ProofCostCollectorTo && actual != debtRepair2698CleanupCollectorTo && actual != debtRepair2698Round2CollectorTo && actual != debtRepair2698ScheduleCollectorTo) {
		return fmt.Errorf("joined collector is not its exact enumerated source: %s: %v", actual, err)
	}
	if actual == debtCatchUpFreshnessCollectorTo {
		if err := debtCatchUpFreshnessOwnerSource(root); err != nil {
			return err
		}
	}
	if actual == debtCatchUpNativeEntryCollectorTo || actual == debtRepair2698CollectorTo || actual == debtRepair2698ProofCostCollectorTo || actual == debtRepair2698CleanupCollectorTo || actual == debtRepair2698Round2CollectorTo || actual == debtRepair2698ScheduleCollectorTo {
		if err := debtCatchUpNativeEntryOwnerSource(root); err != nil {
			return err
		}
	}
	packet, err := debtCatchUpReadPacket(root)
	if err != nil {
		return err
	}
	if actual == debtRepair2698CollectorTo || actual == debtRepair2698ProofCostCollectorTo || actual == debtRepair2698CleanupCollectorTo || actual == debtRepair2698Round2CollectorTo || actual == debtRepair2698ScheduleCollectorTo {
		if err := debtRepair2698SourcePolicy(root, packet); err != nil {
			return err
		}
	}
	if actual == debtRepair2698ScheduleCollectorTo {
		raw, err := os.ReadFile(filepath.Join(root, debtRepair2698SchedulePath))
		if err != nil || debtBaselineChecksum(raw) != debtRepair2698ScheduleAfterSHA {
			return fmt.Errorf("schedule reader actual endpoint changed: %v", err)
		}
	}
	for _, file := range packet.Files {
		raw, err := os.ReadFile(filepath.Join(root, file.File))
		if file.ComposedSHA256 == "absent" && os.IsNotExist(err) {
			continue
		}
		if err == nil && (actual == debtRepair2698CollectorTo || actual == debtRepair2698ProofCostCollectorTo || actual == debtRepair2698CleanupCollectorTo || actual == debtRepair2698Round2CollectorTo || actual == debtRepair2698ScheduleCollectorTo) {
			raw, err = debtRepair2698HistoricalSource(root, file.File, raw)
		}
		if err != nil || debtBaselineChecksum(raw) != file.ComposedSHA256 {
			return fmt.Errorf("joined collector current-source mismatch: %s: %v", file.File, err)
		}
	}
	return nil
}

func debtCatchUpCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtCatchUpCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	ref := ""
	switch base {
	case debtServedObservationCollectorTo:
		ref = debtCatchUpLane
	case debtProcessLeaseCollectorTo:
		ref = debtCatchUpMaster
	default:
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, ref, base)
	if err != nil || !debtSitesEqual(trusted.Sites, original.Sites) {
		return false
	}
	return debtCatchUpBothPredecessors(root, proofs[0].Actual, head.Sites) == nil
}

func debtCatchUpBothPredecessors(root string, actual, head map[string]authorityDebtSite) error {
	for _, input := range [][2]string{{debtCatchUpLane, debtServedObservationCollectorTo}, {debtCatchUpMaster, debtProcessLeaseCollectorTo}} {
		prior, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			return err
		}
		failures := authorityDebtRatchet(actual, head, prior.Sites, prior.Sites, debtDeliveryRetirementProof{Root: root, Actual: actual})
		if len(failures) != 0 {
			return fmt.Errorf("joined collector exceeds signed predecessor %s: %v", input[0], failures)
		}
	}
	return nil
}

func debtCatchUpUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || debtCatchUpCollectorSource(root) != nil {
		return baseline, prior
	}
	bound := false
	for _, input := range [][2]string{{debtCatchUpLane, debtServedObservationCollectorTo}, {debtCatchUpMaster, debtProcessLeaseCollectorTo}} {
		_, original, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			return baseline, err
		}
		bound = bound || bytes.Equal(metadata, original)
	}
	if !bound {
		return baseline, prior
	}
	// Downward permission only: the census and ordinary ratchet still run before
	// their owner writes either generated artifact or its new exact checksum.
	if err := debtCatchUpBothPredecessors(root, baseline.Sites, baseline.Sites); err != nil {
		return baseline, err
	}
	baseline.Collector = debtCatchUpCollectorTo
	return baseline, nil
}

func debtCatchUpFreshnessCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtCatchUpFreshnessCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	ref := ""
	switch base {
	case debtCatchUpCollectorTo:
		ref = debtCatchUpFreshnessLane
	case debtProcessLeaseFreshnessCollectorTo:
		ref = debtCatchUpFreshnessInput
	case debtProcessLeaseCollectorTo:
		ref = debtCatchUpMaster
	case debtServedObservationCollectorTo:
		ref = debtCatchUpLane
	default:
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, ref, base)
	if err != nil || !debtSitesEqual(trusted.Sites, original.Sites) {
		return false
	}
	return debtCatchUpFreshnessPredecessors(root, proofs[0].Actual, head.Sites) == nil
}

func debtCatchUpFreshnessPredecessors(root string, actual, head map[string]authorityDebtSite) error {
	for _, input := range [][2]string{{debtCatchUpFreshnessLane, debtCatchUpCollectorTo}, {debtCatchUpFreshnessInput, debtProcessLeaseFreshnessCollectorTo}} {
		prior, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			return err
		}
		failures := authorityDebtRatchet(actual, head, prior.Sites, prior.Sites, debtDeliveryRetirementProof{Root: root, Actual: actual})
		if len(failures) != 0 {
			return fmt.Errorf("freshness composition exceeds signed predecessor %s: %v", input[0], failures)
		}
	}
	return nil
}

func debtCatchUpFreshnessOwnerSource(root string) error {
	if digest, err := debtProcessLeaseCustodyDigest(root); err != nil || digest != debtProcessLeaseCustodyPin {
		return fmt.Errorf("freshness custody lacks its reviewed native owner: %s: %v", digest, err)
	}
	for path, want := range debtCatchUpFreshnessNativePins {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || debtBaselineChecksum(raw) != want {
			return fmt.Errorf("freshness native cleanup/observation source changed: %s: %v", path, err)
		}
	}
	return nil
}

func debtCatchUpNativeEntryCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtCatchUpNativeEntryCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	ref := ""
	switch base {
	case debtCatchUpFreshnessCollectorTo:
		ref = debtCatchUpNativeEntryLane
	case debtProcessLeaseFreshnessCollectorTo:
		ref = debtCatchUpNativeEntryInput
	case debtProcessLeaseCollectorTo:
		ref = debtCatchUpNativeEntryMaster
	default:
		return false
	}
	root := proofs[0].Root
	if debtCatchUpCollectorSource(root) != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, ref, base)
	if err != nil || !debtSitesEqual(trusted.Sites, original.Sites) {
		return false
	}
	return debtCatchUpNativeEntryPredecessors(root, proofs[0].Actual, head.Sites) == nil
}

func debtCatchUpNativeEntryPredecessors(root string, actual, head map[string]authorityDebtSite) error {
	for _, input := range [][2]string{{debtCatchUpNativeEntryLane, debtCatchUpFreshnessCollectorTo}, {debtCatchUpNativeEntryInput, debtProcessLeaseFreshnessCollectorTo}} {
		prior, _, err := debtCatchUpSignedBaseline(root, input[0], input[1])
		if err != nil {
			return err
		}
		failures := authorityDebtRatchet(actual, head, prior.Sites, prior.Sites, debtDeliveryRetirementProof{Root: root, Actual: actual})
		if len(failures) != 0 {
			return fmt.Errorf("native-entry composition exceeds signed predecessor %s: %v", input[0], failures)
		}
	}
	return nil
}

func debtCatchUpNativeEntryOwnerSource(root string) error {
	if digest, err := debtProcessLeaseCustodyDigest(root); err != nil || digest != debtProcessLeaseCustodyPin {
		return fmt.Errorf("native-entry custody lacks its reviewed native owner: %s: %v", digest, err)
	}
	for path, want := range debtCatchUpNativeEntryNativePins {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil || debtBaselineChecksum(raw) != want {
			return fmt.Errorf("native-entry owner/assertion source changed: %s: %v", path, err)
		}
	}
	return nil
}

func debtCatchUpNativeEntryUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtCatchUpFreshnessCollectorTo); err != nil {
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtCatchUpNativeEntryLane, debtCatchUpFreshnessCollectorTo)
	if err != nil || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("native-entry refresh lacks its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtCatchUpNativeEntryCollectorTo
	return baseline, nil
}

func debtCollectorMetadataBytes(metadata debtCollectorMetadata) ([]byte, error) {
	data, err := json.MarshalIndent(metadata, "", "  ")
	return append(data, '\n'), err
}

func debtParseCollectorMetadata(data, baseline []byte, collector string) (debtCollectorMetadata, error) {
	var metadata debtCollectorMetadata
	decoder := json.NewDecoder(bytes.NewReader(data))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&metadata); err != nil {
		return metadata, err
	}
	if decoder.Decode(new(any)) != io.EOF {
		return metadata, fmt.Errorf("trailing collector metadata")
	}
	canonical, err := debtCollectorMetadataBytes(metadata)
	if err != nil || !bytes.Equal(canonical, data) {
		return metadata, fmt.Errorf("noncanonical or duplicate collector metadata")
	}
	if metadata.Version != 1 || metadata.Predecessor != debtCacheCollectorFrom ||
		!authorityDebtHex(metadata.Collector, 64) || metadata.Collector != collector ||
		metadata.BaselineSHA256 != debtBaselineChecksum(baseline) {
		return metadata, fmt.Errorf("collector sidecar does not bind exact baseline and current analyzer")
	}
	return metadata, nil
}

func debtBaselineWithCollector(root string, data []byte, collector string, historical bool) (authorityDebtBaseline, error) {
	baseline, err := parseAuthorityDebtBaseline(data)
	if err != nil {
		return baseline, err
	}
	metadataBytes, err := os.ReadFile(filepath.Join(root, debtCollectorSidecarPath))
	if os.IsNotExist(err) && historical && collector == debtCacheCollectorFrom && baseline.Collector == collector {
		// Only the exact immutable pre-cutover archive has historical header authority.
		return baseline, nil
	}
	if err != nil {
		return baseline, fmt.Errorf("read mandatory collector sidecar: %w", err)
	}
	if _, err := debtParseCollectorMetadata(metadataBytes, data, collector); err != nil {
		if current, transitionErr, handled := debtSourceUnrefreshedBaseline(root, metadataBytes, data, baseline, collector, historical, err); handled {
			return current, transitionErr
		}
		if collector == debtReceiverReadbackCollectorTo {
			if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
				return baseline, err
			}
			if _, priorErr := debtParseCollectorMetadata(metadataBytes, data, debtPublicA2EndpointCollectorTo); priorErr != nil {
				return baseline, priorErr
			}
			if baseline.Collector != debtCacheCollectorFrom {
				return baseline, fmt.Errorf("historical TSV collector header changed")
			}
			if err := debtServedDebugSourcePins(root); err != nil {
				return baseline, err
			}
			baseline.Collector = collector
			return baseline, nil
		}
		if collector == debtPublicA2EndpointCollectorTo {
			if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
				return baseline, err
			}
			if priorErr := debtPublicA2EndpointUnrefreshedMetadata(root, metadataBytes, data, baseline); priorErr != nil {
				return baseline, priorErr
			}
			baseline.Collector = collector
			return baseline, nil
		}
		if collector == debtServedDebugCollectorTo {
			if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
				return baseline, err
			}
			if priorErr := debtServedDebugUnrefreshedMetadata(root, metadataBytes, data, baseline); priorErr != nil {
				return baseline, priorErr
			}
			baseline.Collector = collector
			return baseline, nil
		}
		if historical || collector != debtWriterStageCollectorTo || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
			return baseline, err
		}
		if _, priorErr := debtParseCollectorMetadata(metadataBytes, data, debtStageCompletionCollectorTo); priorErr != nil {
			return baseline, err
		}
		for _, pair := range debtWriterStageSignatureTransitions() {
			if baseline.Sites[pair[0].identity()] != pair[0] || baseline.Sites[pair[1].identity()].Multiplicity != 0 {
				return baseline, fmt.Errorf("unrefreshed writer-stage baseline lacks exact donor")
			}
		}
		if err := debtWriterStageSourcePins(root, true); err != nil {
			return baseline, err
		}
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	baseline.Collector = collector
	return baseline, nil
}

func debtSourceUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, collector string, historical bool, prior error) (authorityDebtBaseline, error, bool) {
	var owner func(string, []byte, []byte, authorityDebtBaseline, bool, error) (authorityDebtBaseline, error)
	switch collector {
	case debtRepair2698ScheduleCollectorTo:
		owner = debtRepair2698ScheduleUnrefreshedBaseline
	case debtRepair2698Round2CollectorTo:
		owner = debtRepair2698Round2UnrefreshedBaseline
	case debtRepair2698CleanupCollectorTo:
		owner = debtRepair2698CleanupUnrefreshedBaseline
	case debtRepair2698ProofCostCollectorTo:
		owner = debtRepair2698ProofCostUnrefreshedBaseline
	case debtRepair2698CollectorTo:
		owner = debtRepair2698UnrefreshedBaseline
	case debtCatchUpNativeEntryCollectorTo:
		owner = debtCatchUpNativeEntryUnrefreshedBaseline
	case debtCatchUpCollectorTo:
		owner = debtCatchUpUnrefreshedBaseline
	case debtWorkflowStateCollectorTo:
		owner = debtWorkflowStateUnrefreshedBaseline
	case debtEventNameCollectorTo:
		owner = debtEventNameUnrefreshedBaseline
	case debtDeliveryRetirementCollectorTo:
		owner = debtDeliveryRetirementUnrefreshedBaseline
	case debtDeliveryStatusCollectorTo:
		owner = debtDeliveryStatusUnrefreshedBaseline
	case debtRunScalarCollectorTo:
		owner = debtRunScalarUnrefreshedBaseline
	case debtRequestKeyCollectorTo:
		owner = debtRequestKeyUnrefreshedBaseline
	default:
		return baseline, prior, false
	}
	current, err := owner(root, metadata, data, baseline, historical, prior)
	return current, err, true
}

func debtDeliveryStatusUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRunScalarCollectorTo); err != nil {
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	if err := debtDeliveryStatusSourcePins(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtDeliveryStatusCollectorTo
	return baseline, nil
}

func debtDeliveryRetirementUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRunScalarCollectorTo); err != nil {
		return baseline, err // The unqualified delivery wave never refreshed this sidecar.
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	if err := debtDeliveryStatusSourcePins(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtDeliveryRetirementCollectorTo
	return baseline, nil
}

func debtEventNameUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtDeliveryRetirementCollectorTo); err != nil {
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	if err := debtEventNameSourcePins(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtEventNameCollectorTo
	return baseline, nil
}

func debtWorkflowStateUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtEventNameCollectorTo); err != nil {
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	if err := debtWorkflowStateSourcePins(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtWorkflowStateCollectorTo
	return baseline, nil
}

func debtRunScalarUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRequestKeyCollectorTo); err != nil {
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	if err := debtRunScalarSourcePins(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRunScalarCollectorTo
	return baseline, nil
}

func debtRequestKeyUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if err := debtRequestKeyUnrefreshedMetadata(root, metadata, data, baseline); err != nil {
		return baseline, err
	}
	baseline.Collector = debtRequestKeyCollectorTo
	return baseline, nil
}

func debtRequestKeyUnrefreshedMetadata(root string, metadata, data []byte, baseline authorityDebtBaseline) error {
	if baseline.Collector != debtCacheCollectorFrom {
		return fmt.Errorf("historical TSV collector header changed")
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtReceiverReadbackCollectorTo); err != nil {
		return err
	}
	for _, pair := range debtRequestKeySignatureTransitions() {
		if baseline.Sites[pair[0].identity()] != pair[0] || baseline.Sites[pair[1].identity()].Multiplicity != 0 {
			return fmt.Errorf("unrefreshed request-key baseline lacks exact retained call")
		}
	}
	return debtRequestKeySourcePins(root)
}

func debtServedDebugUnrefreshedMetadata(root string, metadata, data []byte, baseline authorityDebtBaseline) error {
	if baseline.Collector != debtCacheCollectorFrom {
		return fmt.Errorf("historical TSV collector header changed")
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtWriterStageCollectorTo); err != nil {
		return err
	}
	for _, pair := range debtServedDebugSignatureTransitions() {
		if baseline.Sites[pair[0].identity()] != pair[0] || baseline.Sites[pair[1].identity()].Multiplicity != 0 {
			return fmt.Errorf("unrefreshed served-debug baseline lacks exact donor")
		}
	}
	return debtServedDebugSourcePins(root)
}

func debtPublicA2EndpointUnrefreshedMetadata(root string, metadata, data []byte, baseline authorityDebtBaseline) error {
	if baseline.Collector != debtCacheCollectorFrom {
		return fmt.Errorf("historical TSV collector header changed")
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtServedDebugCollectorTo); err != nil {
		return err
	}
	ancestor, _ := debtQuiescenceUncertaintyTransition()
	old, current := debtPublicA2EndpointTransition()
	if baseline.Sites[ancestor.identity()] != ancestor && baseline.Sites[old.identity()] != old || baseline.Sites[current.identity()].Multiplicity != 0 {
		return fmt.Errorf("unrefreshed A2 uncertainty lacks exact predecessor donor")
	}
	if _, _, _, failures := debtPublicA2EndpointComparisons(map[string]authorityDebtSite{current.identity(): current}, baseline.Sites, baseline.Sites, baseline.Sites); len(failures) != 0 {
		return fmt.Errorf("unrefreshed A2 uncertainty has contradictory donors: %v", failures)
	}
	return debtPublicA2EndpointSourcePins(root)
}
