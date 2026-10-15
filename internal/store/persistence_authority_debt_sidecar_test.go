package store_test

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
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
	if err != nil || actual != debtCatchUpCollectorTo {
		return fmt.Errorf("joined collector is not its exact enumerated source: %s: %v", actual, err)
	}
	packet, err := debtCatchUpReadPacket(root)
	if err != nil {
		return err
	}
	for _, file := range packet.Files {
		raw, err := os.ReadFile(filepath.Join(root, file.File))
		if file.ComposedSHA256 == "absent" && os.IsNotExist(err) {
			continue
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
