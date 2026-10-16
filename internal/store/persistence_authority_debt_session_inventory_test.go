package store_test

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"path/filepath"
)

const debtSessionInventoryPacketPath = "tools/fixture-codemod/pipeline-observations/internal/selectedreadrole/testdata/session_inventory_2577_successor.json"
const debtSessionInventorySource = "internal/runtime/channel_runtime_supported_surface_test.go"
const debtSessionInventoryBefore = "f98083f8cef12a0b02e894b729b87c8b9f634921"
const debtSessionInventoryAfter = "f9802ccaf6938dd3744aed977af40455aa588079"
const debtSessionInventoryPacketSHA = "af233288a071ff624ecc9d0eab1a52b1bb1398741b21036e16ebe556e0a8140e"

func debtSessionInventoryReadPacket(root string) (debtRepair2698SourcePacket, error) {
	var packet debtRepair2698SourcePacket
	raw, err := os.ReadFile(filepath.Join(root, debtSessionInventoryPacketPath))
	if err != nil || debtBaselineChecksum(raw) != debtSessionInventoryPacketSHA {
		return packet, fmt.Errorf("session inventory continuation certificate changed or missing: %v", err)
	}
	decoder := json.NewDecoder(bytes.NewReader(raw))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(&packet); err != nil {
		return packet, err
	}
	if decoder.Decode(new(any)) != io.EOF || packet.BeforeCommit != debtSessionInventoryBefore ||
		packet.AfterCommit != debtSessionInventoryAfter || len(packet.Files) != 1 ||
		packet.Files[0].File != debtSessionInventorySource ||
		packet.Authorization != "https://github.com/division-sh/swarm/issues/2577#issuecomment-6223472352" {
		return packet, fmt.Errorf("session inventory continuation lost its exact one-file boundary")
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
	if err := debtSessionInventoryExactSelection(before, after); err != nil {
		return packet, err
	}
	return packet, nil
}

func debtSessionInventoryExactSelection(before, after []byte) error {
	replacements := [][2]string{
		{"\tif err != nil || len(plans) != 1 {\n\t\tt.Fatalf(\"CompileChannelInventory = %#v, %v\", plans, err)\n\t}\n\ttriggerEntry", "\tif err != nil {\n\t\tt.Fatal(\"CompileChannelInventory\", err)\n\t}\n\tplan := packfixture.ChannelPlanByID(t, plans, \"provider.telegram.hitl_channel\")\n\ttriggerEntry"},
		{"channelIdentity := plans[0].ChannelIdentity()", "channelIdentity := plan.ChannelIdentity()"},
		{"structuralSubject, err := plans[0].CapabilitySubject()", "structuralSubject, err := plan.CapabilitySubject()"},
		{"packs.NewOutboundBindingPlanWithCredentials(\"ops\", plans[0], \"42\", nil,", "packs.NewOutboundBindingPlanWithCredentials(\"ops\", plan, \"42\", nil,"},
	}
	expected := bytes.Clone(before)
	for _, replacement := range replacements {
		old, next := []byte(replacement[0]), []byte(replacement[1])
		if bytes.Count(expected, old) != 1 {
			return fmt.Errorf("session inventory continuation lost its exact selection site")
		}
		expected = bytes.Replace(expected, old, next, 1)
	}
	if !bytes.Equal(after, expected) {
		return fmt.Errorf("session inventory continuation changed more than canonical plan selection")
	}
	return nil
}

// This inverse never supplies the live census with historical source.
func debtSessionInventoryHistoricalSource(root, path string, actual []byte) ([]byte, error) {
	if path != debtSessionInventorySource {
		return actual, nil
	}
	if _, err := os.Stat(filepath.Join(root, debtSessionInventoryPacketPath)); os.IsNotExist(err) &&
		debtBaselineChecksum(actual) != "488baa15e9b201b364533aa720989004d010b2a7ad9f29e482e7695a3322014f" {
		return actual, nil // Immutable predecessor images predate this packet.
	}
	packet, err := debtSessionInventoryReadPacket(root)
	if err != nil {
		return nil, err
	}
	if err := debtSessionInventoryCurrentSource(root, actual, packet); err != nil {
		return nil, err
	}
	before, err := debtRepair2698SignedSource(root, packet.BeforeCommit, path, packet.Files[0].BeforeSHA256)
	if err != nil {
		return nil, err
	}
	if _, err := debtSessionInventoryReadPacket(root); err != nil {
		return nil, err
	}
	if err := debtSessionInventoryCurrentSource(root, actual, packet); err != nil {
		return nil, err
	}
	return before, nil
}

func debtSessionInventoryCurrentSource(root string, actual []byte, packet debtRepair2698SourcePacket) error {
	current, err := os.ReadFile(filepath.Join(root, debtSessionInventorySource))
	if err != nil || !bytes.Equal(current, actual) || debtBaselineChecksum(current) != packet.Files[0].AfterSHA256 {
		return fmt.Errorf("session inventory continuation differs from actual copied source: %v", err)
	}
	return nil
}

func debtSessionInventorySourcePolicy(root string) error {
	packet, err := debtSessionInventoryReadPacket(root)
	if err != nil {
		return err
	}
	actual, err := os.ReadFile(filepath.Join(root, debtSessionInventorySource))
	if err != nil {
		return err
	}
	if err := debtSessionInventoryCurrentSource(root, actual, packet); err != nil {
		return err
	}
	_, err = debtSessionInventoryHistoricalSource(root, debtSessionInventorySource, actual)
	return err
}

func debtRepair2698HasSourcePolicy(collector string) bool {
	switch collector {
	case debtRepair2698CollectorTo, debtRepair2698ProofCostCollectorTo,
		debtRepair2698CleanupCollectorTo, debtRepair2698Round2CollectorTo, debtSessionInventoryCollectorTo:
		return true
	}
	return false
}

func debtSessionInventoryCollectorTransition(base, current string, trusted, head authorityDebtBaseline, proofs ...debtDeliveryRetirementProof) bool {
	if current != debtSessionInventoryCollectorTo || head.Collector != current || trusted.Collector != base ||
		trusted.BootstrapSource != debtBootstrapSource || head.BootstrapSource != debtBootstrapSource || len(proofs) != 1 {
		return false
	}
	root := proofs[0].Root
	if err := debtCatchUpCollectorSource(root); err != nil {
		return false
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtSessionInventoryBefore, debtRepair2698Round2CollectorTo)
	if err != nil {
		return false
	}
	if base == debtRepair2698Round2CollectorTo {
		if !debtSitesEqual(trusted.Sites, original.Sites) {
			return false
		}
	} else {
		// Preserve B's existing authenticated edge during the unlanded trial.
		// Final qualification is anchored to B's actual landed predecessor.
		priorProof := debtDeliveryRetirementProof{Root: root, Actual: original.Sites}
		if !debtRepair2698Round2CollectorTransition(base, original.Collector, trusted, original, priorProof) {
			return false
		}
	}
	return len(authorityDebtRatchet(proofs[0].Actual, head.Sites, original.Sites, original.Sites, proofs[0])) == 0
}

func debtSessionInventoryUnrefreshedBaseline(root string, metadata, data []byte, baseline authorityDebtBaseline, historical bool, prior error) (authorityDebtBaseline, error) {
	if historical || os.Getenv(debtRefreshEnv) != "downward" || os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
		return baseline, prior
	}
	if _, err := debtParseCollectorMetadata(metadata, data, debtRepair2698Round2CollectorTo); err != nil {
		return baseline, err
	}
	original, _, err := debtCatchUpSignedBaseline(root, debtSessionInventoryBefore, debtRepair2698Round2CollectorTo)
	if err != nil || baseline.Collector != debtCacheCollectorFrom || baseline.BootstrapSource != debtBootstrapSource || !debtSitesEqual(baseline.Sites, original.Sites) {
		return baseline, fmt.Errorf("session inventory metadata lost its exact signed predecessor: %v", err)
	}
	if err := debtCatchUpCollectorSource(root); err != nil {
		return baseline, err
	}
	baseline.Collector = debtSessionInventoryCollectorTo
	return baseline, nil
}
