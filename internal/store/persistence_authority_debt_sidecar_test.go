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
		switch collector {
		case debtDeliveryRetirementCollectorTo:
			return debtDeliveryRetirementUnrefreshedBaseline(root, metadataBytes, data, baseline, historical, err)
		case debtDeliveryStatusCollectorTo:
			return debtDeliveryStatusUnrefreshedBaseline(root, metadataBytes, data, baseline, historical, err)
		case debtRunScalarCollectorTo:
			return debtRunScalarUnrefreshedBaseline(root, metadataBytes, data, baseline, historical, err)
		case debtRequestKeyCollectorTo:
			return debtRequestKeyUnrefreshedBaseline(root, metadataBytes, data, baseline, historical, err)
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
