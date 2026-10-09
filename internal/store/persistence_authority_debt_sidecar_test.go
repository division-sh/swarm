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
		return baseline, err
	}
	if baseline.Collector != debtCacheCollectorFrom {
		return baseline, fmt.Errorf("historical TSV collector header changed")
	}
	baseline.Collector = collector
	return baseline, nil
}
