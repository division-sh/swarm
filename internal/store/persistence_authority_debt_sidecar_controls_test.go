package store_test

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestPersistenceAuthorityDebtCollectorSidecarRejectsForeignMetadata(t *testing.T) {
	baseline := []byte("unchanged TSV bytes\n")
	collector := strings.Repeat("a", 64)
	valid := debtCollectorMetadata{Version: 1, Collector: collector, BaselineSHA256: debtBaselineChecksum(baseline), Predecessor: debtCacheCollectorFrom}
	for _, mutation := range []string{"none", "version", "collector", "predecessor", "baseline", "duplicate", "unknown", "truncated", "trailing"} {
		t.Run(mutation, func(t *testing.T) {
			metadata := valid
			switch mutation {
			case "version":
				metadata.Version++
			case "collector":
				metadata.Collector = strings.Repeat("b", 64)
			case "predecessor":
				metadata.Predecessor = strings.Repeat("c", 64)
			case "baseline":
				metadata.BaselineSHA256 = debtBaselineChecksum(append(baseline, 'x'))
			}
			data, err := debtCollectorMetadataBytes(metadata)
			if err != nil {
				t.Fatal(err)
			}
			switch mutation {
			case "duplicate":
				data = bytes.Replace(data, []byte("{\n"), []byte("{\n  \"version\": 1,\n"), 1)
			case "unknown":
				data = bytes.Replace(data, []byte("{\n"), []byte("{\n  \"foreign\": true,\n"), 1)
			case "truncated":
				data = data[:len(data)/2]
			case "trailing":
				data = append(data, []byte("{}")...)
			}
			_, err = debtParseCollectorMetadata(data, baseline, collector)
			if (err == nil) != (mutation == "none") {
				t.Fatalf("metadata admission: %v", err)
			}
		})
	}
}

func TestPersistenceAuthorityDebtCollectorSidecarRequiresCurrentMetadata(t *testing.T) {
	root := t.TempDir()
	baseline := marshalAuthorityDebtBaseline(authorityDebtBaseline{BootstrapSource: strings.Repeat("a", 40), Collector: debtCacheCollectorFrom, Sites: debtControlSet(debtControlSite("call:QueryRow", 2))})
	if _, err := debtBaselineWithCollector(root, baseline, debtCacheCollectorFrom, true); err != nil {
		t.Fatal("exact historical archive rejected:", err)
	}
	if _, err := debtBaselineWithCollector(root, baseline, debtCacheCollectorFrom, false); err == nil {
		t.Fatal("missing current sidecar admitted")
	}
	collector := strings.Repeat("b", 64)
	metadata, _ := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: collector, BaselineSHA256: debtBaselineChecksum(baseline), Predecessor: debtCacheCollectorFrom})
	debtWriteModuleSource(t, root, debtCollectorSidecarPath, string(metadata))
	head, err := debtBaselineWithCollector(root, baseline, collector, false)
	if err != nil || head.Collector != collector {
		t.Fatalf("sidecar collector: %v", err)
	}
	if _, err := debtBaselineWithCollector(root, append(baseline, '\n'), collector, false); err == nil {
		t.Fatal("changed TSV accepted")
	}
	trusted, _ := parseAuthorityDebtBaseline(baseline)
	if err := debtValidateCollectorIdentity(debtCacheCollectorFrom, collector, trusted, head, collector); err != nil {
		t.Fatal(err)
	}
	changed := head
	changed.Sites = debtControlSet(debtControlSite("call:QueryRow", 3))
	if err := debtValidateCollectorIdentity(debtCacheCollectorFrom, collector, trusted, changed, collector); err == nil {
		t.Fatal("sidecar transition increased debt permission")
	}
	if err := debtValidateCollectorIdentity(debtCacheCollectorFrom, collector, trusted, head, strings.Repeat("c", 64)); err == nil {
		t.Fatal("foreign transition admitted")
	}
}

func TestPersistenceAuthorityDebtCollectorMetadataIdentity(t *testing.T) {
	root := persistenceAuthorityRepoRoot(t)
	collector, err := debtCollectorDigest(root)
	if err != nil {
		t.Fatal(err)
	}
	baseline, err := os.ReadFile(filepath.Join(root, debtBaselinePath))
	if err != nil {
		t.Fatal(err)
	}
	if os.Getenv("SWARM_REFRESH_COLLECTOR_METADATA") == "1" {
		if os.Getenv("CI") != "" || os.Getenv("GITHUB_ACTIONS") != "" {
			t.Fatal("CI cannot refresh collector metadata")
		}
		metadata, err := debtCollectorMetadataBytes(debtCollectorMetadata{Version: 1, Collector: collector, BaselineSHA256: debtBaselineChecksum(baseline), Predecessor: debtCacheCollectorFrom})
		if err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(root, debtCollectorSidecarPath), metadata, 0644); err != nil {
			t.Fatal(err)
		}
	}
	if _, err := debtBaselineWithCollector(root, baseline, collector, false); err != nil {
		t.Fatal(err)
	}
	t.Logf("sidecar collector=%s baseline=%s", collector, debtBaselineChecksum(baseline))
}
