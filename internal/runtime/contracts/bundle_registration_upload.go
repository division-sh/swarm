package contracts

import (
	"encoding/base64"
	"encoding/json"
	"fmt"
	"os"
	"sort"
	"strings"
	"unicode/utf8"
)

const (
	bundleRegistrationEnvelopeAPIVersion = "swarm.bundle.register.v1"
	bundleRegistrationDataAPIVersion     = "swarm.bundle.data.v1"
)

type BundleRegistrationUpload struct {
	ContentYAML string
	DataBlob    *BundleRegisterDataBlobV1
}

type BundleRegisterDataBlobV1 struct {
	APIVersion string                      `json:"api_version"`
	Entries    []BundleRegisterDataEntryV1 `json:"entries"`
}

type BundleRegisterDataEntryV1 struct {
	Path       string `json:"path"`
	DataBase64 string `json:"data_base64"`
}

type bundleRegistrationEnvelopeUploadV1 struct {
	APIVersion string                         `yaml:"api_version"`
	Files      []bundleRegistrationUploadFile `yaml:"files"`
}

type bundleRegistrationUploadFile struct {
	Path string `yaml:"path"`
	Text string `yaml:"text"`
}

// BuildBundleRegistrationDirectoryUpload packages a local contracts directory
// into the public bundle.register request shape without computing bundle_hash
// or building a catalog projection.
func BuildBundleRegistrationDirectoryUpload(repoRoot, contractsRoot, platformSpecPath string) (BundleRegistrationUpload, error) {
	bundle, err := LoadWorkflowContractBundleWithOverrides(repoRoot, contractsRoot, platformSpecPath)
	if err != nil {
		return BundleRegistrationUpload{}, err
	}
	if err := ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return BundleRegistrationUpload{}, err
	}
	entries, err := bundleHashEntries(bundle)
	if err != nil {
		return BundleRegistrationUpload{}, err
	}
	files := make([]bundleRegistrationUploadFile, 0, len(entries))
	var dataEntries []BundleRegisterDataEntryV1
	for _, entry := range entries {
		if entry.Label == "platform/platform-spec.yaml" {
			continue
		}
		rel, ok := strings.CutPrefix(entry.Label, "bundle/")
		if !ok {
			return BundleRegistrationUpload{}, fmt.Errorf("bundle registration input %q is not under bundle/", entry.Label)
		}
		raw, err := os.ReadFile(entry.Path)
		if err != nil {
			return BundleRegistrationUpload{}, fmt.Errorf("read %s: %w", rel, err)
		}
		switch entry.Policy {
		case bundleHashRaw:
			if err := validateBundleRegistrationUploadDataPath(rel); err != nil {
				return BundleRegistrationUpload{}, err
			}
			dataEntries = append(dataEntries, BundleRegisterDataEntryV1{
				Path:       rel,
				DataBase64: base64.StdEncoding.EncodeToString(raw),
			})
		case bundleHashYAML, bundleHashPrompt:
			if !utf8.Valid(raw) {
				return BundleRegistrationUpload{}, fmt.Errorf("text bundle input %s is not valid UTF-8", rel)
			}
			files = append(files, bundleRegistrationUploadFile{Path: rel, Text: string(raw)})
		default:
			return BundleRegistrationUpload{}, fmt.Errorf("unknown bundle registration input policy %d for %s", entry.Policy, rel)
		}
	}
	if len(files) == 0 {
		return BundleRegistrationUpload{}, fmt.Errorf("bundle registration directory has no text inputs")
	}
	sort.Slice(files, func(i, j int) bool {
		return files[i].Path < files[j].Path
	})
	sort.Slice(dataEntries, func(i, j int) bool {
		return dataEntries[i].Path < dataEntries[j].Path
	})
	envelope, err := encodeBundleRegistrationEnvelopeYAML(files)
	if err != nil {
		return BundleRegistrationUpload{}, err
	}
	upload := BundleRegistrationUpload{ContentYAML: envelope}
	if len(dataEntries) > 0 {
		upload.DataBlob = &BundleRegisterDataBlobV1{
			APIVersion: bundleRegistrationDataAPIVersion,
			Entries:    dataEntries,
		}
	}
	return upload, nil
}

func encodeBundleRegistrationEnvelopeYAML(files []bundleRegistrationUploadFile) (string, error) {
	var builder strings.Builder
	builder.WriteString("api_version: ")
	apiVersion, err := json.Marshal(bundleRegistrationEnvelopeAPIVersion)
	if err != nil {
		return "", err
	}
	builder.Write(apiVersion)
	builder.WriteString("\nfiles:\n")
	for _, file := range files {
		path, err := json.Marshal(file.Path)
		if err != nil {
			return "", err
		}
		text, err := json.Marshal(file.Text)
		if err != nil {
			return "", err
		}
		builder.WriteString("  - path: ")
		builder.Write(path)
		builder.WriteString("\n    text: ")
		builder.Write(text)
		builder.WriteString("\n")
	}
	return builder.String(), nil
}

func validateBundleRegistrationUploadDataPath(path string) error {
	segments := strings.Split(path, "/")
	if !bundleRegistrationUploadDataPathIsFlowData(segments) {
		return fmt.Errorf("raw data input %s cannot be represented in BundleRegisterDataBlobV1; data entries must be under a flow data directory (.../flows/<flow>/data/...)", path)
	}
	return nil
}

func bundleRegistrationUploadDataPathIsFlowData(segments []string) bool {
	for i := 0; i+3 < len(segments); i++ {
		if segments[i] == "flows" && segments[i+1] != "" && segments[i+2] == "data" {
			return true
		}
	}
	return false
}
