package providerconnectors

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"io/fs"
	"path"
	"sort"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/httpresponsesuccess"
	"gopkg.in/yaml.v3"
)

const (
	CatalogSchemaVersion     = "1"
	CatalogGeneratorVersion  = "swarm-openapi-gen/v1"
	generatedPackIndexFile   = "catalog/generated-packs.yaml"
	generatedProfileRoot     = "catalog/generator-profiles"
	GeneratedPackKindBuiltin = "builtin"
	GeneratedPackKindFixture = "fixture"
	GenerationReviewApproved = "approved"
	GenerationFixturePassing = "passing"
)

type GenerationEvidence struct {
	SchemaVersion    string                        `yaml:"schema_version"`
	GeneratorVersion string                        `yaml:"generator_version"`
	Source           GenerationSourceEvidence      `yaml:"source"`
	Profile          GenerationProfileEvidence     `yaml:"profile"`
	Operations       []GenerationOperationEvidence `yaml:"operations"`
}

type GenerationSourceEvidence struct {
	Path           string `yaml:"path"`
	OpenAPIVersion string `yaml:"openapi_version"`
	SHA256         string `yaml:"sha256"`
}

type GenerationProfileEvidence struct {
	Path          string `yaml:"path"`
	SchemaVersion string `yaml:"schema_version"`
	SHA256        string `yaml:"sha256"`
}

type GenerationPermission struct {
	ID   string `yaml:"id"`
	Note string `yaml:"note"`
}

type GenerationOperationEvidence struct {
	OperationID     string                               `yaml:"operation_id"`
	ToolID          string                               `yaml:"tool_id"`
	Permissions     []GenerationPermission               `yaml:"permissions"`
	ResponseSuccess runtimecontracts.HTTPResponseSuccess `yaml:"response_success"`
	FixtureID       string                               `yaml:"fixture_id"`
	FixtureStatus   string                               `yaml:"fixture_status"`
	ReviewStatus    string                               `yaml:"review_status"`
}

func (g GenerationEvidence) Validate(provider string, tools map[string]runtimecontracts.ToolSchemaEntry) error {
	if strings.TrimSpace(g.SchemaVersion) != CatalogSchemaVersion {
		return fmt.Errorf("generation schema_version must be %q", CatalogSchemaVersion)
	}
	if strings.TrimSpace(g.GeneratorVersion) != CatalogGeneratorVersion {
		return fmt.Errorf("generation generator_version must be %q", CatalogGeneratorVersion)
	}
	if strings.TrimSpace(g.Source.Path) == "" || strings.TrimSpace(g.Source.OpenAPIVersion) == "" {
		return fmt.Errorf("generation source path and openapi_version are required")
	}
	if err := validateSHA256("generation source", g.Source.SHA256); err != nil {
		return err
	}
	if strings.TrimSpace(g.Profile.Path) == "" || strings.TrimSpace(g.Profile.SchemaVersion) != CatalogSchemaVersion {
		return fmt.Errorf("generation profile path is required and schema_version must be %q", CatalogSchemaVersion)
	}
	if err := validateSHA256("generation profile", g.Profile.SHA256); err != nil {
		return err
	}
	if len(g.Operations) == 0 {
		return fmt.Errorf("generation operations are required")
	}
	seenOperations := map[string]struct{}{}
	seenTools := map[string]struct{}{}
	for _, operation := range g.Operations {
		operationID := strings.TrimSpace(operation.OperationID)
		toolID := strings.TrimSpace(operation.ToolID)
		if operationID == "" || toolID == "" {
			return fmt.Errorf("generation operation_id and tool_id are required")
		}
		if _, exists := seenOperations[operationID]; exists {
			return fmt.Errorf("generation contains duplicate operation_id %q", operationID)
		}
		seenOperations[operationID] = struct{}{}
		if _, exists := seenTools[toolID]; exists {
			return fmt.Errorf("generation contains duplicate tool_id %q", toolID)
		}
		seenTools[toolID] = struct{}{}
		tool, exists := tools[toolID]
		if !exists {
			return fmt.Errorf("generation operation %q references unknown tool_id %q", operationID, toolID)
		}
		toolProvider, _, ok := splitToolID(toolID)
		if !ok || toolProvider != normalizeToken(provider) {
			return fmt.Errorf("generation tool_id %q does not match provider %q", toolID, provider)
		}
		if len(operation.Permissions) == 0 {
			return fmt.Errorf("generation operation %q permissions are required", operationID)
		}
		seenPermissions := map[string]struct{}{}
		for _, permission := range operation.Permissions {
			permissionID := strings.TrimSpace(permission.ID)
			if permissionID == "" || strings.TrimSpace(permission.Note) == "" {
				return fmt.Errorf("generation operation %q permission id and note are required", operationID)
			}
			if _, exists := seenPermissions[permissionID]; exists {
				return fmt.Errorf("generation operation %q contains duplicate permission id %q", operationID, permissionID)
			}
			seenPermissions[permissionID] = struct{}{}
		}
		if err := httpresponsesuccess.Validate(operation.ResponseSuccess); err != nil {
			return fmt.Errorf("generation operation %q: %w", operationID, err)
		}
		if tool.ResponseSuccess == nil || !responseSuccessEqual(*tool.ResponseSuccess, operation.ResponseSuccess) {
			return fmt.Errorf("generation operation %q response_success does not match tool %q", operationID, toolID)
		}
		if strings.TrimSpace(operation.FixtureID) == "" || strings.TrimSpace(operation.FixtureStatus) != GenerationFixturePassing {
			return fmt.Errorf("generation operation %q requires fixture_id and fixture_status %q", operationID, GenerationFixturePassing)
		}
		if strings.TrimSpace(operation.ReviewStatus) != GenerationReviewApproved {
			return fmt.Errorf("generation operation %q review_status must be %q", operationID, GenerationReviewApproved)
		}
	}
	if len(seenTools) != len(tools) {
		missing := make([]string, 0)
		for toolID := range tools {
			if _, exists := seenTools[toolID]; !exists {
				missing = append(missing, toolID)
			}
		}
		sort.Strings(missing)
		return fmt.Errorf("generation operation set does not cover connector tools: %s", strings.Join(missing, ", "))
	}
	return nil
}

func (g GenerationEvidence) OperationForTool(toolID string) (GenerationOperationEvidence, bool) {
	toolID = strings.TrimSpace(toolID)
	for _, operation := range g.Operations {
		if strings.TrimSpace(operation.ToolID) == toolID {
			return operation, true
		}
	}
	return GenerationOperationEvidence{}, false
}

type GeneratedPackIndex struct {
	SchemaVersion string                    `yaml:"schema_version"`
	Packs         []GeneratedPackIndexEntry `yaml:"packs"`
}

type GeneratedPackIndexEntry struct {
	ID       string `yaml:"id"`
	Provider string `yaml:"provider"`
	Profile  string `yaml:"profile"`
	Kind     string `yaml:"kind"`
	Output   string `yaml:"output"`
}

func loadGeneratedPackIndex(fsys fs.FS) (GeneratedPackIndex, error) {
	body, err := fs.ReadFile(fsys, generatedPackIndexFile)
	if err != nil {
		return GeneratedPackIndex{}, fmt.Errorf("read generated connector pack index: %w", err)
	}
	var index GeneratedPackIndex
	if err := decodeYAMLStrict(body, &index); err != nil {
		return GeneratedPackIndex{}, fmt.Errorf("parse generated connector pack index: %w", err)
	}
	if err := index.Validate(fsys); err != nil {
		return GeneratedPackIndex{}, err
	}
	return index, nil
}

func decodeYAMLStrict(body []byte, target any) error {
	decoder := yaml.NewDecoder(bytes.NewReader(body))
	decoder.KnownFields(true)
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var extra any
	if err := decoder.Decode(&extra); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple YAML documents are forbidden")
		}
		return fmt.Errorf("decode trailing YAML document: %w", err)
	}
	return nil
}

func (i GeneratedPackIndex) Validate(fsys fs.FS) error {
	if strings.TrimSpace(i.SchemaVersion) != CatalogSchemaVersion {
		return fmt.Errorf("generated connector pack index schema_version must be %q", CatalogSchemaVersion)
	}
	if len(i.Packs) == 0 {
		return fmt.Errorf("generated connector pack index packs are required")
	}
	seenIDs := map[string]struct{}{}
	seenProfiles := map[string]struct{}{}
	seenOutputs := map[string]struct{}{}
	for _, entry := range i.Packs {
		id := strings.TrimSpace(entry.ID)
		provider := normalizeToken(entry.Provider)
		profile := strings.TrimSpace(entry.Profile)
		kind := strings.TrimSpace(entry.Kind)
		output := strings.Trim(strings.TrimSpace(entry.Output), "/")
		if id == "" || provider == "" || profile == "" || output == "" {
			return fmt.Errorf("generated connector pack index id, provider, profile, and output are required")
		}
		if kind != GeneratedPackKindBuiltin && kind != GeneratedPackKindFixture {
			return fmt.Errorf("generated connector pack index %q has unsupported kind %q", id, entry.Kind)
		}
		if !catalogPathWithin(profile, generatedProfileRoot) {
			return fmt.Errorf("generated connector pack index %q profile %q is outside %s", id, profile, generatedProfileRoot)
		}
		outputRoot := connectorPackRoot
		if kind == GeneratedPackKindFixture {
			outputRoot = "catalog/testdata/generated"
		}
		if !catalogPathWithin(output, outputRoot) {
			return fmt.Errorf("generated connector pack index %q output %q is outside %s", id, output, outputRoot)
		}
		if _, exists := seenIDs[id]; exists {
			return fmt.Errorf("generated connector pack index contains duplicate pack id %q", id)
		}
		seenIDs[id] = struct{}{}
		if _, exists := seenProfiles[profile]; exists {
			return fmt.Errorf("generated connector pack index contains duplicate profile binding %q", profile)
		}
		seenProfiles[profile] = struct{}{}
		if _, exists := seenOutputs[output]; exists {
			return fmt.Errorf("generated connector pack index contains duplicate output %q", output)
		}
		seenOutputs[output] = struct{}{}
		if _, err := fs.Stat(fsys, profile); err != nil {
			return fmt.Errorf("generated connector pack index %q profile %q is unavailable: %w", id, profile, err)
		}
	}
	profiles, err := fs.ReadDir(fsys, generatedProfileRoot)
	if err != nil {
		return fmt.Errorf("read generated connector profiles: %w", err)
	}
	for _, profileEntry := range profiles {
		if profileEntry.IsDir() || !strings.HasSuffix(profileEntry.Name(), ".yaml") {
			continue
		}
		profile := generatedProfileRoot + "/" + profileEntry.Name()
		if _, exists := seenProfiles[profile]; !exists {
			return fmt.Errorf("generated connector profile %q is not indexed", profile)
		}
	}
	return nil
}

func catalogPathWithin(raw, root string) bool {
	raw = strings.TrimSpace(raw)
	root = strings.Trim(strings.TrimSpace(root), "/")
	return raw != "" && root != "" && path.Clean(raw) == raw && !strings.Contains(raw, "\\") && strings.HasPrefix(raw, root+"/")
}

func (i GeneratedPackIndex) BuiltinByID() map[string]GeneratedPackIndexEntry {
	out := map[string]GeneratedPackIndexEntry{}
	for _, entry := range i.Packs {
		if strings.TrimSpace(entry.Kind) == GeneratedPackKindBuiltin {
			out[strings.TrimSpace(entry.ID)] = entry
		}
	}
	return out
}

func validateGeneratedPackIdentity(fsys fs.FS, pack LoadedPack, expected GeneratedPackIndexEntry) error {
	if pack.Manifest.Generation == nil {
		return fmt.Errorf("generated connector pack %q is indexed as generated but generation evidence is missing", pack.Envelope.ID)
	}
	if normalizeToken(pack.Manifest.Provider) != normalizeToken(expected.Provider) {
		return fmt.Errorf("generated connector pack %q provider %q does not match indexed provider %q", pack.Envelope.ID, pack.Manifest.Provider, expected.Provider)
	}
	if strings.TrimSpace(pack.Manifest.Generation.Profile.Path) != strings.TrimSpace(expected.Profile) {
		return fmt.Errorf("generated connector pack %q profile %q does not match indexed profile %q", pack.Envelope.ID, pack.Manifest.Generation.Profile.Path, expected.Profile)
	}
	profileBody, err := fs.ReadFile(fsys, strings.TrimSpace(expected.Profile))
	if err != nil {
		return fmt.Errorf("generated connector pack %q read indexed profile: %w", pack.Envelope.ID, err)
	}
	if got, want := sha256String(profileBody), "sha256:"+normalizeSHA(pack.Manifest.Generation.Profile.SHA256); got != want {
		return fmt.Errorf("generated connector pack %q profile hash mismatch: got %s want %s", pack.Envelope.ID, got, want)
	}
	return nil
}

func validateSHA256(context, value string) error {
	raw := strings.TrimPrefix(strings.TrimSpace(value), "sha256:")
	if len(raw) != sha256.Size*2 {
		return fmt.Errorf("%s sha256 has invalid length", context)
	}
	if _, err := hex.DecodeString(raw); err != nil {
		return fmt.Errorf("%s sha256 is invalid: %w", context, err)
	}
	return nil
}

func sha256String(body []byte) string {
	sum := sha256.Sum256(body)
	return "sha256:" + hex.EncodeToString(sum[:])
}

func responseSuccessEqual(a, b runtimecontracts.HTTPResponseSuccess) bool {
	if httpresponsesuccess.NormalizeKind(a.Kind) != httpresponsesuccess.NormalizeKind(b.Kind) || strings.TrimSpace(a.Path) != strings.TrimSpace(b.Path) {
		return false
	}
	return fmt.Sprint(a.Equals) == fmt.Sprint(b.Equals)
}
