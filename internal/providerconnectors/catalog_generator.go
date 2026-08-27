package providerconnectors

import (
	"bytes"
	"compress/gzip"
	"encoding/json"
	"fmt"
	"io"
	"io/fs"
	"net/url"
	"os"
	"path"
	"regexp"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/platform"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"gopkg.in/yaml.v3"
)

const (
	profileAuthManaged = "managed_credential"
	profileAuthStatic  = "static_credentials"
)

var (
	openAPIPathParameterPattern = regexp.MustCompile(`\{([^{}]+)\}`)
	catalogIdentityTokenPattern = regexp.MustCompile(`^[a-z][a-z0-9_]*$`)
)

type GeneratorProfile struct {
	SchemaVersion string               `yaml:"schema_version"`
	Provider      string               `yaml:"provider"`
	Source        GeneratorSource      `yaml:"source"`
	Pack          GeneratorPack        `yaml:"pack"`
	Auth          GeneratorAuth        `yaml:"auth"`
	StaticHeaders map[string]string    `yaml:"static_headers"`
	Operations    []GeneratorOperation `yaml:"operations"`
}

type GeneratorSource struct {
	Path           string `yaml:"path"`
	Compression    string `yaml:"compression"`
	SHA256         string `yaml:"sha256"`
	OpenAPIVersion string `yaml:"openapi_version"`
	Server         string `yaml:"server"`
}

type GeneratorPack struct {
	ID              string   `yaml:"id"`
	Version         string   `yaml:"version"`
	PlatformVersion string   `yaml:"platform_version"`
	Provenance      string   `yaml:"provenance"`
	Tests           []string `yaml:"tests"`
}

type GeneratorAuth struct {
	Mode              string                                 `yaml:"mode"`
	Credentials       []string                               `yaml:"credentials"`
	ManagedCredential *runtimecontracts.ManagedCredentialRef `yaml:"managed_credential"`
}

type GeneratorOperation struct {
	OperationID     string                               `yaml:"operation_id"`
	Path            string                               `yaml:"path"`
	Method          string                               `yaml:"method"`
	ToolID          string                               `yaml:"tool_id"`
	Description     string                               `yaml:"description"`
	EffectClass     string                               `yaml:"effect_class"`
	PathParameters  []GeneratorField                     `yaml:"path_parameters"`
	RequestBody     GeneratorRequestBody                 `yaml:"request_body"`
	InjectedInputs  []GeneratorInjectedInput             `yaml:"injected_inputs"`
	Permissions     []GenerationPermission               `yaml:"permissions"`
	Output          GeneratorOutput                      `yaml:"output"`
	ResponseSuccess runtimecontracts.HTTPResponseSuccess `yaml:"response_success"`
	Fixture         GeneratorFixtureRef                  `yaml:"fixture"`
	ReviewStatus    string                               `yaml:"review_status"`
}

type GeneratorField struct {
	Source    string `yaml:"source"`
	Input     string `yaml:"input"`
	Type      string `yaml:"type"`
	ItemsType string `yaml:"items_type"`
	Required  bool   `yaml:"required"`
}

type GeneratorRequestBody struct {
	Required bool                 `yaml:"required"`
	Variant  GeneratorBodyVariant `yaml:"variant"`
	Fields   []GeneratorField     `yaml:"fields"`
}

type GeneratorBodyVariant struct {
	Kind   string   `yaml:"kind"`
	Fields []string `yaml:"fields"`
}

type GeneratorInjectedInput struct {
	Input    string `yaml:"input"`
	Type     string `yaml:"type"`
	Required bool   `yaml:"required"`
	Purpose  string `yaml:"purpose"`
}

type GeneratorOutput struct {
	Type      string `yaml:"type"`
	ItemsType string `yaml:"items_type,omitempty"`
}

type GeneratorFixtureRef struct {
	ID     string `yaml:"id"`
	Status string `yaml:"status"`
}

type GeneratedCatalogArtifact struct {
	IndexEntry    GeneratedPackIndexEntry
	Profile       GeneratorProfile
	ProfileHash   string
	SourceHash    string
	Manifest      ConnectorManifest
	ConnectorBody []byte
	PackEnvelope  packs.Envelope
	PackBody      []byte
}

func ParseGeneratorProfile(body []byte) (GeneratorProfile, error) {
	var profile GeneratorProfile
	if err := decodeYAMLStrict(body, &profile); err != nil {
		return GeneratorProfile{}, err
	}
	if err := profile.Validate(); err != nil {
		return GeneratorProfile{}, err
	}
	return profile, nil
}

func (p GeneratorProfile) Validate() error {
	if strings.TrimSpace(p.SchemaVersion) != CatalogSchemaVersion {
		return fmt.Errorf("generator profile schema_version must be %q", CatalogSchemaVersion)
	}
	provider := normalizeToken(p.Provider)
	if provider == "" || strings.TrimSpace(p.Provider) != provider || !catalogIdentityTokenPattern.MatchString(provider) {
		return fmt.Errorf("generator profile provider must be canonical lowercase snake_case")
	}
	if strings.TrimSpace(p.Source.Path) == "" || strings.TrimSpace(p.Source.OpenAPIVersion) == "" || strings.TrimSpace(p.Source.Server) == "" {
		return fmt.Errorf("generator profile source path, openapi_version, and server are required")
	}
	if !catalogPathWithin(p.Source.Path, "catalog/sources") {
		return fmt.Errorf("generator profile source path %q is outside catalog/sources", p.Source.Path)
	}
	if p.Source.Compression != "none" && p.Source.Compression != "gzip" {
		return fmt.Errorf("generator profile source compression %q is unsupported", p.Source.Compression)
	}
	if err := validateSHA256("generator profile source", p.Source.SHA256); err != nil {
		return err
	}
	if _, err := url.ParseRequestURI(strings.TrimSpace(p.Source.Server)); err != nil {
		return fmt.Errorf("generator profile source server is invalid: %w", err)
	}
	if strings.TrimSpace(p.Pack.ID) == "" || strings.TrimSpace(p.Pack.Version) == "" || strings.TrimSpace(p.Pack.PlatformVersion) == "" || strings.TrimSpace(p.Pack.Provenance) == "" || len(p.Pack.Tests) == 0 {
		return fmt.Errorf("generator profile pack id, version, platform_version, provenance, and tests are required")
	}
	for _, test := range p.Pack.Tests {
		if strings.TrimSpace(test) == "" {
			return fmt.Errorf("generator profile pack tests must not contain empty entries")
		}
	}
	switch strings.TrimSpace(p.Auth.Mode) {
	case profileAuthManaged:
		if p.Auth.ManagedCredential == nil || strings.TrimSpace(p.Auth.ManagedCredential.Key) == "" {
			return fmt.Errorf("generator profile managed_credential auth requires managed_credential.key")
		}
		if len(p.Auth.Credentials) > 0 {
			return fmt.Errorf("generator profile managed_credential auth forbids static credentials")
		}
	case profileAuthStatic:
		if p.Auth.ManagedCredential != nil || len(p.Auth.Credentials) == 0 {
			return fmt.Errorf("generator profile static_credentials auth requires credentials and forbids managed_credential")
		}
		for _, credential := range p.Auth.Credentials {
			if strings.TrimSpace(credential) == "" {
				return fmt.Errorf("generator profile credentials must not contain empty entries")
			}
		}
	default:
		return fmt.Errorf("generator profile auth mode %q is unsupported", p.Auth.Mode)
	}
	if p.StaticHeaders == nil {
		return fmt.Errorf("generator profile static_headers must be declared, including declared-empty")
	}
	for key, value := range p.StaticHeaders {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return fmt.Errorf("generator profile static_headers names and values must be non-empty")
		}
	}
	if len(p.Operations) == 0 {
		return fmt.Errorf("generator profile operations are required")
	}
	seenOperations := map[string]struct{}{}
	seenTools := map[string]struct{}{}
	seenFixtures := map[string]struct{}{}
	for _, operation := range p.Operations {
		if err := operation.Validate(provider); err != nil {
			return err
		}
		operationID := strings.TrimSpace(operation.OperationID)
		toolID := strings.TrimSpace(operation.ToolID)
		fixtureID := strings.TrimSpace(operation.Fixture.ID)
		if _, exists := seenOperations[operationID]; exists {
			return fmt.Errorf("generator profile contains duplicate operation_id %q", operationID)
		}
		seenOperations[operationID] = struct{}{}
		if _, exists := seenTools[toolID]; exists {
			return fmt.Errorf("generator profile contains duplicate tool_id %q", toolID)
		}
		seenTools[toolID] = struct{}{}
		if _, exists := seenFixtures[fixtureID]; exists {
			return fmt.Errorf("generator profile contains duplicate fixture id %q", fixtureID)
		}
		seenFixtures[fixtureID] = struct{}{}
	}
	return nil
}

func (o GeneratorOperation) Validate(provider string) error {
	context := fmt.Sprintf("generator operation %q", strings.TrimSpace(o.OperationID))
	if strings.TrimSpace(o.OperationID) == "" || strings.TrimSpace(o.Path) == "" || strings.TrimSpace(o.Method) == "" || strings.TrimSpace(o.ToolID) == "" || strings.TrimSpace(o.Description) == "" {
		return fmt.Errorf("%s operation_id, path, method, tool_id, and description are required", context)
	}
	toolID := strings.TrimSpace(o.ToolID)
	toolProvider, action, ok := splitToolID(toolID)
	if !ok || toolProvider != provider || action == "" || toolID != provider+"."+action || !catalogIdentityTokenPattern.MatchString(action) {
		return fmt.Errorf("%s tool_id %q must use stable %s.<action> form", context, o.ToolID, provider)
	}
	if strings.TrimSpace(o.EffectClass) != string(runtimecontracts.ActivityEffectClassNonIdempotentWrite) {
		return fmt.Errorf("%s effect_class must be non_idempotent_write", context)
	}
	if o.PathParameters == nil || o.InjectedInputs == nil {
		return fmt.Errorf("%s path_parameters and injected_inputs must be declared, including declared-empty", context)
	}
	if !o.RequestBody.Required || strings.TrimSpace(o.RequestBody.Variant.Kind) != "object" || len(o.RequestBody.Variant.Fields) == 0 || len(o.RequestBody.Fields) == 0 {
		return fmt.Errorf("%s requires an explicit required object request_body variant and fields", context)
	}
	seenInputs := map[string]struct{}{}
	for _, field := range append(append([]GeneratorField(nil), o.PathParameters...), o.RequestBody.Fields...) {
		if err := validateGeneratorField(context, field); err != nil {
			return err
		}
		if _, exists := seenInputs[field.Input]; exists {
			return fmt.Errorf("%s contains duplicate input %q", context, field.Input)
		}
		seenInputs[field.Input] = struct{}{}
	}
	for _, input := range o.InjectedInputs {
		if strings.TrimSpace(input.Input) == "" || strings.TrimSpace(input.Type) == "" || strings.TrimSpace(input.Purpose) == "" || !input.Required {
			return fmt.Errorf("%s injected inputs require input, type, purpose, and required=true", context)
		}
		if !catalogIdentityTokenPattern.MatchString(strings.TrimSpace(input.Input)) || strings.TrimSpace(input.Type) != "string" {
			return fmt.Errorf("%s injected input %q must use a canonical lowercase snake_case name and string type in the first slice", context, input.Input)
		}
		if _, exists := seenInputs[input.Input]; exists {
			return fmt.Errorf("%s contains duplicate input %q", context, input.Input)
		}
		seenInputs[input.Input] = struct{}{}
	}
	if len(o.Permissions) == 0 {
		return fmt.Errorf("%s permissions are required", context)
	}
	seenPermissions := map[string]struct{}{}
	for _, permission := range o.Permissions {
		permissionID := strings.TrimSpace(permission.ID)
		if permissionID == "" || strings.TrimSpace(permission.Note) == "" {
			return fmt.Errorf("%s permission id and note are required", context)
		}
		if _, exists := seenPermissions[permissionID]; exists {
			return fmt.Errorf("%s contains duplicate permission id %q", context, permissionID)
		}
		seenPermissions[permissionID] = struct{}{}
	}
	outputKind, err := generatedToolSchemaKind(o.Output.Type)
	if err != nil {
		return fmt.Errorf("%s output: %w", context, err)
	}
	if outputKind == runtimecontracts.ToolSchemaArray {
		if _, err := generatedToolSchemaKind(o.Output.ItemsType); err != nil {
			return fmt.Errorf("%s output array items: %w", context, err)
		}
	} else if strings.TrimSpace(o.Output.ItemsType) != "" {
		return fmt.Errorf("%s non-array output forbids items_type", context)
	}
	if _, err := runtimecontracts.AdmitToolResponseSuccessPolicy(o.ResponseSuccess); err != nil {
		return fmt.Errorf("%s: %w", context, err)
	}
	if strings.TrimSpace(o.Fixture.ID) == "" || strings.TrimSpace(o.Fixture.Status) != GenerationFixturePassing {
		return fmt.Errorf("%s fixture id and status %q are required", context, GenerationFixturePassing)
	}
	if strings.TrimSpace(o.ReviewStatus) != GenerationReviewApproved {
		return fmt.Errorf("%s review_status must be %q", context, GenerationReviewApproved)
	}
	return nil
}

func validateGeneratorField(context string, field GeneratorField) error {
	if strings.TrimSpace(field.Source) == "" || strings.TrimSpace(field.Input) == "" || strings.TrimSpace(field.Type) == "" || !field.Required {
		return fmt.Errorf("%s fields require source, input, type, and required=true", context)
	}
	if !catalogIdentityTokenPattern.MatchString(strings.TrimSpace(field.Input)) {
		return fmt.Errorf("%s input %q must be canonical lowercase snake_case", context, field.Input)
	}
	switch strings.TrimSpace(field.Type) {
	case "string", "integer", "array":
	default:
		return fmt.Errorf("%s input %q type %q is unsupported in the first slice", context, field.Input, field.Type)
	}
	if field.Type == "array" && strings.TrimSpace(field.ItemsType) == "" {
		return fmt.Errorf("%s array input %q requires items_type", context, field.Input)
	}
	if field.Type == "array" && strings.TrimSpace(field.ItemsType) != "string" {
		return fmt.Errorf("%s array input %q items_type must be string in the first slice", context, field.Input)
	}
	if field.Type != "array" && strings.TrimSpace(field.ItemsType) != "" {
		return fmt.Errorf("%s non-array input %q forbids items_type", context, field.Input)
	}
	return nil
}

func GenerateCatalog(fsys fs.FS) ([]GeneratedCatalogArtifact, error) {
	index, err := loadGeneratedPackIndex(fsys)
	if err != nil {
		return nil, err
	}
	artifacts := make([]GeneratedCatalogArtifact, 0, len(index.Packs))
	for _, entry := range index.Packs {
		artifact, err := generateCatalogPack(fsys, entry)
		if err != nil {
			return nil, fmt.Errorf("generate connector pack %q: %w", entry.ID, err)
		}
		artifacts = append(artifacts, artifact)
	}
	return artifacts, nil
}

func WriteGeneratedCatalog(catalogRoot, packRoot string) error {
	catalogRoot = strings.TrimSpace(catalogRoot)
	packRoot = strings.TrimSpace(packRoot)
	if catalogRoot == "" || packRoot == "" {
		return fmt.Errorf("connector catalog root is required")
	}
	artifacts, err := GenerateCatalog(os.DirFS(catalogRoot))
	if err != nil {
		return err
	}
	for _, artifact := range artifacts {
		output := strings.Trim(strings.TrimSpace(artifact.IndexEntry.Output), "/")
		root := catalogRoot
		if artifact.IndexEntry.Kind == GeneratedPackKindBuiltin {
			root = packRoot
		}
		if err := os.MkdirAll(path.Join(root, output), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path.Join(root, output, packs.ConnectorManifestFileName), artifact.ConnectorBody, 0o644); err != nil {
			return err
		}
		if err := os.WriteFile(path.Join(root, output, packs.EnvelopeFileName), artifact.PackBody, 0o644); err != nil {
			return err
		}
	}
	return validateGeneratedInventoryMembership(os.DirFS(packRoot), artifacts)
}

func CheckGeneratedCatalog(catalogFS, packFS fs.FS) ([]GeneratedCatalogArtifact, error) {
	if err := validateCheckedInGeneratedIdentity(catalogFS, packFS); err != nil {
		return nil, err
	}
	artifacts, err := GenerateCatalog(catalogFS)
	if err != nil {
		return nil, err
	}
	for _, artifact := range artifacts {
		output := strings.Trim(strings.TrimSpace(artifact.IndexEntry.Output), "/")
		outputFS := catalogFS
		if artifact.IndexEntry.Kind == GeneratedPackKindBuiltin {
			outputFS = packFS
		}
		if err := compareGeneratedFile(outputFS, path.Join(output, packs.ConnectorManifestFileName), artifact.ConnectorBody); err != nil {
			return nil, err
		}
		if err := compareGeneratedFile(outputFS, path.Join(output, packs.EnvelopeFileName), artifact.PackBody); err != nil {
			return nil, err
		}
	}
	if err := ValidateCatalogConformance(catalogFS, artifacts); err != nil {
		return nil, err
	}
	if err := validateGeneratedInventoryMembership(packFS, artifacts); err != nil {
		return nil, err
	}
	return artifacts, nil
}

func validateCheckedInGeneratedIdentity(catalogFS, packFS fs.FS) error {
	index, err := loadGeneratedPackIndex(catalogFS)
	if err != nil {
		return err
	}
	manifest, err := packartifact.LoadInventoryManifest(packFS, packartifact.InventoryManifestFileName)
	if err != nil {
		return fmt.Errorf("load explicit embedded inventory for generated connectors: %w", err)
	}
	runningPlatformVersion, err := platform.PlatformVersion()
	if err != nil {
		return fmt.Errorf("resolve platform version for generated connectors: %w", err)
	}
	expected := index.BuiltinByID()
	seen := make(map[string]struct{}, len(expected))
	for _, entry := range manifest.Packs {
		if entry.Type != packartifact.TypeConnector {
			continue
		}
		pack, err := LoadPackFS(packFS, entry.Path, runningPlatformVersion)
		if err != nil {
			return fmt.Errorf("load embedded connector pack %q for generated identity: %w", entry.ID, err)
		}
		indexed, ok := expected[entry.ID]
		if !ok {
			if pack.Manifest.Generation != nil {
				return fmt.Errorf("builtin connector pack %q carries generation evidence but is not in the generated pack index", entry.ID)
			}
			continue
		}
		if strings.Trim(strings.TrimSpace(indexed.Output), "/") != entry.Path {
			return fmt.Errorf("generated connector pack %q output %q contradicts explicit inventory path %q", entry.ID, indexed.Output, entry.Path)
		}
		if err := validateGeneratedPackIdentity(catalogFS, pack, indexed); err != nil {
			return err
		}
		seen[entry.ID] = struct{}{}
	}
	for packID := range expected {
		if _, ok := seen[packID]; !ok {
			return fmt.Errorf("generated connector pack index references unknown builtin pack id %q", packID)
		}
	}
	return nil
}

func validateGeneratedInventoryMembership(packFS fs.FS, artifacts []GeneratedCatalogArtifact) error {
	manifest, err := packartifact.LoadInventoryManifest(packFS, packartifact.InventoryManifestFileName)
	if err != nil {
		return fmt.Errorf("load explicit embedded inventory for generated connectors: %w", err)
	}
	declared := make(map[string]packartifact.InventoryManifestPack, len(manifest.Packs))
	for _, entry := range manifest.Packs {
		declared[entry.ID] = entry
	}
	for _, artifact := range artifacts {
		if artifact.IndexEntry.Kind != GeneratedPackKindBuiltin {
			continue
		}
		entry, ok := declared[artifact.PackEnvelope.ID]
		if !ok {
			return fmt.Errorf("generated connector pack %q is absent from %s", artifact.PackEnvelope.ID, packartifact.InventoryManifestFileName)
		}
		output := strings.Trim(strings.TrimSpace(artifact.IndexEntry.Output), "/")
		if entry.Type != packartifact.TypeConnector || entry.Path != output {
			return fmt.Errorf("generated connector pack %q output %q contradicts explicit inventory type=%q path=%q", artifact.PackEnvelope.ID, output, entry.Type, entry.Path)
		}
	}
	return nil
}

func compareGeneratedFile(fsys fs.FS, name string, want []byte) error {
	got, err := fs.ReadFile(fsys, name)
	if err != nil {
		return fmt.Errorf("read generated output %q: %w", name, err)
	}
	if !bytes.Equal(got, want) {
		return fmt.Errorf("generated connector catalog differs from %s; regenerate it", name)
	}
	return nil
}

func generateCatalogPack(fsys fs.FS, entry GeneratedPackIndexEntry) (GeneratedCatalogArtifact, error) {
	profileBody, err := fs.ReadFile(fsys, strings.TrimSpace(entry.Profile))
	if err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	profile, err := ParseGeneratorProfile(profileBody)
	if err != nil {
		return GeneratedCatalogArtifact{}, fmt.Errorf("parse profile %q: %w", entry.Profile, err)
	}
	if strings.TrimSpace(profile.Pack.ID) != strings.TrimSpace(entry.ID) || normalizeToken(profile.Provider) != normalizeToken(entry.Provider) {
		return GeneratedCatalogArtifact{}, fmt.Errorf("profile pack/provider identity does not match generated index")
	}
	sourceBody, err := readGeneratorSource(fsys, profile.Source)
	if err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	document, err := parseOpenAPIDocument(sourceBody, profile.Source)
	if err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	profileHash := sha256String(profileBody)
	sourceHash := sha256String(sourceBody)
	tools := map[string]runtimecontracts.ToolSchemaEntry{}
	evidence := make([]GenerationOperationEvidence, 0, len(profile.Operations))
	for _, operation := range profile.Operations {
		tool, err := generateOperationTool(document, profile, operation)
		if err != nil {
			return GeneratedCatalogArtifact{}, err
		}
		tools[strings.TrimSpace(operation.ToolID)] = tool
		evidence = append(evidence, GenerationOperationEvidence{
			OperationID:     strings.TrimSpace(operation.OperationID),
			ToolID:          strings.TrimSpace(operation.ToolID),
			Permissions:     append([]GenerationPermission(nil), operation.Permissions...),
			ResponseSuccess: operation.ResponseSuccess,
			FixtureID:       strings.TrimSpace(operation.Fixture.ID),
			FixtureStatus:   strings.TrimSpace(operation.Fixture.Status),
			ReviewStatus:    strings.TrimSpace(operation.ReviewStatus),
		})
	}
	sort.Slice(evidence, func(i, j int) bool { return evidence[i].ToolID < evidence[j].ToolID })
	manifest := ConnectorManifest{
		Provider: normalizeToken(profile.Provider),
		Generation: &GenerationEvidence{
			SchemaVersion:    CatalogSchemaVersion,
			GeneratorVersion: CatalogGeneratorVersion,
			Source: GenerationSourceEvidence{
				Path:           strings.TrimSpace(profile.Source.Path),
				OpenAPIVersion: strings.TrimSpace(profile.Source.OpenAPIVersion),
				SHA256:         sourceHash,
			},
			Profile: GenerationProfileEvidence{
				Path:          strings.TrimSpace(entry.Profile),
				SchemaVersion: strings.TrimSpace(profile.SchemaVersion),
				SHA256:        profileHash,
			},
			Operations: evidence,
		},
		Tools: tools,
	}
	if err := manifest.Validate(); err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	if err := manifest.Generation.Validate(manifest.Provider, manifest.Tools); err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	connectorBody, err := yaml.Marshal(manifest)
	if err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	envelope := packs.Envelope{
		ID:              strings.TrimSpace(profile.Pack.ID),
		Version:         strings.TrimSpace(profile.Pack.Version),
		PlatformVersion: strings.TrimSpace(profile.Pack.PlatformVersion),
		Type:            packs.TypeConnector,
		Provenance:      packs.Provenance{Source: strings.TrimSpace(profile.Pack.Provenance)},
		Capabilities:    DerivedCapabilities(manifest),
		Requires:        DerivedRequires(manifest),
		Tests:           append([]string(nil), profile.Pack.Tests...),
	}
	envelope, packBody, err := packs.StampEnvelope(envelope, connectorBody)
	if err != nil {
		return GeneratedCatalogArtifact{}, err
	}
	return GeneratedCatalogArtifact{
		IndexEntry:    entry,
		Profile:       profile,
		ProfileHash:   profileHash,
		SourceHash:    sourceHash,
		Manifest:      manifest,
		ConnectorBody: connectorBody,
		PackEnvelope:  envelope,
		PackBody:      packBody,
	}, nil
}

func readGeneratorSource(fsys fs.FS, source GeneratorSource) ([]byte, error) {
	body, err := fs.ReadFile(fsys, strings.TrimSpace(source.Path))
	if err != nil {
		return nil, fmt.Errorf("read OpenAPI source %q: %w", source.Path, err)
	}
	switch strings.TrimSpace(source.Compression) {
	case "none":
	case "gzip":
		reader, err := gzip.NewReader(bytes.NewReader(body))
		if err != nil {
			return nil, fmt.Errorf("open gzip OpenAPI source %q: %w", source.Path, err)
		}
		decompressed, err := io.ReadAll(reader)
		closeErr := reader.Close()
		if err != nil {
			return nil, fmt.Errorf("read gzip OpenAPI source %q: %w", source.Path, err)
		}
		if closeErr != nil {
			return nil, fmt.Errorf("close gzip OpenAPI source %q: %w", source.Path, closeErr)
		}
		body = decompressed
	default:
		return nil, fmt.Errorf("unsupported OpenAPI source compression %q", source.Compression)
	}
	if got, want := sha256String(body), "sha256:"+strings.TrimPrefix(strings.TrimSpace(source.SHA256), "sha256:"); got != want {
		return nil, fmt.Errorf("OpenAPI source %q hash mismatch: got %s want %s", source.Path, got, want)
	}
	return body, nil
}

type openAPIDocument struct {
	Version    string
	Server     string
	Paths      map[string]json.RawMessage
	Parameters map[string]json.RawMessage
	Schemas    map[string]json.RawMessage
}

type openAPIDocumentWire struct {
	OpenAPI    string                     `json:"openapi"`
	Servers    []openAPIServer            `json:"servers"`
	Paths      map[string]json.RawMessage `json:"paths"`
	Components openAPIComponents          `json:"components"`
}

type openAPIServer struct {
	URL         string                     `json:"url"`
	Description string                     `json:"description,omitempty"`
	Variables   map[string]json.RawMessage `json:"variables,omitempty"`
}

type openAPIComponents struct {
	Parameters map[string]json.RawMessage `json:"parameters"`
	Schemas    map[string]json.RawMessage `json:"schemas"`
}

type openAPIOperation struct {
	Summary      string                      `json:"summary"`
	Description  string                      `json:"description"`
	Tags         []string                    `json:"tags"`
	OperationID  string                      `json:"operationId"`
	ExternalDocs json.RawMessage             `json:"externalDocs"`
	Parameters   []openAPIParameterReference `json:"parameters"`
	RequestBody  openAPIRequestBody          `json:"requestBody"`
	Responses    map[string]json.RawMessage  `json:"responses"`
	XGitHub      json.RawMessage             `json:"x-github"`
}

type openAPIParameterReference struct {
	Ref string `json:"$ref"`
}

type openAPIParameter struct {
	Name        string          `json:"name"`
	Description string          `json:"description"`
	In          string          `json:"in"`
	Required    bool            `json:"required"`
	Schema      json.RawMessage `json:"schema"`
}

type openAPIRequestBody struct {
	Required bool                        `json:"required"`
	Content  map[string]openAPIMediaType `json:"content"`
}

type openAPIMediaType struct {
	Schema   json.RawMessage `json:"schema"`
	Examples json.RawMessage `json:"examples,omitempty"`
}

type openAPIResponse struct {
	Ref     string                      `json:"$ref,omitempty"`
	Content map[string]openAPIMediaType `json:"content,omitempty"`
}

type openAPISchema struct {
	Ref                  string                     `json:"$ref,omitempty"`
	Type                 string                     `json:"type,omitempty"`
	Description          string                     `json:"description,omitempty"`
	Properties           map[string]json.RawMessage `json:"properties,omitempty"`
	Required             []string                   `json:"required,omitempty"`
	Items                json.RawMessage            `json:"items,omitempty"`
	OneOf                []json.RawMessage          `json:"oneOf,omitempty"`
	Nullable             bool                       `json:"nullable,omitempty"`
	MinItems             *int                       `json:"minItems,omitempty"`
	AdditionalProperties json.RawMessage            `json:"additionalProperties,omitempty"`
	Example              json.RawMessage            `json:"example,omitempty"`
}

func parseOpenAPIDocument(body []byte, expected GeneratorSource) (openAPIDocument, error) {
	var wire openAPIDocumentWire
	if err := json.Unmarshal(body, &wire); err != nil {
		return openAPIDocument{}, fmt.Errorf("parse OpenAPI source: %w", err)
	}
	if strings.TrimSpace(wire.OpenAPI) != strings.TrimSpace(expected.OpenAPIVersion) {
		return openAPIDocument{}, fmt.Errorf("OpenAPI version %q does not match profile %q", wire.OpenAPI, expected.OpenAPIVersion)
	}
	server := strings.TrimSpace(expected.Server)
	serverFound := false
	for _, candidate := range wire.Servers {
		if strings.TrimRight(strings.TrimSpace(candidate.URL), "/") == strings.TrimRight(server, "/") {
			serverFound = true
			break
		}
	}
	if !serverFound {
		return openAPIDocument{}, fmt.Errorf("OpenAPI server %q is not present", server)
	}
	if len(wire.Paths) == 0 {
		return openAPIDocument{}, fmt.Errorf("OpenAPI paths are required")
	}
	return openAPIDocument{
		Version: wire.OpenAPI, Server: server, Paths: wire.Paths,
		Parameters: wire.Components.Parameters, Schemas: wire.Components.Schemas,
	}, nil
}

func generateOperationTool(document openAPIDocument, profile GeneratorProfile, selected GeneratorOperation) (runtimecontracts.ToolSchemaEntry, error) {
	pathItem, exists := document.Paths[strings.TrimSpace(selected.Path)]
	if !exists {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q path %q is missing", selected.OperationID, selected.Path)
	}
	var methods map[string]json.RawMessage
	if err := json.Unmarshal(pathItem, &methods); err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("parse path %q: %w", selected.Path, err)
	}
	method := strings.ToLower(strings.TrimSpace(selected.Method))
	operationBody, exists := methods[method]
	if !exists {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q method %q is missing", selected.OperationID, method)
	}
	var operation openAPIOperation
	if err := decodeJSONStrict(operationBody, &operation); err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("parse selected operation %q: %w", selected.OperationID, err)
	}
	if strings.TrimSpace(operation.OperationID) != strings.TrimSpace(selected.OperationID) {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("selected operation id %q does not match OpenAPI %q", selected.OperationID, operation.OperationID)
	}
	inputProperties := map[string]runtimecontracts.ToolInputSchema{}
	required := make([]string, 0)
	for _, injected := range selected.InjectedInputs {
		schema, err := toolInputSchema(injected.Type, "")
		if err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q injected input %q: %w", selected.OperationID, injected.Input, err)
		}
		inputProperties[injected.Input] = schema
		if injected.Required {
			required = append(required, injected.Input)
		}
	}
	resolvedURL := strings.TrimRight(document.Server, "/") + selected.Path
	for _, field := range selected.PathParameters {
		parameter, err := resolveSelectedParameter(document, operation, field.Source)
		if err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q: %w", selected.OperationID, err)
		}
		if strings.TrimSpace(parameter.In) != "path" || parameter.Required != field.Required {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q parameter %q path/required semantics do not match profile", selected.OperationID, field.Source)
		}
		var schema openAPISchema
		if err := decodeJSONStrict(parameter.Schema, &schema); err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q parameter %q schema: %w", selected.OperationID, field.Source, err)
		}
		if !schemaSupportsType(schema, field.Type, field.ItemsType) {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q parameter %q does not support profile type %q", selected.OperationID, field.Source, field.Type)
		}
		admittedSchema, err := toolInputSchema(field.Type, field.ItemsType)
		if err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q parameter %q: %w", selected.OperationID, field.Source, err)
		}
		inputProperties[field.Input] = admittedSchema
		if field.Required {
			required = append(required, field.Input)
		}
		placeholder := "{" + strings.TrimSpace(parameter.Name) + "}"
		if !strings.Contains(resolvedURL, placeholder) {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q path lacks parameter placeholder %q", selected.OperationID, placeholder)
		}
		resolvedURL = strings.ReplaceAll(resolvedURL, placeholder, "{{input."+field.Input+"}}")
	}
	withoutInputTemplates := regexp.MustCompile(`\{\{[^{}]+\}\}`).ReplaceAllString(resolvedURL, "")
	if openAPIPathParameterPattern.MatchString(withoutInputTemplates) {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q has unbound path parameters in %q", selected.OperationID, resolvedURL)
	}
	bodySchema, err := selectRequestBodySchema(operation.RequestBody, selected.RequestBody)
	if err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q: %w", selected.OperationID, err)
	}
	body := map[string]any{}
	for _, field := range selected.RequestBody.Fields {
		raw, exists := bodySchema.Properties[field.Source]
		if !exists {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q body field %q is missing", selected.OperationID, field.Source)
		}
		var schema openAPISchema
		if err := decodeJSONStrict(raw, &schema); err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q body field %q: %w", selected.OperationID, field.Source, err)
		}
		if !schemaSupportsType(schema, field.Type, field.ItemsType) {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q body field %q does not support profile type %q", selected.OperationID, field.Source, field.Type)
		}
		admittedSchema, err := toolInputSchema(field.Type, field.ItemsType)
		if err != nil {
			return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q body field %q: %w", selected.OperationID, field.Source, err)
		}
		inputProperties[field.Input] = admittedSchema
		if field.Required {
			required = append(required, field.Input)
		}
		body[field.Source] = "{{input." + field.Input + "}}"
	}
	inputSchema, err := runtimecontracts.NewToolInputSchema(
		runtimecontracts.ToolSchemaObject,
		runtimecontracts.ToolSchemaProperties(inputProperties),
		runtimecontracts.ToolSchemaRequired(required...),
	)
	if err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q input schema: %w", selected.OperationID, err)
	}
	if !has2xxResponse(operation.Responses) {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q has no declared 2xx response", selected.OperationID)
	}
	if err := validateSelectedOutputSchema(document, operation.Responses, selected.Output); err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q output schema: %w", selected.OperationID, err)
	}
	outputSchema, err := toolInputSchema(selected.Output.Type, selected.Output.ItemsType)
	if err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q output schema: %w", selected.OperationID, err)
	}
	options := []runtimecontracts.ToolSchemaEntryOption{
		runtimecontracts.WithToolCategory(Category),
		runtimecontracts.WithToolDescription(selected.Description),
		runtimecontracts.WithToolHandler(runtimecontracts.ToolHandlerHTTP),
		runtimecontracts.WithToolEffect(runtimecontracts.NormalizeActivityEffectClass(selected.EffectClass)),
		runtimecontracts.WithToolSchemas(inputSchema, outputSchema),
		runtimecontracts.WithToolResponseSuccess(selected.ResponseSuccess),
		runtimecontracts.WithToolHTTP(runtimecontracts.HTTPToolSpec{
			Method:  strings.ToUpper(method),
			URL:     resolvedURL,
			Headers: cloneStringMap(profile.StaticHeaders),
			Body:    body,
		}),
	}
	switch strings.TrimSpace(profile.Auth.Mode) {
	case profileAuthManaged:
		ref := *profile.Auth.ManagedCredential
		ref.Scopes = append([]string(nil), profile.Auth.ManagedCredential.Scopes...)
		options = append(options, runtimecontracts.WithToolManagedCredential(ref))
	case profileAuthStatic:
		options = append(options, runtimecontracts.WithToolCredentials(profile.Auth.Credentials...))
	}
	tool, err := runtimecontracts.NewToolSchemaEntry(options...)
	if err != nil {
		return runtimecontracts.ToolSchemaEntry{}, fmt.Errorf("operation %q admitted tool: %w", selected.OperationID, err)
	}
	return tool, nil
}

func resolveSelectedParameter(document openAPIDocument, operation openAPIOperation, componentKey string) (openAPIParameter, error) {
	wantRef := "#/components/parameters/" + strings.TrimSpace(componentKey)
	found := false
	for _, reference := range operation.Parameters {
		if strings.TrimSpace(reference.Ref) == wantRef {
			found = true
			break
		}
	}
	if !found {
		return openAPIParameter{}, fmt.Errorf("selected local parameter ref %q is missing", wantRef)
	}
	raw, exists := document.Parameters[strings.TrimSpace(componentKey)]
	if !exists {
		return openAPIParameter{}, fmt.Errorf("selected local parameter component %q is missing", componentKey)
	}
	var parameter openAPIParameter
	if err := decodeJSONStrict(raw, &parameter); err != nil {
		return openAPIParameter{}, err
	}
	return parameter, nil
}

func selectRequestBodySchema(request openAPIRequestBody, selected GeneratorRequestBody) (openAPISchema, error) {
	if request.Required != selected.Required {
		// A reviewed profile may deliberately narrow an optional provider body to required.
		if request.Required || !selected.Required {
			return openAPISchema{}, fmt.Errorf("request body requiredness does not match profile")
		}
	}
	media, exists := request.Content["application/json"]
	if !exists {
		return openAPISchema{}, fmt.Errorf("application/json request body is required")
	}
	var root openAPISchema
	if err := decodeJSONStrict(media.Schema, &root); err != nil {
		return openAPISchema{}, err
	}
	candidates := []openAPISchema{root}
	if len(root.OneOf) > 0 {
		candidates = nil
		for _, raw := range root.OneOf {
			var candidate openAPISchema
			if err := decodeJSONStrict(raw, &candidate); err != nil {
				return openAPISchema{}, err
			}
			if schemaContainsFields(candidate, selected.Variant.Fields) {
				candidates = append(candidates, candidate)
			}
		}
	}
	matched := make([]openAPISchema, 0, len(candidates))
	for _, candidate := range candidates {
		if strings.TrimSpace(candidate.Type) == strings.TrimSpace(selected.Variant.Kind) && schemaContainsFields(candidate, selected.Variant.Fields) {
			matched = append(matched, candidate)
		}
	}
	if len(matched) != 1 {
		return openAPISchema{}, fmt.Errorf("request body variant selector matched %d schemas, want exactly one", len(matched))
	}
	return matched[0], nil
}

func schemaContainsFields(schema openAPISchema, fields []string) bool {
	for _, field := range fields {
		if _, exists := schema.Properties[strings.TrimSpace(field)]; !exists {
			return false
		}
	}
	return true
}

func schemaSupportsType(schema openAPISchema, wantType, wantItems string) bool {
	if strings.TrimSpace(schema.Type) == strings.TrimSpace(wantType) {
		if wantType != "array" {
			return true
		}
		var items openAPISchema
		return len(schema.Items) > 0 && decodeJSONStrict(schema.Items, &items) == nil && strings.TrimSpace(items.Type) == strings.TrimSpace(wantItems)
	}
	for _, raw := range schema.OneOf {
		var candidate openAPISchema
		if decodeJSONStrict(raw, &candidate) == nil && schemaSupportsType(candidate, wantType, wantItems) {
			return true
		}
	}
	return false
}

func toolInputSchema(fieldType, itemsType string) (runtimecontracts.ToolInputSchema, error) {
	kind, err := generatedToolSchemaKind(fieldType)
	if err != nil {
		return runtimecontracts.ToolInputSchema{}, err
	}
	if kind == runtimecontracts.ToolSchemaArray {
		itemKind, err := generatedToolSchemaKind(itemsType)
		if err != nil {
			return runtimecontracts.ToolInputSchema{}, fmt.Errorf("array items: %w", err)
		}
		items, err := runtimecontracts.NewToolInputSchema(itemKind)
		if err != nil {
			return runtimecontracts.ToolInputSchema{}, err
		}
		return runtimecontracts.NewToolInputSchema(kind, runtimecontracts.ToolSchemaItems(items))
	}
	return runtimecontracts.NewToolInputSchema(kind)
}

func validateSelectedOutputSchema(document openAPIDocument, responses map[string]json.RawMessage, output GeneratorOutput) error {
	statuses := make([]string, 0, len(responses))
	for status := range responses {
		status = strings.TrimSpace(status)
		if len(status) == 3 && status[0] == '2' {
			statuses = append(statuses, status)
		}
	}
	sort.Strings(statuses)
	for _, status := range statuses {
		var response openAPIResponse
		if err := json.Unmarshal(responses[status], &response); err != nil {
			return fmt.Errorf("parse %s response: %w", status, err)
		}
		if strings.TrimSpace(response.Ref) != "" {
			return fmt.Errorf("%s response reference %q is unsupported", status, response.Ref)
		}
		media, ok := response.Content["application/json"]
		if !ok || len(media.Schema) == 0 {
			return fmt.Errorf("%s response has no application/json schema", status)
		}
		var schema openAPISchema
		if err := decodeJSONStrict(media.Schema, &schema); err != nil {
			return fmt.Errorf("parse %s application/json response schema: %w", status, err)
		}
		if err := validateOpenAPIOutputKind(document, schema, output, 0); err != nil {
			return fmt.Errorf("%s application/json response %w", status, err)
		}
	}
	return nil
}

func validateOpenAPIOutputKind(document openAPIDocument, schema openAPISchema, output GeneratorOutput, depth int) error {
	resolved, err := resolveOpenAPIOutputSchema(document, schema, depth)
	if err != nil {
		return err
	}
	want, err := generatedToolSchemaKind(output.Type)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resolved.Type) != string(want) {
		return fmt.Errorf("type %q does not match profile type %q", resolved.Type, want)
	}
	if want != runtimecontracts.ToolSchemaArray {
		return nil
	}
	if len(resolved.Items) == 0 {
		return fmt.Errorf("array items are missing")
	}
	var items openAPISchema
	if err := decodeJSONStrict(resolved.Items, &items); err != nil {
		return fmt.Errorf("parse array items: %w", err)
	}
	items, err = resolveOpenAPIOutputSchema(document, items, depth+1)
	if err != nil {
		return fmt.Errorf("array items: %w", err)
	}
	wantItems, err := generatedToolSchemaKind(output.ItemsType)
	if err != nil {
		return fmt.Errorf("array items: %w", err)
	}
	if strings.TrimSpace(items.Type) != string(wantItems) {
		return fmt.Errorf("array item type %q does not match profile items_type %q", items.Type, wantItems)
	}
	return nil
}

func resolveOpenAPIOutputSchema(document openAPIDocument, schema openAPISchema, depth int) (openAPISchema, error) {
	if strings.TrimSpace(schema.Ref) == "" {
		return schema, nil
	}
	if depth >= 32 {
		return openAPISchema{}, fmt.Errorf("local schema reference depth exceeds 32")
	}
	const prefix = "#/components/schemas/"
	if !strings.HasPrefix(schema.Ref, prefix) {
		return openAPISchema{}, fmt.Errorf("unsupported schema reference %q", schema.Ref)
	}
	name := strings.TrimPrefix(schema.Ref, prefix)
	raw, ok := document.Schemas[name]
	if !ok {
		return openAPISchema{}, fmt.Errorf("schema reference %q is missing", schema.Ref)
	}
	var resolved openAPISchema
	if err := json.Unmarshal(raw, &resolved); err != nil {
		return openAPISchema{}, fmt.Errorf("parse schema reference %q: %w", schema.Ref, err)
	}
	return resolveOpenAPIOutputSchema(document, resolved, depth+1)
}

func generatedToolSchemaKind(value string) (runtimecontracts.ToolSchemaKind, error) {
	switch runtimecontracts.ToolSchemaKind(strings.TrimSpace(value)) {
	case runtimecontracts.ToolSchemaString:
		return runtimecontracts.ToolSchemaString, nil
	case runtimecontracts.ToolSchemaInteger:
		return runtimecontracts.ToolSchemaInteger, nil
	case runtimecontracts.ToolSchemaNumber:
		return runtimecontracts.ToolSchemaNumber, nil
	case runtimecontracts.ToolSchemaBoolean:
		return runtimecontracts.ToolSchemaBoolean, nil
	case runtimecontracts.ToolSchemaObject:
		return runtimecontracts.ToolSchemaObject, nil
	case runtimecontracts.ToolSchemaArray:
		return runtimecontracts.ToolSchemaArray, nil
	case runtimecontracts.ToolSchemaNull:
		return runtimecontracts.ToolSchemaNull, nil
	default:
		return "", fmt.Errorf("unsupported generated tool schema type %q", value)
	}
}

func has2xxResponse(responses map[string]json.RawMessage) bool {
	for status := range responses {
		status = strings.TrimSpace(status)
		if len(status) == 3 && status[0] == '2' {
			return true
		}
	}
	return false
}

func decodeJSONStrict(body []byte, target any) error {
	decoder := json.NewDecoder(bytes.NewReader(body))
	decoder.DisallowUnknownFields()
	if err := decoder.Decode(target); err != nil {
		return err
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		if err == nil {
			return fmt.Errorf("multiple JSON values are not allowed")
		}
		return err
	}
	return nil
}

func cloneStringMap(in map[string]string) map[string]string {
	out := make(map[string]string, len(in))
	for key, value := range in {
		out[key] = value
	}
	return out
}
