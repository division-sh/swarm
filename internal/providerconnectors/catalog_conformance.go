package providerconnectors

import (
	"fmt"
	"io/fs"
	"net/url"
	"path"
	"reflect"
	"sort"
	"strconv"
	"strings"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/httpresponsesuccess"
)

const catalogConformanceRoot = "catalog/conformance"

type CatalogConformanceFixture struct {
	SchemaVersion   string                               `yaml:"schema_version"`
	Provider        string                               `yaml:"provider"`
	OperationID     string                               `yaml:"operation_id"`
	ToolID          string                               `yaml:"tool_id"`
	SourceSHA256    string                               `yaml:"source_sha256"`
	ProfileSHA256   string                               `yaml:"profile_sha256"`
	ManifestSHA256  string                               `yaml:"manifest_sha256"`
	FixtureStatus   string                               `yaml:"fixture_status"`
	ReviewStatus    string                               `yaml:"review_status"`
	Input           map[string]any                       `yaml:"input"`
	Credentials     map[string]string                    `yaml:"credentials"`
	Expected        CatalogConformanceExpectedRequest    `yaml:"expected"`
	ResponseSuccess runtimecontracts.HTTPResponseSuccess `yaml:"response_success"`
}

type CatalogConformanceExpectedRequest struct {
	Method  string            `yaml:"method"`
	URL     string            `yaml:"url"`
	Headers map[string]string `yaml:"headers"`
	Body    map[string]any    `yaml:"body"`
}

func ParseCatalogConformanceFixture(body []byte) (CatalogConformanceFixture, error) {
	var fixture CatalogConformanceFixture
	if err := decodeYAMLStrict(body, &fixture); err != nil {
		return CatalogConformanceFixture{}, err
	}
	if err := fixture.Validate(); err != nil {
		return CatalogConformanceFixture{}, err
	}
	return fixture, nil
}

func (f CatalogConformanceFixture) Validate() error {
	if strings.TrimSpace(f.SchemaVersion) != CatalogSchemaVersion {
		return fmt.Errorf("catalog conformance schema_version must be %q", CatalogSchemaVersion)
	}
	if normalizeToken(f.Provider) == "" || strings.TrimSpace(f.OperationID) == "" || strings.TrimSpace(f.ToolID) == "" {
		return fmt.Errorf("catalog conformance provider, operation_id, and tool_id are required")
	}
	if err := validateSHA256("catalog conformance source", f.SourceSHA256); err != nil {
		return err
	}
	if err := validateSHA256("catalog conformance profile", f.ProfileSHA256); err != nil {
		return err
	}
	if err := validateSHA256("catalog conformance manifest", f.ManifestSHA256); err != nil {
		return err
	}
	if strings.TrimSpace(f.FixtureStatus) != GenerationFixturePassing || strings.TrimSpace(f.ReviewStatus) != GenerationReviewApproved {
		return fmt.Errorf("catalog conformance fixture_status/review_status must be passing/approved")
	}
	if f.Input == nil || f.Credentials == nil || strings.TrimSpace(f.Expected.Method) == "" || strings.TrimSpace(f.Expected.URL) == "" || f.Expected.Headers == nil || f.Expected.Body == nil {
		return fmt.Errorf("catalog conformance input, credentials, and expected method/url/headers/body are required, including declared-empty maps")
	}
	for key, value := range f.Credentials {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" {
			return fmt.Errorf("catalog conformance credential names and fixture values must be non-empty")
		}
	}
	for key, value := range f.Expected.Headers {
		if strings.TrimSpace(key) == "" || strings.TrimSpace(value) == "" || strings.Contains(value, "{{") {
			return fmt.Errorf("catalog conformance expected headers must be non-empty resolved values")
		}
	}
	if err := validateURLNoCredentials(f.Expected.URL); err != nil {
		return err
	}
	return nil
}

func validateURLNoCredentials(raw string) error {
	parsed, err := url.ParseRequestURI(strings.TrimSpace(raw))
	if err != nil {
		return fmt.Errorf("catalog conformance expected URL is invalid: %w", err)
	}
	if parsed.User != nil {
		return fmt.Errorf("catalog conformance expected URL must not carry credentials")
	}
	return nil
}

func ValidateCatalogConformance(fsys fs.FS, artifacts []GeneratedCatalogArtifact) error {
	referenced := map[string]struct{}{}
	seenKeys := map[string]string{}
	for _, artifact := range artifacts {
		if artifact.Manifest.Generation == nil {
			return fmt.Errorf("generated artifact %q lacks generation evidence", artifact.IndexEntry.ID)
		}
		for _, operation := range artifact.Manifest.Generation.Operations {
			fixturePath, err := catalogFixturePath(operation.FixtureID)
			if err != nil {
				return err
			}
			if _, exists := referenced[fixturePath]; exists {
				return fmt.Errorf("catalog conformance fixture %q is referenced more than once", fixturePath)
			}
			referenced[fixturePath] = struct{}{}
			body, err := fs.ReadFile(fsys, fixturePath)
			if err != nil {
				return fmt.Errorf("read catalog conformance fixture %q: %w", fixturePath, err)
			}
			fixture, err := ParseCatalogConformanceFixture(body)
			if err != nil {
				return fmt.Errorf("parse catalog conformance fixture %q: %w", fixturePath, err)
			}
			key := catalogConformanceKey(fixture.Provider, fixture.OperationID, fixture.ToolID)
			if prior, exists := seenKeys[key]; exists {
				return fmt.Errorf("catalog conformance key %q is duplicated by %s and %s", key, prior, fixturePath)
			}
			seenKeys[key] = fixturePath
			if err := validateCatalogFixtureBinding(artifact, operation, fixture); err != nil {
				return fmt.Errorf("catalog conformance fixture %q: %w", fixturePath, err)
			}
		}
	}
	var extra []string
	err := fs.WalkDir(fsys, catalogConformanceRoot, func(name string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".yaml") {
			return nil
		}
		if _, exists := referenced[name]; !exists {
			extra = append(extra, name)
		}
		return nil
	})
	if err != nil {
		return fmt.Errorf("walk catalog conformance fixtures: %w", err)
	}
	if len(extra) > 0 {
		sort.Strings(extra)
		return fmt.Errorf("unreferenced catalog conformance fixtures: %s", strings.Join(extra, ", "))
	}
	return nil
}

func validateCatalogFixtureBinding(artifact GeneratedCatalogArtifact, operation GenerationOperationEvidence, fixture CatalogConformanceFixture) error {
	if normalizeToken(fixture.Provider) != normalizeToken(artifact.Manifest.Provider) || strings.TrimSpace(fixture.OperationID) != strings.TrimSpace(operation.OperationID) || strings.TrimSpace(fixture.ToolID) != strings.TrimSpace(operation.ToolID) {
		return fmt.Errorf("provider/operation/tool identity does not match generated evidence")
	}
	if normalizeSHA(fixture.SourceSHA256) != normalizeSHA(artifact.SourceHash) || normalizeSHA(fixture.ProfileSHA256) != normalizeSHA(artifact.ProfileHash) || normalizeSHA(fixture.ManifestSHA256) != normalizeSHA(artifact.PackEnvelope.ManifestHash) {
		return fmt.Errorf("source/profile/manifest freshness binding does not match generated output")
	}
	if strings.TrimSpace(fixture.FixtureStatus) != strings.TrimSpace(operation.FixtureStatus) || strings.TrimSpace(fixture.ReviewStatus) != strings.TrimSpace(operation.ReviewStatus) {
		return fmt.Errorf("fixture/review status does not match generated evidence")
	}
	if !httpresponsesuccess.Equivalent(fixture.ResponseSuccess, operation.ResponseSuccess) {
		return fmt.Errorf("response_success does not match generated evidence")
	}
	tool, exists := artifact.Manifest.Tools[strings.TrimSpace(operation.ToolID)]
	if !exists || tool.HTTP == nil {
		return fmt.Errorf("generated tool is unavailable or lacks HTTP declaration")
	}
	if strings.ToUpper(strings.TrimSpace(fixture.Expected.Method)) != strings.ToUpper(strings.TrimSpace(tool.HTTP.Method)) {
		return fmt.Errorf("expected method does not match generated tool")
	}
	if err := validateCatalogFixtureCredentials(tool, fixture.Credentials); err != nil {
		return err
	}
	resolvedURL, err := resolveCatalogTemplateString(tool.HTTP.URL, fixture.Input, fixture.Credentials, true)
	if err != nil {
		return err
	}
	if strings.TrimSpace(resolvedURL) != strings.TrimSpace(fixture.Expected.URL) {
		return fmt.Errorf("expected URL %q does not match generated %q", fixture.Expected.URL, resolvedURL)
	}
	resolvedHeaders := make(map[string]string, len(tool.HTTP.Headers))
	for key, value := range tool.HTTP.Headers {
		resolved, err := resolveCatalogTemplateString(value, fixture.Input, fixture.Credentials, false)
		if err != nil {
			return fmt.Errorf("resolve catalog conformance header %q: %w", key, err)
		}
		resolvedHeaders[key] = resolved
	}
	if !reflect.DeepEqual(resolvedHeaders, fixture.Expected.Headers) {
		return fmt.Errorf("expected resolved headers %#v do not match generated %#v", fixture.Expected.Headers, resolvedHeaders)
	}
	resolvedBody, err := resolveCatalogTemplateValue(tool.HTTP.Body, fixture.Input, fixture.Credentials)
	if err != nil {
		return err
	}
	if !reflect.DeepEqual(normalizeYAMLValue(resolvedBody), normalizeYAMLValue(fixture.Expected.Body)) {
		return fmt.Errorf("expected body %#v does not match generated %#v", fixture.Expected.Body, resolvedBody)
	}
	return nil
}

func validateCatalogFixtureCredentials(tool runtimecontracts.ToolSchemaEntry, credentials map[string]string) error {
	expected := make(map[string]struct{}, len(tool.Credentials))
	for _, key := range tool.Credentials {
		key = strings.TrimSpace(key)
		if key != "" {
			expected[key] = struct{}{}
		}
	}
	for key := range expected {
		if _, exists := credentials[key]; !exists {
			return fmt.Errorf("catalog conformance credential %q is unavailable", key)
		}
	}
	for key := range credentials {
		if _, exists := expected[strings.TrimSpace(key)]; !exists {
			return fmt.Errorf("catalog conformance credential %q is not declared by the generated tool", key)
		}
	}
	return nil
}

func catalogFixturePath(id string) (string, error) {
	id = strings.Trim(strings.TrimSpace(id), "/")
	if id == "" || path.Clean(id) != id || strings.HasPrefix(id, "../") || strings.Contains(id, "\\") {
		return "", fmt.Errorf("catalog conformance fixture id %q is invalid", id)
	}
	return path.Join(catalogConformanceRoot, id+".yaml"), nil
}

func catalogConformanceKey(provider, operationID, toolID string) string {
	return normalizeToken(provider) + "|" + strings.TrimSpace(operationID) + "|" + strings.TrimSpace(toolID)
}

func normalizeSHA(raw string) string {
	return strings.TrimPrefix(strings.TrimSpace(raw), "sha256:")
}

func resolveCatalogTemplateValue(value any, input map[string]any, credentials map[string]string) (any, error) {
	switch typed := value.(type) {
	case string:
		trimmed := strings.TrimSpace(typed)
		if strings.HasPrefix(trimmed, "{{") && strings.HasSuffix(trimmed, "}}") && strings.Count(trimmed, "{{") == 1 && strings.Count(trimmed, "}}") == 1 {
			return resolveCatalogTemplateToken(strings.TrimSuffix(strings.TrimPrefix(trimmed, "{{"), "}}"), input, credentials)
		}
		return resolveCatalogTemplateString(typed, input, credentials, false)
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			resolved, err := resolveCatalogTemplateValue(item, input, credentials)
			if err != nil {
				return nil, err
			}
			out[key] = resolved
		}
		return out, nil
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			resolved, err := resolveCatalogTemplateValue(item, input, credentials)
			if err != nil {
				return nil, err
			}
			out[i] = resolved
		}
		return out, nil
	default:
		return value, nil
	}
}

func resolveCatalogTemplateString(raw string, input map[string]any, credentials map[string]string, escapePath bool) (string, error) {
	out := raw
	for {
		start := strings.Index(out, "{{")
		if start < 0 {
			break
		}
		endOffset := strings.Index(out[start:], "}}")
		if endOffset < 0 {
			return "", fmt.Errorf("unterminated catalog template in %q", raw)
		}
		end := start + endOffset + 2
		token := strings.TrimSpace(strings.TrimSuffix(strings.TrimPrefix(out[start:end], "{{"), "}}"))
		value, err := resolveCatalogTemplateToken(token, input, credentials)
		if err != nil {
			return "", err
		}
		replacement := fmt.Sprint(value)
		if escapePath {
			replacement = url.PathEscape(replacement)
		}
		out = out[:start] + replacement + out[end:]
	}
	return out, nil
}

func resolveCatalogTemplateToken(token string, input map[string]any, credentials map[string]string) (any, error) {
	token = strings.TrimSpace(token)
	switch {
	case strings.HasPrefix(token, "input."):
		key := strings.TrimSpace(strings.TrimPrefix(token, "input."))
		value, exists := input[key]
		if !exists {
			return nil, fmt.Errorf("catalog conformance input %q is unavailable", key)
		}
		return value, nil
	case strings.HasPrefix(token, "credentials."):
		key := strings.TrimSpace(strings.TrimPrefix(token, "credentials."))
		value, exists := credentials[key]
		if !exists {
			return nil, fmt.Errorf("catalog conformance credential %q is unavailable", key)
		}
		return value, nil
	default:
		return nil, fmt.Errorf("catalog conformance template %q is unsupported", token)
	}
}

func normalizeYAMLValue(value any) any {
	switch typed := value.(type) {
	case map[string]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[key] = normalizeYAMLValue(item)
		}
		return out
	case map[any]any:
		out := make(map[string]any, len(typed))
		for key, item := range typed {
			out[fmt.Sprint(key)] = normalizeYAMLValue(item)
		}
		return out
	case []any:
		out := make([]any, len(typed))
		for i, item := range typed {
			out[i] = normalizeYAMLValue(item)
		}
		return out
	case int:
		return strconv.FormatInt(int64(typed), 10)
	case int64:
		return strconv.FormatInt(typed, 10)
	default:
		return value
	}
}
