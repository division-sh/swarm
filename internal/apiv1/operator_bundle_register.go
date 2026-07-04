package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"strings"
	"time"
	"unicode/utf8"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/store"

	"golang.org/x/text/unicode/norm"
	"gopkg.in/yaml.v3"
)

const bundleRegisterIdempotencyTTL = 24 * time.Hour

type BundleCatalogRegisterStore interface {
	UpsertBundleCatalog(context.Context, store.BundleCatalogUpsert) (store.BundleCatalogUpsertResult, error)
}

type bundleRegisterResult struct {
	BundleHash    string `json:"bundle_hash"`
	Registered    bool   `json:"registered"`
	HasData       bool   `json:"has_data"`
	DataSizeBytes int64  `json:"data_size_bytes"`
}

type bundleRegistrationEnvelopeV1 struct {
	APIVersion string                   `yaml:"api_version"`
	Files      []bundleRegistrationFile `yaml:"files"`
}

type bundleRegistrationFile struct {
	Path string  `yaml:"path"`
	Text *string `yaml:"text"`
}

type bundleRegistrationMaterializedInput struct {
	Label string
}

type bundleRegistrationDataEntry struct {
	Path string
	Data []byte
}

type bundleRegistrationRuntimeContext struct {
	RepoRoot         string
	PlatformSpecPath string
}

func OperatorBundleRegisterHandlers(opts OperatorReadOptions) map[string]MethodHandler {
	if opts.BundleCatalog == nil || opts.Idempotency == nil {
		return nil
	}
	if _, ok := opts.BundleCatalog.(BundleCatalogRegisterStore); !ok {
		return nil
	}
	now := opts.Now
	if now == nil {
		now = func() time.Time { return time.Now().UTC() }
	}
	return map[string]MethodHandler{
		"bundle.register": func(ctx context.Context, req Request) (any, error) {
			return executeBundleRegister(ctx, req, opts, now().UTC())
		},
	}
}

func executeBundleRegister(ctx context.Context, req Request, opts OperatorReadOptions, now time.Time) (any, error) {
	writer, err := requireBundleCatalogRegisterStore(opts.BundleCatalog)
	if err != nil {
		return nil, err
	}
	params, err := bundleRegistrationParamsFromRequest(req.Params)
	if err != nil {
		return nil, err
	}
	idempotencyKey, _, err := optionalStringParam(req.Params, "idempotency_key")
	if err != nil {
		return nil, err
	}
	completion, replay, err := opts.Idempotency.WithAPIIdempotency(ctx, store.APIIdempotencyRequest{
		Method:         req.Method,
		ActorTokenID:   req.ActorTokenID,
		IdempotencyKey: idempotencyKey,
		RequestHash:    req.RequestHash,
		TTL:            bundleRegisterIdempotencyTTL,
		Now:            now,
	}, func(ctx context.Context) (store.APIIdempotencyCompletion, error) {
		projection, err := buildBundleRegistrationProjection(params, bundleRegistrationRuntimeContextFromOptions(opts))
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		upsert, err := writer.UpsertBundleCatalog(ctx, store.BundleCatalogUpsert{
			BundleHash:  projection.BundleHash,
			ContentYAML: projection.ContentYAML,
			ParsedJSON:  projection.ParsedJSON,
			DataBlob:    projection.DataBlob,
			Metadata:    projection.Metadata,
		})
		if errors.Is(err, store.ErrBundleCatalogConflict) {
			return store.APIIdempotencyCompletion{}, NewApplicationError(BundleRegisterConflictCode, false, map[string]any{"bundle_hash": projection.BundleHash})
		}
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		result := bundleRegisterResult{
			BundleHash:    upsert.Detail.BundleHash,
			Registered:    upsert.Registered,
			HasData:       upsert.Detail.HasData,
			DataSizeBytes: upsert.Detail.DataSizeBytes,
		}
		raw, err := json.Marshal(result)
		if err != nil {
			return store.APIIdempotencyCompletion{}, err
		}
		return store.APIIdempotencyCompletion{ResourceID: result.BundleHash, Response: raw}, nil
	})
	if err != nil {
		return nil, bundleRegisterIdempotencyError(err)
	}
	var result bundleRegisterResult
	if err := json.Unmarshal(completion.Response, &result); err != nil {
		if replay {
			return nil, fmt.Errorf("decode bundle.register idempotency response: %w", err)
		}
		return nil, fmt.Errorf("decode bundle.register response: %w", err)
	}
	return result, nil
}

type bundleRegistrationParams struct {
	ContentYAML string
	DataBlob    []bundleRegistrationDataEntry
}

func bundleRegistrationParamsFromRequest(params map[string]any) (bundleRegistrationParams, error) {
	contentYAML, err := requiredStringParam(params, "content_yaml")
	if err != nil {
		return bundleRegistrationParams{}, err
	}
	dataBlob, err := bundleRegistrationDataBlobParam(params)
	if err != nil {
		return bundleRegistrationParams{}, err
	}
	return bundleRegistrationParams{ContentYAML: contentYAML, DataBlob: dataBlob}, nil
}

func bundleRegistrationDataBlobParam(params map[string]any) ([]bundleRegistrationDataEntry, error) {
	if params == nil || isEmptyParam(params["data_blob"]) {
		return nil, nil
	}
	raw, ok := params["data_blob"].(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_blob", "reason": "must be a BundleRegisterDataBlobV1 object"})
	}
	if len(raw) != 2 {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_blob", "reason": "must contain only api_version and entries"})
	}
	apiVersion, ok := raw["api_version"].(string)
	if !ok || strings.TrimSpace(apiVersion) != "swarm.bundle.data.v1" {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_blob.api_version", "reason": "must be swarm.bundle.data.v1"})
	}
	rawEntries, ok := raw["entries"].([]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_blob.entries", "reason": "must be an ordered array"})
	}
	out := make([]bundleRegistrationDataEntry, 0, len(rawEntries))
	seen := map[string]string{}
	var previous string
	for i, value := range rawEntries {
		field := fmt.Sprintf("data_blob.entries[%d]", i)
		entry, ok := value.(map[string]any)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": field, "reason": "must be an object"})
		}
		if len(entry) != 2 {
			return nil, NewInvalidParamsError(map[string]any{"field": field, "reason": "must contain only path and data_base64"})
		}
		rawPath, ok := entry["path"].(string)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": field + ".path", "reason": "must be a string"})
		}
		path, err := cleanBundleRegistrationPath(rawPath, field+".path")
		if err != nil {
			return nil, err
		}
		if err := validateBundleRegistrationDataPath(path, field+".path"); err != nil {
			return nil, err
		}
		folded := asciiFoldBundleRegistrationPath(path)
		if existing, exists := seen[folded]; exists {
			if existing == path {
				return nil, NewInvalidParamsError(map[string]any{"field": field + ".path", "reason": "duplicate bundle-relative path " + path})
			}
			return nil, NewInvalidParamsError(map[string]any{"field": field + ".path", "reason": "ASCII case-colliding bundle-relative paths " + existing + " and " + path})
		}
		if previous != "" && path <= previous {
			return nil, NewInvalidParamsError(map[string]any{"field": field + ".path", "reason": "data entries must be sorted strictly by normalized path"})
		}
		seen[folded] = path
		previous = path
		encoded, ok := entry["data_base64"].(string)
		if !ok {
			return nil, NewInvalidParamsError(map[string]any{"field": field + ".data_base64", "reason": "must be a base64 string"})
		}
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, NewInvalidParamsError(map[string]any{"field": field + ".data_base64", "reason": "must be standard padded base64"})
		}
		out = append(out, bundleRegistrationDataEntry{Path: path, Data: decoded})
	}
	if len(out) == 0 {
		return nil, nil
	}
	return out, nil
}

func buildBundleRegistrationProjection(params bundleRegistrationParams, runtimeCtx bundleRegistrationRuntimeContext) (runtimecontracts.BundleCatalogProjection, error) {
	root, inputs, err := materializeBundleRegistration(params)
	if err != nil {
		return runtimecontracts.BundleCatalogProjection{}, err
	}
	defer os.RemoveAll(root)

	repoRoot := strings.TrimSpace(runtimeCtx.RepoRoot)
	platformSpec := strings.TrimSpace(runtimeCtx.PlatformSpecPath)
	platformHash, err := fileSHA256Hex(platformSpec)
	if err != nil {
		return runtimecontracts.BundleCatalogProjection{}, err
	}
	bundle, err := runtimecontracts.LoadWorkflowContractBundleWithOverrides(repoRoot, root, platformSpec)
	if err != nil {
		return runtimecontracts.BundleCatalogProjection{}, NewInvalidParamsError(map[string]any{"field": "content_yaml", "reason": "bundle registration envelope does not materialize a valid workflow contract bundle", "error": err.Error()})
	}
	if err := runtimecontracts.ValidateBundlePlatformVersionCompatibility(bundle); err != nil {
		return runtimecontracts.BundleCatalogProjection{}, NewInvalidParamsError(map[string]any{"field": "content_yaml", "reason": "bundle registration envelope declares incompatible platform_version", "error": err.Error()})
	}
	projection, err := runtimecontracts.BuildBundleCatalogProjectionWithOptions(bundle, runtimecontracts.BundleCatalogProjectionOptions{
		Source:             "bundle.register",
		PlatformSpecSHA256: platformHash,
	})
	if err != nil {
		return runtimecontracts.BundleCatalogProjection{}, err
	}
	if err := verifyBundleRegistrationInputsConsumed(inputs, projection); err != nil {
		return runtimecontracts.BundleCatalogProjection{}, err
	}
	projection.Metadata = bundleRegistrationMetadata(projection, platformHash)
	return projection, nil
}

func materializeBundleRegistration(params bundleRegistrationParams) (string, []bundleRegistrationMaterializedInput, error) {
	var envelope bundleRegistrationEnvelopeV1
	decoder := yaml.NewDecoder(strings.NewReader(params.ContentYAML))
	decoder.KnownFields(true)
	if err := decoder.Decode(&envelope); err != nil {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml", "reason": "must be a BundleRegistrationEnvelopeV1 YAML document"})
	}
	var trailing any
	if err := decoder.Decode(&trailing); err != io.EOF {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml", "reason": "must contain exactly one YAML document"})
	}
	if strings.TrimSpace(envelope.APIVersion) != "swarm.bundle.register.v1" {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml.api_version", "reason": "must be swarm.bundle.register.v1"})
	}
	if len(envelope.Files) == 0 {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml.files", "reason": "must contain at least package.yaml"})
	}
	root, err := os.MkdirTemp("", "swarm-bundle-register-*")
	if err != nil {
		return "", nil, err
	}
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.RemoveAll(root)
		}
	}()

	seen := map[string]string{}
	var inputs []bundleRegistrationMaterializedInput
	writeInput := func(rel string, content []byte, field string) error {
		clean, err := cleanBundleRegistrationPath(rel, field)
		if err != nil {
			return err
		}
		folded := asciiFoldBundleRegistrationPath(clean)
		if existing, exists := seen[folded]; exists {
			if existing != clean {
				return NewInvalidParamsError(map[string]any{"field": field, "reason": "ASCII case-colliding bundle-relative paths " + existing + " and " + clean})
			}
			return NewInvalidParamsError(map[string]any{"field": field, "reason": "duplicate bundle-relative path " + clean})
		}
		seen[folded] = clean
		path := filepath.Join(root, filepath.FromSlash(clean))
		if err := os.MkdirAll(filepath.Dir(path), 0o755); err != nil {
			return err
		}
		if err := os.WriteFile(path, content, 0o644); err != nil {
			return err
		}
		inputs = append(inputs, bundleRegistrationMaterializedInput{Label: "bundle/" + clean})
		return nil
	}
	for i, file := range envelope.Files {
		field := fmt.Sprintf("content_yaml.files[%d].path", i)
		if file.Text == nil {
			return "", nil, NewInvalidParamsError(map[string]any{"field": fmt.Sprintf("content_yaml.files[%d].text", i), "reason": "text is required"})
		}
		if err := writeInput(file.Path, []byte(*file.Text), field); err != nil {
			return "", nil, err
		}
	}
	for i, entry := range params.DataBlob {
		if err := writeInput(entry.Path, entry.Data, fmt.Sprintf("data_blob.entries[%d].path", i)); err != nil {
			return "", nil, err
		}
	}
	if seen["package.yaml"] != "package.yaml" {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml.files", "reason": "package.yaml is required"})
	}
	cleanup = false
	return root, inputs, nil
}

func cleanBundleRegistrationPath(raw, field string) (string, error) {
	if raw == "" {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be non-empty"})
	}
	if strings.TrimSpace(raw) != raw {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must not contain surrounding whitespace"})
	}
	if !utf8.ValidString(raw) {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be valid UTF-8"})
	}
	if norm.NFC.String(raw) != raw {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be NFC-normalized"})
	}
	if strings.Contains(raw, "\x00") || strings.Contains(raw, "\\") || strings.HasPrefix(raw, "/") || filepath.IsAbs(raw) {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be a slash-separated relative bundle path"})
	}
	for _, segment := range strings.Split(raw, "/") {
		if segment == "" || segment == "." || segment == ".." {
			return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must not contain empty, '.', or '..' segments"})
		}
	}
	return raw, nil
}

func validateBundleRegistrationDataPath(path, field string) error {
	segments := strings.Split(path, "/")
	if !bundleRegistrationDataPathIsFlowData(segments) {
		return NewInvalidParamsError(map[string]any{"field": field, "reason": "data_blob entries must be under a flow data directory (.../flows/<flow>/data/...)"})
	}
	return nil
}

func bundleRegistrationDataPathIsFlowData(segments []string) bool {
	for i := 0; i+3 < len(segments); i++ {
		if segments[i] == "flows" && segments[i+1] != "" && segments[i+2] == "data" {
			return true
		}
	}
	return false
}

func asciiFoldBundleRegistrationPath(path string) string {
	buf := []byte(path)
	for i, b := range buf {
		if b >= 'A' && b <= 'Z' {
			buf[i] = b + ('a' - 'A')
		}
	}
	return string(buf)
}

func bundleRegistrationMetadata(projection runtimecontracts.BundleCatalogProjection, platformHash string) map[string]any {
	metadata := map[string]any{
		"api_version":               "swarm.bundle.metadata.v1",
		"registered_by":             "bundle.register",
		"platform_spec_source":      "server_effective",
		"platform_spec_hash":        "sha256:" + platformHash,
		"platform_spec_path_policy": "server_internal_not_user_supplied",
	}
	for _, key := range []string{"projection_version", "workflow_name", "workflow_version", "file_count", "data_file_count"} {
		if value, ok := projection.Metadata[key]; ok {
			metadata[key] = value
		}
	}
	return metadata
}

func verifyBundleRegistrationInputsConsumed(inputs []bundleRegistrationMaterializedInput, projection runtimecontracts.BundleCatalogProjection) error {
	consumed := map[string]struct{}{}
	files, ok := projection.ParsedJSON["files"].([]map[string]any)
	if ok {
		for _, file := range files {
			if label, ok := file["label"].(string); ok {
				consumed[strings.TrimSpace(label)] = struct{}{}
			}
		}
	} else if rawFiles, ok := projection.ParsedJSON["files"].([]any); ok {
		for _, raw := range rawFiles {
			file, ok := raw.(map[string]any)
			if !ok {
				continue
			}
			if label, ok := file["label"].(string); ok {
				consumed[strings.TrimSpace(label)] = struct{}{}
			}
		}
	}
	for _, input := range inputs {
		if _, ok := consumed[input.Label]; !ok {
			return NewInvalidParamsError(map[string]any{
				"field":  "content_yaml",
				"reason": "materialized input is not consumed by canonical bundle_hash owner",
				"label":  input.Label,
			})
		}
	}
	return nil
}

func requireBundleCatalogRegisterStore(reads BundleCatalogReadStore) (BundleCatalogRegisterStore, error) {
	writer, ok := reads.(BundleCatalogRegisterStore)
	if !ok || writer == nil {
		return nil, fmt.Errorf("bundle catalog register store is required")
	}
	return writer, nil
}

func bundleRegisterIdempotencyError(err error) error {
	var conflict *store.APIIdempotencyConflictError
	if errors.As(err, &conflict) {
		return NewApplicationError(IdempotencyConflictCode, false, map[string]any{
			"original_request_hash":    conflict.OriginalRequestHash,
			"conflicting_request_hash": conflict.ConflictingRequestHash,
			"original_response_ref": map[string]any{
				"method":      conflict.Method,
				"resource_id": conflict.ResourceID,
			},
		})
	}
	return err
}

func bundleRegistrationRuntimeContextFromOptions(opts OperatorReadOptions) bundleRegistrationRuntimeContext {
	repoRoot := strings.TrimSpace(opts.RepoRoot)
	platformSpec := strings.TrimSpace(opts.PlatformSpecPath)
	if platformSpec == "" {
		if repoRoot == "" {
			repoRoot = defaultBundleRegistrationRepoRoot()
		}
		platformSpec = runtimecontracts.DefaultPlatformSpecFile(repoRoot)
	}
	if repoRoot == "" {
		repoRoot = filepath.Dir(platformSpec)
	}
	return bundleRegistrationRuntimeContext{
		RepoRoot:         repoRoot,
		PlatformSpecPath: platformSpec,
	}
}

func fileSHA256Hex(path string) (string, error) {
	raw, err := os.ReadFile(path)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:]), nil
}

func defaultBundleRegistrationRepoRoot() string {
	return "."
}
