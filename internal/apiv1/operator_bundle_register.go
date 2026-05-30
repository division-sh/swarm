package apiv1

import (
	"context"
	"crypto/sha256"
	"encoding/base64"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"time"

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/store"

	"gopkg.in/yaml.v3"
)

const bundleRegisterIdempotencyTTL = 24 * time.Hour

type BundleCatalogRegisterStore interface {
	UpsertBundleCatalog(context.Context, store.BundleCatalogUpsert) (store.BundleCatalogUpsertResult, error)
}

type bundleRegisterResult struct {
	BundleHash          string `json:"bundle_hash"`
	Registered          bool   `json:"registered"`
	IdempotencyReplayed bool   `json:"idempotency_replayed"`
}

type bundleRegistrationEnvelopeV1 struct {
	Version string                   `yaml:"version"`
	Files   []bundleRegistrationFile `yaml:"files"`
}

type bundleRegistrationFile struct {
	Path    string `yaml:"path"`
	Content string `yaml:"content"`
}

type bundleRegistrationMaterializedInput struct {
	Label string
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
			BundleHash: upsert.Detail.BundleHash,
			Registered: upsert.Registered,
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
	result.IdempotencyReplayed = replay
	return result, nil
}

type bundleRegistrationParams struct {
	ContentYAML string
	DataBlob    map[string][]byte
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

func bundleRegistrationDataBlobParam(params map[string]any) (map[string][]byte, error) {
	if params == nil || isEmptyParam(params["data_blob"]) {
		return nil, nil
	}
	raw, ok := params["data_blob"].(map[string]any)
	if !ok {
		return nil, NewInvalidParamsError(map[string]any{"field": "data_blob", "reason": "must be an object mapping bundle-relative paths to base64 strings"})
	}
	out := make(map[string][]byte, len(raw))
	for key, value := range raw {
		path, err := cleanBundleRegistrationPath(key, "data_blob")
		if err != nil {
			return nil, err
		}
		encoded, ok := value.(string)
		if !ok || strings.TrimSpace(encoded) == "" {
			return nil, NewInvalidParamsError(map[string]any{"field": "data_blob." + path, "reason": "must be a non-empty base64 string"})
		}
		decoded, err := base64.StdEncoding.DecodeString(strings.TrimSpace(encoded))
		if err != nil {
			return nil, NewInvalidParamsError(map[string]any{"field": "data_blob." + path, "reason": "must be standard base64"})
		}
		out[path] = decoded
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
	return projection, nil
}

func materializeBundleRegistration(params bundleRegistrationParams) (string, []bundleRegistrationMaterializedInput, error) {
	var envelope bundleRegistrationEnvelopeV1
	if err := yaml.Unmarshal([]byte(params.ContentYAML), &envelope); err != nil {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml", "reason": "must be a BundleRegistrationEnvelopeV1 YAML document"})
	}
	if strings.TrimSpace(envelope.Version) != "swarm.bundle.registration.v1" {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml.version", "reason": "must be swarm.bundle.registration.v1"})
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

	seen := map[string]struct{}{}
	var inputs []bundleRegistrationMaterializedInput
	writeInput := func(rel string, content []byte, field string) error {
		clean, err := cleanBundleRegistrationPath(rel, field)
		if err != nil {
			return err
		}
		if _, exists := seen[clean]; exists {
			return NewInvalidParamsError(map[string]any{"field": field, "reason": "duplicate bundle-relative path " + clean})
		}
		seen[clean] = struct{}{}
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
		if err := writeInput(file.Path, []byte(file.Content), field); err != nil {
			return "", nil, err
		}
	}
	for rel, content := range params.DataBlob {
		if err := writeInput(rel, content, "data_blob"); err != nil {
			return "", nil, err
		}
	}
	if _, ok := seen["package.yaml"]; !ok {
		return "", nil, NewInvalidParamsError(map[string]any{"field": "content_yaml.files", "reason": "package.yaml is required"})
	}
	cleanup = false
	return root, inputs, nil
}

func cleanBundleRegistrationPath(raw, field string) (string, error) {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be non-empty"})
	}
	if strings.Contains(raw, "\x00") || strings.Contains(raw, "\\") || filepath.IsAbs(raw) {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must be a slash-separated relative bundle path"})
	}
	clean := filepath.ToSlash(filepath.Clean(raw))
	if clean == "." || strings.HasPrefix(clean, "../") || clean == ".." || strings.Contains(clean, "/../") {
		return "", NewInvalidParamsError(map[string]any{"field": field, "reason": "path must stay inside the bundle root"})
	}
	return clean, nil
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
