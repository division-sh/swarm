package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"errors"
	"fmt"
	"os"
	"os/exec"
	"path"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/google/uuid"
	"gopkg.in/yaml.v3"

	"github.com/division-sh/swarm/internal/events"
	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
	"github.com/division-sh/swarm/internal/runtime/core/identity"
	"github.com/division-sh/swarm/internal/runtime/core/paths"
	runtimeengine "github.com/division-sh/swarm/internal/runtime/engine"
	runtimefailures "github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/semanticview"
)

const (
	artifactRepoCommitActionID   = "artifact_repo_commit"
	artifactRepoProviderLocalGit = "local_git"
	artifactRepoPublicScheme     = "swarm-artifact://repos/"
	defaultArtifactRoot          = "/var/lib/swarm/artifacts"
	defaultYAMLMaxBytes          = 1 << 20
	defaultMarkdownMaxBytes      = 5 << 20
	defaultTextMaxBytes          = 1 << 20
	defaultRepoMaxBytes          = 50 << 20
)

var errArtifactRepoResultEmitCollectorMissing = errors.New("artifact_repo_commit result event requires transactional action emit collector")

var invalidArtifactRootMounts = []string{"/workspace", "/data", "/opt/swarm/contracts"}

type artifactRepoPreparedFile struct {
	Path        string
	Content     []byte
	ContentType string
	SHA256      string
	Size        int
}

func (pc *PipelineCoordinator) commitArtifactRepo(ctx context.Context, action runtimecontracts.ActionSpec, execCtx runtimeengine.ExecutionContext) error {
	if pc == nil {
		return fmt.Errorf("artifact_repo_commit requires pipeline coordinator")
	}
	if pc.workflowStore == nil || !pc.workflowStore.Enabled() {
		return fmt.Errorf("artifact_repo_commit requires workflow instance store")
	}
	spec := action.ArtifactRepo
	if spec == nil {
		return fmt.Errorf("artifact_repo_commit requires artifact_repo declaration")
	}
	if strings.TrimSpace(spec.Provider) != artifactRepoProviderLocalGit {
		return fmt.Errorf("artifact_repo_commit provider %q is unsupported", strings.TrimSpace(spec.Provider))
	}
	sourceEventID := strings.TrimSpace(execCtx.Request.Event.ID())
	if _, err := uuid.Parse(sourceEventID); err != nil {
		return fmt.Errorf("artifact_repo_commit requires UUID source event id: %w", err)
	}
	repoID, err := requiredArtifactUUID(execCtx.Base, spec.RepoID, "artifact_repo.repo_id")
	if err != nil {
		return err
	}
	namespace, err := artifactNamespace(execCtx, spec)
	if err != nil {
		return err
	}
	requestID, err := requiredArtifactUUID(execCtx.Base, spec.RequestID, "artifact_repo.request_id")
	if err != nil {
		return err
	}
	partitionKey := ""
	provenance := map[string]any{}
	displaySlug := ""
	fail := func(err error) error {
		if err == nil {
			return nil
		}
		return pc.persistAndPublishArtifactRepoFailure(ctx, execCtx, spec, repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, err)
	}
	partitionKey, err = optionalArtifactSegment(execCtx.Base, spec.PartitionKey, "artifact_repo.partition_key")
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassSchemaInvalid, "artifact_repo_partition_key_invalid", "resolve_input"))
	}
	displaySlug, err = optionalArtifactDisplaySlug(execCtx.Base, spec.DisplaySlug)
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassSchemaInvalid, "artifact_repo_display_slug_invalid", "resolve_input"))
	}
	provenance, err = artifactRepoProvenance(execCtx.Base, spec)
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassSchemaInvalid, "artifact_repo_provenance_invalid", "resolve_input"))
	}
	if previous := strings.TrimSpace(asString(execCtx.Request.State.StateCarrier.Metadata[spec.Output.LastSourceEventID])); previous == sourceEventID && artifactRepoOutputsComplete(execCtx.Request.State.StateCarrier.Metadata, spec) {
		return nil
	}
	files, treeHash, err := prepareArtifactRepoFiles(execCtx.Base, spec)
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassSchemaInvalid, "artifact_repo_file_invalid", "validate_input"))
	}
	if previousRequest := strings.TrimSpace(asString(execCtx.Request.State.StateCarrier.Metadata[spec.Output.LastRequestID])); previousRequest == requestID {
		if currentManifest, ok := execCtx.Request.State.StateCarrier.Metadata[spec.Output.FileManifest].(map[string]any); ok {
			if previousTree := strings.TrimSpace(asString(currentManifest["tree_hash"])); previousTree != "" && previousTree != treeHash {
				return fail(runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "artifact_repo_request_conflict", "artifact-repo", "admit_request", map[string]any{"request_id": requestID}))
			}
		}
	}
	artifactRoot, err := pc.artifactRepoRoot()
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_root_unavailable", "resolve_storage"))
	}
	repoPath, err := artifactRepoPath(artifactRoot, namespace, repoID)
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassSchemaInvalid, "artifact_repo_path_invalid", "resolve_storage"))
	}
	if err := ensureArtifactRepoInitialized(ctx, repoPath, commitTime(execCtx.Request.Event.CreatedAt())); err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_provider_unavailable", "initialize_repository"))
	}
	if previous, found, err := artifactRepoRequestRecord(ctx, repoPath, requestID); err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_history_unavailable", "read_history"))
	} else if found {
		if previous.TreeHash != treeHash {
			return fail(runtimefailures.New(runtimefailures.ClassConflictingDuplicate, "artifact_repo_request_conflict", "artifact-repo", "admit_request", map[string]any{"request_id": requestID}))
		}
		repoURL := artifactRepoPublicScheme + repoID
		manifest := artifactRepoManifest(repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, repoURL, previous.Ref, treeHash, files)
		successPayload, err := artifactRepoSuccessPayload(execCtx.Base, spec, repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, repoURL, previous.Ref, manifest)
		if err != nil {
			return fail(err)
		}
		if successEvent := strings.TrimSpace(spec.SuccessEvent); successEvent != "" {
			if err := pc.validateArtifactRepoResultPayload(execCtx, successEvent, successPayload); err != nil {
				return fail(err)
			}
		}
		return pc.persistAndPublishArtifactRepoSuccess(ctx, execCtx, spec, repoURL, previous.Ref, manifest, requestID, sourceEventID, successPayload)
	}
	if err := writeArtifactRepoFiles(repoPath, files); err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_write_failed", "materialize"))
	}
	if size, err := artifactRepoProjectedTreeSize(ctx, repoPath, files); err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_size_check_failed", "measure_repository"))
	} else if maxRepo := artifactRepoMaxBytes(spec.Limits); maxRepo > 0 && size > maxRepo {
		return fail(runtimefailures.New(runtimefailures.ClassDataLimitExceeded, "artifact_repo_tree_limit_exceeded", "artifact-repo", "materialize", map[string]any{
			"limit_kind": "repository_bytes",
			"actual":     size,
			"limit":      maxRepo,
		}))
	}
	ref, err := commitArtifactRepoFiles(ctx, repoPath, files, sourceEventID, requestID, treeHash, optionalArtifactString(execCtx.Base, spec.Author), commitTime(execCtx.Request.Event.CreatedAt()))
	if err != nil {
		return fail(artifactRepoClassify(err, runtimefailures.ClassDependencyUnavailable, "artifact_repo_commit_failed", "commit_repository"))
	}
	repoURL := artifactRepoPublicScheme + repoID
	manifest := artifactRepoManifest(repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, repoURL, ref, treeHash, files)
	successPayload, err := artifactRepoSuccessPayload(execCtx.Base, spec, repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, repoURL, ref, manifest)
	if err != nil {
		return fail(err)
	}
	if successEvent := strings.TrimSpace(spec.SuccessEvent); successEvent != "" {
		if err := pc.validateArtifactRepoResultPayload(execCtx, successEvent, successPayload); err != nil {
			return fail(err)
		}
	}
	return pc.persistAndPublishArtifactRepoSuccess(ctx, execCtx, spec, repoURL, ref, manifest, requestID, sourceEventID, successPayload)
}

func artifactRepoClassify(err error, class runtimefailures.Class, code, operation string) error {
	if err == nil {
		return nil
	}
	if _, ok := runtimefailures.As(err); ok {
		return err
	}
	return runtimefailures.Wrap(class, code, "artifact-repo", operation, nil, err)
}

func (pc *PipelineCoordinator) persistAndPublishArtifactRepoFailure(ctx context.Context, execCtx runtimeengine.ExecutionContext, spec *runtimecontracts.ArtifactRepoSpec, repoID, namespace, partitionKey, displaySlug string, provenance map[string]any, requestID, sourceEventID string, cause error) error {
	failure := runtimefailures.Normalize(cause, "artifact-repo", "commit")
	failureValue, err := runtimefailures.EnvelopeValue(failure)
	if err != nil {
		return errors.Join(cause, fmt.Errorf("artifact_repo_commit failed to encode canonical failure: %w", err))
	}
	fields := map[string]any{
		spec.Output.Status:            "failed",
		spec.Output.Failure:           failureValue,
		spec.Output.LastRequestID:     requestID,
		spec.Output.LastSourceEventID: sourceEventID,
	}
	failureEvent := strings.TrimSpace(spec.FailureEvent)
	if failureEvent == "" {
		if persistErr := pc.persistArtifactRepoResult(ctx, execCtx, spec, fields); persistErr != nil {
			return errors.Join(cause, fmt.Errorf("artifact_repo_commit failed to persist failure state: %w", persistErr))
		}
		return cause
	}
	payload, payloadErr := artifactRepoFailurePayload(execCtx.Base, spec, repoID, namespace, partitionKey, displaySlug, provenance, requestID, sourceEventID, failureValue)
	if payloadErr != nil {
		return errors.Join(cause, payloadErr)
	}
	if validateErr := pc.validateArtifactRepoResultPayload(execCtx, failureEvent, payload); validateErr != nil {
		return errors.Join(cause, validateErr)
	}
	if queued, queueErr := pc.queueArtifactRepoResultEvent(ctx, execCtx, failureEvent, payload); queueErr != nil {
		return errors.Join(cause, queueErr)
	} else if queued {
		if persistErr := pc.persistArtifactRepoResult(ctx, execCtx, spec, fields); persistErr != nil {
			return errors.Join(cause, fmt.Errorf("artifact_repo_commit failed to persist failure state: %w", persistErr))
		}
		return nil
	}
	return errors.Join(cause, errArtifactRepoResultEmitCollectorMissing)
}

func (pc *PipelineCoordinator) persistAndPublishArtifactRepoSuccess(ctx context.Context, execCtx runtimeengine.ExecutionContext, spec *runtimecontracts.ArtifactRepoSpec, repoURL, ref string, manifest map[string]any, requestID, sourceEventID string, successPayload map[string]any) error {
	fields := map[string]any{
		spec.Output.RepoURL:       repoURL,
		spec.Output.CurrentRef:    ref,
		spec.Output.FileManifest:  manifest,
		spec.Output.Status:        "committed",
		spec.Output.LastRequestID: requestID,
		spec.Output.Failure:       nil,
	}
	successEvent := strings.TrimSpace(spec.SuccessEvent)
	if successEvent == "" {
		fields[spec.Output.LastSourceEventID] = sourceEventID
		return pc.persistArtifactRepoResult(ctx, execCtx, spec, fields)
	}
	if queued, err := pc.queueArtifactRepoResultEvent(ctx, execCtx, successEvent, successPayload); err != nil {
		return err
	} else if queued {
		fields[spec.Output.LastSourceEventID] = sourceEventID
		return pc.persistArtifactRepoResult(ctx, execCtx, spec, fields)
	}
	return errArtifactRepoResultEmitCollectorMissing
}

func (pc *PipelineCoordinator) queueArtifactRepoResultEvent(ctx context.Context, execCtx runtimeengine.ExecutionContext, eventType string, payload map[string]any) (bool, error) {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return false, nil
	}
	if payload == nil {
		payload = map[string]any{}
	}
	chainDepth := execCtx.Request.ChainDepth + 1
	if chainDepth <= 0 {
		chainDepth = execCtx.Request.Event.ChainDepth() + 1
	}
	if chainDepth <= 0 {
		chainDepth = 1
	}
	sourceRoute := pc.artifactRepoResultProducerRoute(execCtx)
	eventType = actionResultEventType(pc.SemanticSource(), execCtx.Request.FlowID.String(), eventType, sourceRoute)
	entityID := sourceRoute.EntityID
	flowInstance := sourceRoute.FlowInstance
	envelope := events.EventEnvelope{
		EntityID:     entityID,
		FlowInstance: flowInstance,
	}
	routingSource, err := events.RuntimeRoutingSourceFromRoute(sourceRoute)
	if err != nil {
		return false, fmt.Errorf("artifact result routing source: %w", err)
	}
	if !routingSource.Empty() {
		envelope = events.EnvelopeForSourceRoute(envelope, routingSource.Route())
	}
	evt, err := events.NewChildEvent(events.ChildEventInput{
		Facts: events.EventFacts{
			ID: uuid.NewString(), Type: events.EventType(eventType),
			Producer: events.ProducerClaim{Type: events.EventProducerPlatform, ID: runtimeWorkflowID},
			Payload:  mustJSON(payload), ChainDepth: chainDepth, Envelope: envelope,
			RoutingSource: routingSource, CreatedAt: time.Now().UTC(),
		},
		Lineage: events.LineageFromEvent(execCtx.Request.Event),
	})
	if err != nil {
		return false, fmt.Errorf("construct artifact result event: %w", err)
	}
	return runtimeengine.QueueActionEmitIntent(ctx, runtimeengine.EmitIntent{
		Event:         evt,
		ChainDepth:    chainDepth,
		ParentEventID: evt.ParentEventID(),
	}), nil
}

func (pc *PipelineCoordinator) artifactRepoResultProducerRoute(execCtx runtimeengine.ExecutionContext) events.RouteIdentity {
	return actionResultProducerRoute(
		pc.SemanticSource(),
		execCtx.Request.FlowID.String(),
		execCtx.Request.EntityID.String(),
		execCtx.Request.Event,
		execCtx.Request.State,
		execCtx.Request.ProducerRoute,
	)
}

func (pc *PipelineCoordinator) artifactRepoRoot() (string, error) {
	explicit := ""
	if pc != nil && strings.TrimSpace(pc.artifactRoot) != "" {
		explicit = pc.artifactRoot
	}
	resolution, err := EnsureArtifactRepoRootWritable(explicit)
	if err != nil {
		return "", err
	}
	return resolution.Root, nil
}

type ArtifactRepoRootResolution struct {
	Root   string
	Source string
}

// ResolveArtifactRepoRoot returns the validated runtime-private artifact root.
// The explicit root has precedence over SWARM_ARTIFACT_ROOT, which has
// precedence over the platform default.
func ResolveArtifactRepoRoot(explicit string) (string, error) {
	resolution, err := ResolveArtifactRepoRootWithSource(explicit)
	if err != nil {
		return "", err
	}
	return resolution.Root, nil
}

// ResolveArtifactRepoRootWithSource returns the validated root plus the source
// selected for operator diagnostics.
func ResolveArtifactRepoRootWithSource(explicit string) (ArtifactRepoRootResolution, error) {
	root := strings.TrimSpace(explicit)
	if root != "" {
		validated, err := validateArtifactRepoRoot(root)
		return ArtifactRepoRootResolution{Root: validated, Source: "explicit runtime ArtifactRoot option"}, err
	}
	if env := strings.TrimSpace(os.Getenv("SWARM_ARTIFACT_ROOT")); env != "" {
		validated, err := validateArtifactRepoRoot(env)
		return ArtifactRepoRootResolution{Root: validated, Source: "SWARM_ARTIFACT_ROOT"}, err
	}
	validated, err := validateArtifactRepoRoot(defaultArtifactRoot)
	return ArtifactRepoRootResolution{Root: validated, Source: "platform default /var/lib/swarm/artifacts"}, err
}

// EnsureArtifactRepoRootWritable validates and exercises the runtime-private
// artifact root so platform config defects surface before action execution.
func EnsureArtifactRepoRootWritable(explicit string) (ArtifactRepoRootResolution, error) {
	resolution, err := ResolveArtifactRepoRootWithSource(explicit)
	if err != nil {
		return resolution, err
	}
	if err := validateArtifactRepoRootWritable(resolution.Root); err != nil {
		return resolution, fmt.Errorf("artifact root %q from %s is not writable by the runtime process: %w; set SWARM_ARTIFACT_ROOT=<writable runtime-private absolute path>", resolution.Root, resolution.Source, err)
	}
	return resolution, nil
}

func validateArtifactRepoRootWritable(root string) error {
	cleaned, err := validateArtifactRepoRoot(root)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(cleaned, 0o755); err != nil {
		return err
	}
	cleaned, err = validateArtifactRepoRoot(cleaned)
	if err != nil {
		return err
	}
	if err := validateArtifactRepoWritableDirectory(cleaned, ".swarm-artifact-root-check-*"); err != nil {
		return err
	}
	reposRoot, err := artifactRepoLocalGitStorageBase(cleaned)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(reposRoot, 0o755); err != nil {
		return fmt.Errorf("artifact repository storage base %q: %w", reposRoot, err)
	}
	if resolved, ok := artifactRootResolveExistingPrefix(reposRoot); ok {
		if mount, ok := artifactRootAgentMount(resolved); ok {
			return fmt.Errorf("artifact repository storage base %q resolves under agent-visible mount %s", reposRoot, mount)
		}
	}
	if err := validateArtifactRepoWritableDirectory(reposRoot, ".swarm-artifact-repos-check-*"); err != nil {
		return fmt.Errorf("artifact repository storage base %q: %w", reposRoot, err)
	}
	return nil
}

func validateArtifactRepoWritableDirectory(dir, pattern string) error {
	probe, err := os.CreateTemp(dir, pattern)
	if err != nil {
		return err
	}
	probeName := probe.Name()
	if err := probe.Close(); err != nil {
		_ = os.Remove(probeName)
		return err
	}
	return os.Remove(probeName)
}

func artifactRepoLocalGitStorageBase(root string) (string, error) {
	cleaned, err := validateArtifactRepoRoot(root)
	if err != nil {
		return "", err
	}
	reposRoot := filepath.Join(cleaned, "repos")
	if !artifactPathWithinRoot(reposRoot, cleaned) {
		return "", fmt.Errorf("artifact repository storage base escaped artifact root")
	}
	return reposRoot, nil
}

func SourceUsesArtifactRepoCommit(source semanticview.Source) bool {
	if source == nil {
		return false
	}
	for _, node := range source.NodeEntries() {
		for _, handler := range node.EventHandlers {
			if artifactActionSpecIsCommit(handler.Action) {
				return true
			}
			for _, rule := range handler.Rules {
				if artifactActionSpecIsCommit(rule.Action) {
					return true
				}
			}
		}
	}
	return false
}

func artifactActionSpecIsCommit(action runtimecontracts.ActionSpec) bool {
	return strings.TrimSpace(strings.ToLower(action.ID)) == artifactRepoCommitActionID
}

func validateArtifactRepoRoot(raw string) (string, error) {
	root := strings.TrimSpace(raw)
	if root == "" {
		return "", fmt.Errorf("artifact root is required")
	}
	if strings.Contains(root, "\x00") {
		return "", fmt.Errorf("artifact root contains NUL byte")
	}
	cleaned := filepath.Clean(root)
	if !filepath.IsAbs(cleaned) {
		return "", fmt.Errorf("artifact root %q must be an absolute runtime-private host path", root)
	}
	if mount, ok := artifactRootAgentMount(cleaned); ok {
		return "", fmt.Errorf("artifact root %q cannot live under agent-visible mount %s", cleaned, mount)
	}
	if resolved, ok := artifactRootResolveExistingPrefix(cleaned); ok {
		if mount, ok := artifactRootAgentMount(resolved); ok {
			return "", fmt.Errorf("artifact root %q resolves under agent-visible mount %s", cleaned, mount)
		}
	}
	return cleaned, nil
}

func artifactRootAgentMount(root string) (string, bool) {
	for _, mount := range invalidArtifactRootMounts {
		cleanMount := filepath.Clean(mount)
		if artifactPathWithinRoot(root, cleanMount) {
			return cleanMount, true
		}
	}
	return "", false
}

func artifactRootResolveExistingPrefix(cleaned string) (string, bool) {
	current := cleaned
	remaining := []string{}
	for {
		resolved, err := filepath.EvalSymlinks(current)
		if err == nil {
			for i := len(remaining) - 1; i >= 0; i-- {
				resolved = filepath.Join(resolved, remaining[i])
			}
			return filepath.Clean(resolved), true
		}
		parent := filepath.Dir(current)
		if parent == current {
			return "", false
		}
		remaining = append(remaining, filepath.Base(current))
		current = parent
	}
}

func artifactRepoOutputsComplete(metadata map[string]any, spec *runtimecontracts.ArtifactRepoSpec) bool {
	if metadata == nil || spec == nil {
		return false
	}
	if got := strings.TrimSpace(asString(metadata[spec.Output.Status])); got != "committed" {
		return false
	}
	if strings.TrimSpace(asString(metadata[spec.Output.RepoURL])) == "" {
		return false
	}
	if ref := strings.TrimSpace(asString(metadata[spec.Output.CurrentRef])); len(ref) != 40 {
		return false
	}
	manifest, ok := metadata[spec.Output.FileManifest].(map[string]any)
	if !ok || manifest == nil {
		return false
	}
	if strings.TrimSpace(asString(manifest["tree_hash"])) == "" {
		return false
	}
	return true
}

func artifactNamespace(execCtx runtimeengine.ExecutionContext, spec *runtimecontracts.ArtifactRepoSpec) (string, error) {
	if spec != nil && !spec.Namespace.IsZero() {
		return requiredArtifactSegment(execCtx.Base, spec.Namespace, "artifact_repo.namespace")
	}
	namespace := strings.TrimSpace(execCtx.Request.Event.RunID())
	if namespace == "" {
		if value, ok := execCtx.Base.Event.Lookup(paths.Parse("run_id")); ok {
			namespace = strings.TrimSpace(asString(value))
		}
	}
	if namespace == "" {
		return "", fmt.Errorf("artifact_repo_commit requires artifact_repo.namespace or event run_id")
	}
	if err := validateArtifactRepoSegment(namespace); err != nil {
		return "", fmt.Errorf("artifact_repo.namespace: %w", err)
	}
	return namespace, nil
}

func requiredArtifactUUID(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue, field string) (string, error) {
	value, err := requiredArtifactString(base, expr, field)
	if err != nil {
		return "", err
	}
	if _, err := uuid.Parse(value); err != nil {
		return "", fmt.Errorf("%s must resolve to UUID: %w", field, err)
	}
	return value, nil
}

func requiredArtifactString(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue, field string) (string, error) {
	value, ok, err := evalMailboxExpressionValue(base, expr)
	if err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	if !ok {
		return "", fmt.Errorf("%s resolved empty", field)
	}
	out := strings.TrimSpace(asString(value))
	if out == "" {
		return "", fmt.Errorf("%s resolved empty", field)
	}
	return out, nil
}

func requiredArtifactSegment(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue, field string) (string, error) {
	value, err := requiredArtifactString(base, expr, field)
	if err != nil {
		return "", err
	}
	if err := validateArtifactRepoSegment(value); err != nil {
		return "", fmt.Errorf("%s: %w", field, err)
	}
	return value, nil
}

func optionalArtifactSegment(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue, field string) (string, error) {
	if expr.IsZero() {
		return "", nil
	}
	return requiredArtifactSegment(base, expr, field)
}

func optionalArtifactString(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue) string {
	if expr.IsZero() {
		return ""
	}
	value, ok, err := evalMailboxExpressionValue(base, expr)
	if err != nil || !ok {
		return ""
	}
	return strings.TrimSpace(asString(value))
}

func optionalArtifactDisplaySlug(base runtimeengine.BaseContext, expr runtimecontracts.ExpressionValue) (string, error) {
	if expr.IsZero() {
		return "", nil
	}
	value, ok, err := evalMailboxExpressionValue(base, expr)
	if err != nil {
		return "", fmt.Errorf("artifact_repo.display_slug: %w", err)
	}
	if !ok {
		return "", nil
	}
	raw := strings.TrimSpace(asString(value))
	if err := validateArtifactRepoDisplaySlug(raw); err != nil {
		return "", fmt.Errorf("artifact_repo.display_slug: %w", err)
	}
	return raw, nil
}

func artifactRepoProvenance(base runtimeengine.BaseContext, spec *runtimecontracts.ArtifactRepoSpec) (map[string]any, error) {
	out := map[string]any{}
	if spec == nil {
		return out, nil
	}
	for key, expr := range spec.Provenance {
		key = strings.TrimSpace(key)
		if err := validateArtifactRepoProvenanceKey(key); err != nil {
			return nil, fmt.Errorf("artifact_repo.provenance key %q: %w", key, err)
		}
		value, ok, err := evalMailboxExpressionValue(base, expr)
		if err != nil {
			return nil, fmt.Errorf("artifact_repo.provenance.%s: %w", key, err)
		}
		if !ok {
			return nil, fmt.Errorf("artifact_repo.provenance.%s resolved empty", key)
		}
		out[key] = value
	}
	return out, nil
}

func prepareArtifactRepoFiles(base runtimeengine.BaseContext, spec *runtimecontracts.ArtifactRepoSpec) ([]artifactRepoPreparedFile, string, error) {
	if spec == nil {
		return nil, "", fmt.Errorf("artifact_repo declaration is required")
	}
	allowed := map[string]struct{}{}
	for _, raw := range spec.AllowedPaths {
		cleaned, err := artifactRepoCleanPath(raw)
		if err != nil {
			return nil, "", fmt.Errorf("artifact_repo.allowed_paths %q: %w", raw, err)
		}
		allowed[cleaned] = struct{}{}
	}
	if len(allowed) == 0 {
		return nil, "", fmt.Errorf("artifact_repo.allowed_paths is required")
	}
	if len(spec.Files) == 0 {
		return nil, "", fmt.Errorf("artifact_repo.files is required")
	}
	files := make([]artifactRepoPreparedFile, 0, len(spec.Files))
	seen := map[string]struct{}{}
	total := 0
	for i, file := range spec.Files {
		rawPath, err := requiredArtifactString(base, file.Path, fmt.Sprintf("artifact_repo.files[%d].path", i))
		if err != nil {
			return nil, "", err
		}
		cleaned, err := artifactRepoCleanPath(rawPath)
		if err != nil {
			return nil, "", fmt.Errorf("artifact_repo.files[%d].path: %w", i, err)
		}
		if _, ok := allowed[cleaned]; !ok {
			return nil, "", fmt.Errorf("artifact_repo.files[%d].path %s is not allowlisted", i, cleaned)
		}
		if _, ok := seen[cleaned]; ok {
			return nil, "", fmt.Errorf("artifact_repo.files duplicate canonical path %s", cleaned)
		}
		seen[cleaned] = struct{}{}
		rawContent, err := requiredArtifactString(base, file.Content, fmt.Sprintf("artifact_repo.files[%d].content", i))
		if err != nil {
			return nil, "", err
		}
		contentType := strings.TrimSpace(file.ContentType)
		normalized, err := normalizeArtifactContent(contentType, rawContent, file.Schema)
		if err != nil {
			return nil, "", fmt.Errorf("artifact_repo.files[%d].content: %w", i, err)
		}
		limit := artifactFileLimit(contentType, spec.Limits, file.MaxBytes)
		if limit > 0 && len(normalized) > limit {
			return nil, "", runtimefailures.New(runtimefailures.ClassDataLimitExceeded, "artifact_file_limit_exceeded", "artifact-repo", "validate_file", map[string]any{
				"limit_kind": "file_bytes",
				"actual":     len(normalized),
				"limit":      limit,
				"file_index": i,
			})
		}
		total += len(normalized)
		files = append(files, artifactRepoPreparedFile{
			Path:        cleaned,
			Content:     normalized,
			ContentType: contentType,
			SHA256:      sha256Hex(normalized),
			Size:        len(normalized),
		})
	}
	if maxRepo := artifactRepoMaxBytes(spec.Limits); maxRepo > 0 && total > maxRepo {
		return nil, "", runtimefailures.New(runtimefailures.ClassDataLimitExceeded, "artifact_repo_limit_exceeded", "artifact-repo", "validate_repository", map[string]any{
			"limit_kind": "repository_bytes",
			"actual":     total,
			"limit":      maxRepo,
		})
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, artifactTreeHash(files), nil
}

func artifactRepoMaxBytes(limits runtimecontracts.ArtifactRepoLimitsSpec) int {
	if limits.MaxRepoBytes > 0 {
		return limits.MaxRepoBytes
	}
	return defaultRepoMaxBytes
}

func artifactFileLimit(contentType string, limits runtimecontracts.ArtifactRepoLimitsSpec, fileLimit int) int {
	if fileLimit > 0 {
		return fileLimit
	}
	switch contentType {
	case "yaml":
		if limits.MaxYAMLBytes > 0 {
			return limits.MaxYAMLBytes
		}
		return defaultYAMLMaxBytes
	case "markdown":
		if limits.MaxMarkdownBytes > 0 {
			return limits.MaxMarkdownBytes
		}
		return defaultMarkdownMaxBytes
	default:
		if limits.MaxTextBytes > 0 {
			return limits.MaxTextBytes
		}
		return defaultTextMaxBytes
	}
}

func normalizeArtifactContent(contentType, raw string, schema runtimecontracts.ArtifactRepoSchemaSpec) ([]byte, error) {
	switch strings.TrimSpace(contentType) {
	case "yaml":
		var value map[string]any
		if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
			return nil, err
		}
		if err := validateArtifactYAMLSchema(value, schema); err != nil {
			return nil, err
		}
		out, err := yaml.Marshal(value)
		if err != nil {
			return nil, err
		}
		return ensureTrailingNewline(out), nil
	case "markdown", "text":
		return ensureTrailingNewline([]byte(raw)), nil
	default:
		return nil, fmt.Errorf("unsupported content_type %q", contentType)
	}
}

func validateArtifactYAMLSchema(value map[string]any, schema runtimecontracts.ArtifactRepoSchemaSpec) error {
	if strings.TrimSpace(schema.Type) != "object" {
		return fmt.Errorf("yaml schema.type must be object")
	}
	if value == nil {
		return fmt.Errorf("yaml content must be an object")
	}
	for _, field := range schema.RequiredFields {
		field = strings.TrimSpace(field)
		if field == "" {
			return fmt.Errorf("yaml schema.required_fields contains an empty field")
		}
		if _, ok := value[field]; !ok {
			return fmt.Errorf("yaml content missing required field %s", field)
		}
	}
	return nil
}

func artifactRepoCleanPath(raw string) (string, error) {
	value := strings.TrimSpace(strings.ReplaceAll(raw, "\\", "/"))
	if value == "" {
		return "", fmt.Errorf("path is required")
	}
	if strings.HasPrefix(value, "/") {
		return "", fmt.Errorf("absolute paths are not allowed")
	}
	cleaned := path.Clean(value)
	if cleaned == "." || cleaned == "" {
		return "", fmt.Errorf("path is required")
	}
	if cleaned == ".." || strings.HasPrefix(cleaned, "../") || strings.Contains(cleaned, "/../") {
		return "", fmt.Errorf("path traversal is not allowed")
	}
	return cleaned, nil
}

func validateArtifactRepoSegment(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("value is required")
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("only letters, digits, dash, underscore, and dot are allowed")
	}
	if value == "." || value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("path traversal markers are not allowed")
	}
	return nil
}

func validateArtifactRepoProvenanceKey(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return fmt.Errorf("key is required")
	}
	for _, r := range value {
		if r >= 'a' && r <= 'z' || r >= 'A' && r <= 'Z' || r >= '0' && r <= '9' || r == '-' || r == '_' || r == '.' {
			continue
		}
		return fmt.Errorf("only letters, digits, dash, underscore, and dot are allowed")
	}
	return nil
}

func validateArtifactRepoDisplaySlug(raw string) error {
	value := strings.TrimSpace(raw)
	if value == "" {
		return nil
	}
	if strings.Contains(value, "\x00") || strings.ContainsAny(value, `/\`) {
		return fmt.Errorf("path separators are not allowed")
	}
	if value == "." || value == ".." || strings.Contains(value, "..") {
		return fmt.Errorf("path traversal markers are not allowed")
	}
	if sanitizeArtifactSlug(value) == "" {
		return fmt.Errorf("must contain at least one letter or digit")
	}
	return nil
}

func artifactRepoPath(root, namespace, repoID string) (string, error) {
	var err error
	root, err = validateArtifactRepoRoot(root)
	if err != nil {
		return "", err
	}
	if _, err := uuid.Parse(repoID); err != nil {
		return "", fmt.Errorf("repo_id must be UUID: %w", err)
	}
	if err := validateArtifactRepoSegment(namespace); err != nil {
		return "", fmt.Errorf("namespace: %w", err)
	}
	parts := []string{root, "repos", namespace}
	parts = append(parts, repoID+".git")
	repoPath := filepath.Join(parts...)
	if !artifactPathWithinRoot(repoPath, root) {
		return "", fmt.Errorf("artifact_repo path escaped artifact root")
	}
	return repoPath, nil
}

func sanitizeArtifactSlug(raw string) string {
	raw = strings.ToLower(strings.TrimSpace(raw))
	var b strings.Builder
	lastDash := false
	for _, r := range raw {
		if r >= 'a' && r <= 'z' || r >= '0' && r <= '9' {
			b.WriteRune(r)
			lastDash = false
			continue
		}
		if !lastDash {
			b.WriteByte('-')
			lastDash = true
		}
	}
	return strings.Trim(b.String(), "-")
}

func ensureArtifactRepoInitialized(ctx context.Context, repoPath string, when time.Time) error {
	if err := os.MkdirAll(repoPath, 0o755); err != nil {
		return err
	}
	if _, err := os.Stat(filepath.Join(repoPath, ".git")); err == nil {
		return nil
	}
	if _, err := runArtifactGit(ctx, repoPath, nil, "init"); err != nil {
		return err
	}
	if _, err := runArtifactGit(ctx, repoPath, nil, "config", "user.name", "Swarm Artifact Repo"); err != nil {
		return err
	}
	if _, err := runArtifactGit(ctx, repoPath, nil, "config", "user.email", "swarm-artifacts@localhost"); err != nil {
		return err
	}
	if _, err := runArtifactGit(ctx, repoPath, gitCommitEnv(when), "commit", "--allow-empty", "-m", "chore: initialize artifact repo", "--no-gpg-sign"); err != nil {
		return err
	}
	return nil
}

func writeArtifactRepoFiles(repoPath string, files []artifactRepoPreparedFile) error {
	for _, file := range files {
		target := filepath.Join(repoPath, filepath.FromSlash(file.Path))
		if !artifactPathWithinRoot(target, repoPath) {
			return fmt.Errorf("artifact_repo path %s escaped repo root", file.Path)
		}
		if err := os.MkdirAll(filepath.Dir(target), 0o755); err != nil {
			return err
		}
		if !artifactRealPathWithinRoot(filepath.Dir(target), repoPath) {
			return fmt.Errorf("artifact_repo path %s escaped repo root through symlink", file.Path)
		}
		if info, err := os.Lstat(target); err == nil && info.Mode()&os.ModeSymlink != 0 {
			return fmt.Errorf("artifact_repo path %s targets a symlink", file.Path)
		}
		if err := os.WriteFile(target, file.Content, 0o644); err != nil {
			return err
		}
	}
	return nil
}

func commitArtifactRepoFiles(ctx context.Context, repoPath string, files []artifactRepoPreparedFile, sourceEventID, requestID, treeHash, author string, when time.Time) (string, error) {
	if _, err := runArtifactGit(ctx, repoPath, nil, "reset", "--"); err != nil {
		return "", err
	}
	args := []string{"add", "--"}
	for _, file := range files {
		args = append(args, file.Path)
	}
	if _, err := runArtifactGit(ctx, repoPath, nil, args...); err != nil {
		return "", err
	}
	hasStagedDiff := true
	if _, err := runArtifactGit(ctx, repoPath, nil, "diff", "--cached", "--quiet"); err == nil {
		hasStagedDiff = false
	}
	if err := verifyArtifactRepoStagedPaths(ctx, repoPath, files); err != nil {
		return "", err
	}
	msg := fmt.Sprintf("artifact: commit request %s\n\nSwarm-Request-Id: %s\nSwarm-Source-Event-Id: %s\nSwarm-Tree-Hash: %s", requestID, requestID, sourceEventID, treeHash)
	env := gitCommitEnv(when)
	if author = strings.TrimSpace(author); author != "" {
		env = append(env, "GIT_AUTHOR_NAME="+author, "GIT_AUTHOR_EMAIL=artifact-author@localhost")
	}
	commitArgs := []string{"commit", "-m", msg, "--no-gpg-sign"}
	if !hasStagedDiff {
		commitArgs = append(commitArgs, "--allow-empty")
	}
	if _, err := runArtifactGit(ctx, repoPath, env, commitArgs...); err != nil {
		return "", err
	}
	return artifactRepoHead(ctx, repoPath)
}

func verifyArtifactRepoStagedPaths(ctx context.Context, repoPath string, files []artifactRepoPreparedFile) error {
	out, err := runArtifactGit(ctx, repoPath, nil, "diff", "--cached", "--name-only")
	if err != nil {
		return err
	}
	allowed := map[string]struct{}{}
	for _, file := range files {
		allowed[file.Path] = struct{}{}
	}
	for _, raw := range strings.Split(out, "\n") {
		staged := strings.TrimSpace(raw)
		if staged == "" {
			continue
		}
		if _, ok := allowed[staged]; !ok {
			return fmt.Errorf("artifact_repo staged non-allowlisted path %s", staged)
		}
	}
	return nil
}

func artifactRepoHead(ctx context.Context, repoPath string) (string, error) {
	out, err := runArtifactGit(ctx, repoPath, nil, "rev-parse", "HEAD")
	if err != nil {
		return "", err
	}
	ref := strings.TrimSpace(out)
	if len(ref) != 40 {
		return "", fmt.Errorf("artifact_repo commit ref %q is not a 40-character git SHA", ref)
	}
	return ref, nil
}

func runArtifactGit(ctx context.Context, dir string, extraEnv []string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, "git", args...)
	cmd.Dir = dir
	cmd.Env = append(os.Environ(), extraEnv...)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("git %s: %w: %s", strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func gitCommitEnv(when time.Time) []string {
	when = commitTime(when)
	stamp := when.UTC().Format(time.RFC3339)
	return []string{
		"GIT_AUTHOR_NAME=Swarm Artifact Repo",
		"GIT_AUTHOR_EMAIL=swarm-artifacts@localhost",
		"GIT_COMMITTER_NAME=Swarm Artifact Repo",
		"GIT_COMMITTER_EMAIL=swarm-artifacts@localhost",
		"GIT_AUTHOR_DATE=" + stamp,
		"GIT_COMMITTER_DATE=" + stamp,
	}
}

func commitTime(when time.Time) time.Time {
	if when.IsZero() {
		return time.Unix(0, 0).UTC()
	}
	return when.UTC()
}

func (pc *PipelineCoordinator) persistArtifactRepoResult(ctx context.Context, execCtx runtimeengine.ExecutionContext, spec *runtimecontracts.ArtifactRepoSpec, fields map[string]any) error {
	if spec == nil {
		return fmt.Errorf("artifact_repo output declaration is required")
	}
	metadata := cloneStringAnyMap(execCtx.Request.State.StateCarrier.Metadata)
	if metadata == nil {
		metadata = map[string]any{}
	}
	for field, value := range fields {
		field = strings.TrimSpace(field)
		if field == "" {
			continue
		}
		metadata[field] = value
	}
	mutation := runtimeengine.StateMutation{
		StateCarrier: runtimeengine.NewStateCarrier(metadata, execCtx.Request.State.StateCarrier.Gates, execCtx.Request.State.StateCarrier.StateBuckets),
	}
	repo := pipelineEngineStateRepo{coordinator: pc}
	return repo.SaveState(ctx, identity.NormalizeEntityID(execCtx.Request.EntityID.String()), mutation)
}

type artifactRepoHistoryRecord struct {
	Ref      string
	TreeHash string
}

func artifactRepoProjectedTreeSize(ctx context.Context, repoPath string, files []artifactRepoPreparedFile) (int, error) {
	sizes := map[string]int{}
	out, err := runArtifactGit(ctx, repoPath, nil, "ls-tree", "-r", "-l", "HEAD")
	if err != nil {
		return 0, err
	}
	for _, raw := range strings.Split(out, "\n") {
		raw = strings.TrimSpace(raw)
		if raw == "" {
			continue
		}
		parts := strings.Fields(raw)
		if len(parts) < 5 {
			return 0, fmt.Errorf("artifact_repo cannot parse git tree entry %q", raw)
		}
		sizeValue := parts[3]
		if sizeValue == "-" {
			continue
		}
		var size int
		if _, err := fmt.Sscanf(sizeValue, "%d", &size); err != nil {
			return 0, fmt.Errorf("artifact_repo cannot parse git tree size %q: %w", sizeValue, err)
		}
		pathStart := strings.Index(raw, "\t")
		if pathStart < 0 || pathStart+1 >= len(raw) {
			return 0, fmt.Errorf("artifact_repo cannot parse git tree path %q", raw)
		}
		sizes[strings.TrimSpace(raw[pathStart+1:])] = size
	}
	for _, file := range files {
		sizes[file.Path] = file.Size
	}
	total := 0
	for _, size := range sizes {
		total += size
	}
	return total, nil
}

func artifactRepoRequestRecord(ctx context.Context, repoPath, requestID string) (artifactRepoHistoryRecord, bool, error) {
	requestID = strings.TrimSpace(requestID)
	if requestID == "" {
		return artifactRepoHistoryRecord{}, false, nil
	}
	out, err := runArtifactGit(ctx, repoPath, nil, "log", "--format=%H%x1f%B%x00")
	if err != nil {
		return artifactRepoHistoryRecord{}, false, err
	}
	for _, record := range strings.Split(out, "\x00") {
		record = strings.Trim(record, "\n")
		if strings.TrimSpace(record) == "" {
			continue
		}
		ref, message, ok := strings.Cut(record, "\x1f")
		if !ok {
			return artifactRepoHistoryRecord{}, false, fmt.Errorf("artifact_repo cannot parse git history record")
		}
		ref = strings.TrimSpace(ref)
		if len(ref) != 40 {
			return artifactRepoHistoryRecord{}, false, fmt.Errorf("artifact_repo history ref %q is not a 40-character git SHA", ref)
		}
		foundRequest := ""
		foundTree := ""
		for _, line := range strings.Split(message, "\n") {
			key, value, ok := strings.Cut(line, ":")
			if !ok {
				continue
			}
			switch strings.ToLower(strings.TrimSpace(key)) {
			case "swarm-request-id":
				foundRequest = strings.TrimSpace(value)
			case "swarm-tree-hash":
				foundTree = strings.TrimSpace(value)
			}
		}
		if foundRequest != requestID {
			continue
		}
		if foundTree == "" {
			return artifactRepoHistoryRecord{}, true, fmt.Errorf("artifact_repo_commit request_id %s has no recorded tree hash", requestID)
		}
		return artifactRepoHistoryRecord{Ref: ref, TreeHash: foundTree}, true, nil
	}
	return artifactRepoHistoryRecord{}, false, nil
}

func artifactRepoManifest(repoID, namespace, partitionKey, displaySlug string, provenance map[string]any, requestID, sourceEventID, repoURL, ref, treeHash string, files []artifactRepoPreparedFile) map[string]any {
	fileItems := make([]any, 0, len(files))
	for _, file := range files {
		fileItems = append(fileItems, map[string]any{
			"path":         file.Path,
			"content_type": file.ContentType,
			"sha256":       file.SHA256,
			"size_bytes":   file.Size,
		})
	}
	if provenance == nil {
		provenance = map[string]any{}
	}
	out := map[string]any{
		"provider":        artifactRepoProviderLocalGit,
		"repo_id":         repoID,
		"namespace":       namespace,
		"request_id":      requestID,
		"source_event_id": sourceEventID,
		"repo_url":        repoURL,
		"ref":             ref,
		"tree_hash":       treeHash,
		"files":           fileItems,
		"provenance":      provenance,
	}
	if strings.TrimSpace(partitionKey) != "" {
		out["partition_key"] = partitionKey
	}
	if strings.TrimSpace(displaySlug) != "" {
		out["display_slug"] = displaySlug
	}
	return out
}

var artifactRepoResultReservedPayloadFields = map[string]struct{}{
	"repo_id":         {},
	"namespace":       {},
	"partition_key":   {},
	"display_slug":    {},
	"request_id":      {},
	"source_event_id": {},
	"repo_url":        {},
	"current_ref":     {},
	"file_manifest":   {},
	"failure":         {},
	"provenance":      {},
}

func artifactRepoSuccessPayload(base runtimeengine.BaseContext, spec *runtimecontracts.ArtifactRepoSpec, repoID, namespace, partitionKey, displaySlug string, provenance map[string]any, requestID, sourceEventID, repoURL, currentRef string, manifest map[string]any) (map[string]any, error) {
	out, err := artifactRepoDeclaredResultPayload(base, "success_payload", specSuccessPayload(spec))
	if err != nil {
		return nil, err
	}
	out["repo_id"] = repoID
	out["namespace"] = namespace
	if strings.TrimSpace(partitionKey) != "" {
		out["partition_key"] = partitionKey
	}
	if strings.TrimSpace(displaySlug) != "" {
		out["display_slug"] = displaySlug
	}
	if provenance == nil {
		provenance = map[string]any{}
	}
	out["provenance"] = provenance
	out["request_id"] = requestID
	out["source_event_id"] = sourceEventID
	out["repo_url"] = repoURL
	out["current_ref"] = currentRef
	out["file_manifest"] = manifest
	return out, nil
}

func artifactRepoFailurePayload(base runtimeengine.BaseContext, spec *runtimecontracts.ArtifactRepoSpec, repoID, namespace, partitionKey, displaySlug string, provenance map[string]any, requestID, sourceEventID string, failure map[string]any) (map[string]any, error) {
	out, err := artifactRepoDeclaredResultPayload(base, "failure_payload", specFailurePayload(spec))
	if err != nil {
		return nil, err
	}
	out["repo_id"] = repoID
	out["namespace"] = namespace
	if strings.TrimSpace(partitionKey) != "" {
		out["partition_key"] = partitionKey
	}
	if strings.TrimSpace(displaySlug) != "" {
		out["display_slug"] = displaySlug
	}
	if provenance == nil {
		provenance = map[string]any{}
	}
	out["provenance"] = provenance
	out["request_id"] = requestID
	out["source_event_id"] = sourceEventID
	out["failure"] = failure
	return out, nil
}

func specSuccessPayload(spec *runtimecontracts.ArtifactRepoSpec) map[string]runtimecontracts.ExpressionValue {
	if spec == nil {
		return nil
	}
	return spec.SuccessPayload
}

func specFailurePayload(spec *runtimecontracts.ArtifactRepoSpec) map[string]runtimecontracts.ExpressionValue {
	if spec == nil {
		return nil
	}
	return spec.FailurePayload
}

func artifactRepoDeclaredResultPayload(base runtimeengine.BaseContext, label string, declared map[string]runtimecontracts.ExpressionValue) (map[string]any, error) {
	out := map[string]any{}
	for target, expr := range declared {
		target = strings.TrimSpace(target)
		if target == "" {
			return nil, fmt.Errorf("artifact_repo_commit %s contains an empty target field", label)
		}
		if _, reserved := artifactRepoResultReservedPayloadFields[target]; reserved {
			return nil, fmt.Errorf("artifact_repo_commit %s must not override runtime-owned field %s", label, target)
		}
		value, ok, err := evalMailboxExpressionValue(base, expr)
		if err != nil {
			return nil, fmt.Errorf("artifact_repo_commit %s.%s evaluation failed: %w", label, target, err)
		}
		if !ok {
			return nil, fmt.Errorf("artifact_repo_commit %s.%s did not resolve", label, target)
		}
		out[target] = value
	}
	return out, nil
}

func (pc *PipelineCoordinator) validateArtifactRepoResultPayload(execCtx runtimeengine.ExecutionContext, eventType string, payload map[string]any) error {
	eventType = strings.TrimSpace(eventType)
	if eventType == "" {
		return nil
	}
	if err := validatePipelineEmitPayload(pc.SemanticSource(), execCtx.Request.FlowID.String(), eventType, payload, nil, runtimeengine.EmitSurfaceAction); err != nil {
		return runtimefailures.Wrap(runtimefailures.ClassSchemaInvalid, "artifact_repo_result_schema_invalid", "artifact-repo", "validate_result_event", map[string]any{"event_type": eventType}, err)
	}
	return nil
}

func artifactTreeHash(files []artifactRepoPreparedFile) string {
	h := sha256.New()
	for _, file := range files {
		h.Write([]byte(file.Path))
		h.Write([]byte{0})
		h.Write([]byte(file.SHA256))
		h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sha256Hex(data []byte) string {
	sum := sha256.Sum256(data)
	return hex.EncodeToString(sum[:])
}

func ensureTrailingNewline(data []byte) []byte {
	if len(data) == 0 || data[len(data)-1] != '\n' {
		return append(data, '\n')
	}
	return data
}

func artifactPathWithinRoot(value, root string) bool {
	value = filepath.Clean(strings.TrimSpace(value))
	root = filepath.Clean(strings.TrimSpace(root))
	rel, err := filepath.Rel(root, value)
	if err != nil {
		return false
	}
	return rel == "." || (rel != ".." && !strings.HasPrefix(rel, ".."+string(filepath.Separator)))
}

func artifactRealPathWithinRoot(value, root string) bool {
	realRoot, err := filepath.EvalSymlinks(root)
	if err != nil {
		return false
	}
	realValue, err := filepath.EvalSymlinks(value)
	if err != nil {
		return false
	}
	return artifactPathWithinRoot(realValue, realRoot)
}
