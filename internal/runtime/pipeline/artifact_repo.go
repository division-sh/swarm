package pipeline

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
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

	runtimecontracts "swarm/internal/runtime/contracts"
	"swarm/internal/runtime/core/identity"
	"swarm/internal/runtime/core/paths"
	runtimeengine "swarm/internal/runtime/engine"
)

const (
	artifactRepoProviderLocalGit = "local_git"
	artifactRepoPublicScheme     = "swarm-artifact://spec-repos/"
	defaultArtifactRoot          = "/data/swarm/artifacts"
	defaultYAMLMaxBytes          = 1 << 20
	defaultMarkdownMaxBytes      = 5 << 20
	defaultTextMaxBytes          = 1 << 20
	defaultRepoMaxBytes          = 50 << 20
)

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
	sourceEventID := strings.TrimSpace(execCtx.Request.Event.ID)
	if _, err := uuid.Parse(sourceEventID); err != nil {
		return fmt.Errorf("artifact_repo_commit requires UUID source event id: %w", err)
	}
	repoID, err := requiredArtifactUUID(execCtx.Base, spec.RepoID, "artifact_repo.repo_id")
	if err != nil {
		return err
	}
	runID, err := artifactRunID(execCtx, spec)
	if err != nil {
		return err
	}
	verticalID, err := requiredArtifactUUID(execCtx.Base, spec.VerticalID, "artifact_repo.vertical_id")
	if err != nil {
		return err
	}
	sourceValidationCaseID, err := requiredArtifactUUID(execCtx.Base, spec.SourceValidationCaseID, "artifact_repo.source_validation_case_id")
	if err != nil {
		return err
	}
	requestID, err := requiredArtifactUUID(execCtx.Base, spec.RequestID, "artifact_repo.request_id")
	if err != nil {
		return err
	}
	fail := func(err error) error {
		if err == nil {
			return nil
		}
		_ = pc.persistArtifactRepoResult(ctx, execCtx, spec, map[string]any{
			spec.Output.Status:            "failed",
			spec.Output.FailureReason:     err.Error(),
			spec.Output.LastRequestID:     requestID,
			spec.Output.LastSourceEventID: sourceEventID,
		})
		if failureEvent := strings.TrimSpace(spec.FailureEvent); failureEvent != "" {
			_ = pc.publish(ctx, failureEvent, execCtx.Request.EntityID.String(), artifactRepoFailurePayload(execCtx.Base, spec, repoID, runID, verticalID, sourceValidationCaseID, requestID, sourceEventID, err))
		}
		return err
	}
	if previous := strings.TrimSpace(asString(execCtx.Request.State.StateCarrier.Metadata[spec.Output.LastSourceEventID])); previous == sourceEventID {
		return nil
	}
	files, treeHash, err := prepareArtifactRepoFiles(execCtx.Base, spec)
	if err != nil {
		return fail(err)
	}
	if previousRequest := strings.TrimSpace(asString(execCtx.Request.State.StateCarrier.Metadata[spec.Output.LastRequestID])); previousRequest == requestID {
		if currentManifest, ok := execCtx.Request.State.StateCarrier.Metadata[spec.Output.FileManifest].(map[string]any); ok {
			if previousTree := strings.TrimSpace(asString(currentManifest["tree_hash"])); previousTree != "" && previousTree != treeHash {
				return fail(fmt.Errorf("artifact_repo_commit request_id %s conflicts with previously committed tree %s", requestID, previousTree))
			}
		}
	}
	businessSlug := optionalArtifactString(execCtx.Base, spec.BusinessSlug)
	repoPath, err := artifactRepoPath(pc.artifactRepoRoot(), runID, verticalID, businessSlug, repoID)
	if err != nil {
		return fail(err)
	}
	if err := ensureArtifactRepoInitialized(ctx, repoPath, commitTime(execCtx.Request.Event.CreatedAt)); err != nil {
		return fail(err)
	}
	if err := writeArtifactRepoFiles(repoPath, files); err != nil {
		return fail(err)
	}
	ref, err := commitArtifactRepoFiles(ctx, repoPath, files, sourceEventID, requestID, optionalArtifactString(execCtx.Base, spec.Author), commitTime(execCtx.Request.Event.CreatedAt))
	if err != nil {
		return fail(err)
	}
	repoURL := artifactRepoPublicScheme + repoID
	manifest := artifactRepoManifest(repoID, runID, verticalID, sourceValidationCaseID, requestID, sourceEventID, repoURL, ref, treeHash, files)
	return pc.persistArtifactRepoResult(ctx, execCtx, spec, map[string]any{
		spec.Output.RepoURL:           repoURL,
		spec.Output.CurrentRef:        ref,
		spec.Output.FileManifest:      manifest,
		spec.Output.Status:            "committed",
		spec.Output.LastRequestID:     requestID,
		spec.Output.LastSourceEventID: sourceEventID,
		spec.Output.FailureReason:     "",
	})
}

func (pc *PipelineCoordinator) artifactRepoRoot() string {
	if pc != nil && strings.TrimSpace(pc.artifactRoot) != "" {
		return strings.TrimSpace(pc.artifactRoot)
	}
	if env := strings.TrimSpace(os.Getenv("SWARM_ARTIFACT_ROOT")); env != "" {
		return env
	}
	return defaultArtifactRoot
}

func artifactRunID(execCtx runtimeengine.ExecutionContext, spec *runtimecontracts.ArtifactRepoSpec) (string, error) {
	if spec != nil && !spec.RunID.IsZero() {
		return requiredArtifactUUID(execCtx.Base, spec.RunID, "artifact_repo.run_id")
	}
	runID := strings.TrimSpace(execCtx.Request.Event.RunID)
	if runID == "" {
		if value, ok := execCtx.Base.Event.Lookup(paths.Parse("run_id")); ok {
			runID = strings.TrimSpace(asString(value))
		}
	}
	if _, err := uuid.Parse(runID); err != nil {
		return "", fmt.Errorf("artifact_repo_commit requires UUID run_id: %w", err)
	}
	return runID, nil
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
		normalized, err := normalizeArtifactContent(contentType, rawContent)
		if err != nil {
			return nil, "", fmt.Errorf("artifact_repo.files[%d].content: %w", i, err)
		}
		limit := artifactFileLimit(contentType, spec.Limits, file.MaxBytes)
		if limit > 0 && len(normalized) > limit {
			return nil, "", fmt.Errorf("artifact_repo.files[%d].content exceeds %d bytes", i, limit)
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
	maxRepo := spec.Limits.MaxRepoBytes
	if maxRepo == 0 {
		maxRepo = defaultRepoMaxBytes
	}
	if maxRepo > 0 && total > maxRepo {
		return nil, "", fmt.Errorf("artifact_repo files exceed max repo bytes %d", maxRepo)
	}
	sort.Slice(files, func(i, j int) bool { return files[i].Path < files[j].Path })
	return files, artifactTreeHash(files), nil
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

func normalizeArtifactContent(contentType, raw string) ([]byte, error) {
	switch strings.TrimSpace(contentType) {
	case "yaml":
		var value any
		if err := yaml.Unmarshal([]byte(raw), &value); err != nil {
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

func artifactRepoPath(root, runID, verticalID, businessSlug, repoID string) (string, error) {
	root = strings.TrimSpace(root)
	if root == "" {
		return "", fmt.Errorf("artifact root is required")
	}
	for label, value := range map[string]string{"run_id": runID, "vertical_id": verticalID, "repo_id": repoID} {
		if _, err := uuid.Parse(value); err != nil {
			return "", fmt.Errorf("%s must be UUID: %w", label, err)
		}
	}
	slug := sanitizeArtifactSlug(businessSlug)
	if slug == "" {
		slug = "artifact"
	}
	shortVertical := strings.ReplaceAll(verticalID, "-", "")
	if len(shortVertical) > 8 {
		shortVertical = shortVertical[:8]
	}
	repoPath := filepath.Join(root, "spec-repos", runID, slug+"-"+shortVertical, repoID+".git")
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

func commitArtifactRepoFiles(ctx context.Context, repoPath string, files []artifactRepoPreparedFile, sourceEventID, requestID, author string, when time.Time) (string, error) {
	if _, err := runArtifactGit(ctx, repoPath, nil, "add", "--all"); err != nil {
		return "", err
	}
	if _, err := runArtifactGit(ctx, repoPath, nil, "diff", "--cached", "--quiet"); err == nil {
		return artifactRepoHead(ctx, repoPath)
	}
	msg := fmt.Sprintf("artifact: commit request %s", requestID)
	env := gitCommitEnv(when)
	if author = strings.TrimSpace(author); author != "" {
		env = append(env, "GIT_AUTHOR_NAME="+author, "GIT_AUTHOR_EMAIL=artifact-author@localhost")
	}
	if _, err := runArtifactGit(ctx, repoPath, env, "commit", "-m", msg, "--no-gpg-sign"); err != nil {
		return "", err
	}
	return artifactRepoHead(ctx, repoPath)
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

func artifactRepoManifest(repoID, runID, verticalID, sourceValidationCaseID, requestID, sourceEventID, repoURL, ref, treeHash string, files []artifactRepoPreparedFile) map[string]any {
	fileItems := make([]map[string]any, 0, len(files))
	for _, file := range files {
		fileItems = append(fileItems, map[string]any{
			"path":         file.Path,
			"content_type": file.ContentType,
			"sha256":       file.SHA256,
			"size_bytes":   file.Size,
		})
	}
	return map[string]any{
		"provider":                  artifactRepoProviderLocalGit,
		"repo_id":                   repoID,
		"run_id":                    runID,
		"vertical_id":               verticalID,
		"source_validation_case_id": sourceValidationCaseID,
		"request_id":                requestID,
		"source_event_id":           sourceEventID,
		"repo_url":                  repoURL,
		"ref":                       ref,
		"tree_hash":                 treeHash,
		"files":                     fileItems,
	}
}

func artifactRepoFailurePayload(base runtimeengine.BaseContext, spec *runtimecontracts.ArtifactRepoSpec, repoID, runID, verticalID, sourceValidationCaseID, requestID, sourceEventID string, cause error) map[string]any {
	out := map[string]any{}
	if spec != nil {
		for target, expr := range spec.FailurePayload {
			target = strings.TrimSpace(target)
			if target == "" {
				continue
			}
			value, ok, err := evalMailboxExpressionValue(base, expr)
			if err != nil || !ok {
				continue
			}
			out[target] = value
		}
	}
	out["repo_id"] = repoID
	out["run_id"] = runID
	out["vertical_id"] = verticalID
	out["source_validation_case_id"] = sourceValidationCaseID
	out["request_id"] = requestID
	out["source_event_id"] = sourceEventID
	out["failure_reason"] = cause.Error()
	return out
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
