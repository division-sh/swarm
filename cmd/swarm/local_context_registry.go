package main

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

const (
	localContextRegistryOwner = "platform-spec.yaml#cli_specification.foundations.local_context_registry_authority"

	localContextDescriptorVersion = 1

	localContextTransportTCP  = "tcp"
	localContextTransportUnix = "unix"

	localContextAuthBuiltinLoopback = "builtin_loopback"
	localContextAuthTokenFile       = "token_file"

	localContextStatusOK                   = "ok"
	localContextStatusNoServer             = "no_server"
	localContextStatusStaleDescriptor      = "stale_descriptor"
	localContextStatusIdentityMismatch     = "identity_mismatch"
	localContextStatusUnsupportedTransport = "unsupported_transport"
	localContextStatusAuthFailure          = "auth_failure"
	localContextStatusPermissionDenied     = "permission_denied"
	localContextStatusCorruptDescriptor    = "corrupt_descriptor"
	localContextStatusInvalidDescriptor    = "invalid_descriptor"
)

type localContextDescriptor struct {
	Version           int                        `json:"version"`
	Name              string                     `json:"name"`
	RuntimeInstanceID string                     `json:"runtime_instance_id"`
	Transport         string                     `json:"transport"`
	APIServer         string                     `json:"api_server,omitempty"`
	SocketPath        string                     `json:"socket_path,omitempty"`
	Auth              localContextDescriptorAuth `json:"auth"`
	ProjectRoot       string                     `json:"project_root,omitempty"`
	ContractsPath     string                     `json:"contracts_path,omitempty"`
	StorePath         string                     `json:"store_path,omitempty"`
	DataDir           string                     `json:"data_dir,omitempty"`
	PID               int                        `json:"pid,omitempty"`
	CreatedAt         string                     `json:"created_at"`
	UpdatedAt         string                     `json:"updated_at"`
}

type localContextDescriptorAuth struct {
	Mode      string `json:"mode"`
	TokenFile string `json:"token_file,omitempty"`
}

type localContextRegistry struct {
	swarmDir string
}

type localContextEntry struct {
	Descriptor localContextDescriptor `json:"descriptor"`
	Path       string                 `json:"path"`
	Status     string                 `json:"status"`
	Detail     string                 `json:"detail,omitempty"`
}

type localContextRegistryReport struct {
	Owner   string              `json:"owner"`
	Status  string              `json:"status"`
	Detail  string              `json:"detail,omitempty"`
	Current *localContextEntry  `json:"current,omitempty"`
	Entries []localContextEntry `json:"entries"`
}

type localContextPruneResult struct {
	Removed []localContextEntry `json:"removed"`
	Kept    []localContextEntry `json:"kept"`
}

type runtimeIdentityCaller interface {
	callRuntimeIdentity(ctx context.Context, rpcEndpoint, token string) (apiv1.RuntimeIdentityResult, error)
}

type cliRuntimeIdentityCaller struct {
	httpClient *http.Client
}

func (c cliRuntimeIdentityCaller) callRuntimeIdentity(ctx context.Context, rpcEndpoint, token string) (apiv1.RuntimeIdentityResult, error) {
	client := c.httpClient
	if client == nil {
		client = &http.Client{Timeout: 2 * time.Second}
	}
	apiClient := cliAPIClient{endpoint: rpcEndpoint, token: token, httpClient: client}
	var result apiv1.RuntimeIdentityResult
	if err := apiClient.call(ctx, "runtime.identity", map[string]any{}, &result); err != nil {
		return apiv1.RuntimeIdentityResult{}, err
	}
	return result, nil
}

func newLocalContextRegistry(swarmDir string) localContextRegistry {
	return localContextRegistry{swarmDir: filepath.Clean(swarmDir)}
}

func (r localContextRegistry) dir() string {
	return filepath.Join(r.swarmDir, "contexts")
}

func (r localContextRegistry) lockPath() string {
	return filepath.Join(r.dir(), ".lock")
}

func (r localContextRegistry) currentPath() string {
	return filepath.Join(r.dir(), "current")
}

func (r localContextRegistry) descriptorPath(name string) (string, error) {
	name, err := normalizeLocalContextName(name)
	if err != nil {
		return "", err
	}
	return filepath.Join(r.dir(), name+".json"), nil
}

func normalizeLocalContextName(raw string) (string, error) {
	name := strings.TrimSpace(raw)
	if name == "" {
		return "", fmt.Errorf("context name must be non-empty")
	}
	if name == "." || name == ".." || strings.HasPrefix(name, ".") {
		return "", fmt.Errorf("context name %q is reserved", name)
	}
	if strings.ContainsAny(name, `/\`+"\x00") || filepath.Base(name) != name {
		return "", fmt.Errorf("context name %q must not contain path separators or NUL", name)
	}
	return name, nil
}

func (r localContextRegistry) WriteDescriptor(desc localContextDescriptor) error {
	if err := validateLocalContextDescriptorShape(desc); err != nil {
		return err
	}
	if err := os.MkdirAll(r.dir(), 0o700); err != nil {
		return fmt.Errorf("create context registry: %w", err)
	}
	unlock, err := acquireLocalContextRegistryLock(r.lockPath())
	if err != nil {
		return err
	}
	defer unlock()
	path, err := r.descriptorPath(desc.Name)
	if err != nil {
		return err
	}
	return atomicWriteJSON(path, desc, 0o600)
}

func (r localContextRegistry) SetCurrent(name string) error {
	name, err := normalizeLocalContextName(name)
	if err != nil {
		return err
	}
	if err := os.MkdirAll(r.dir(), 0o700); err != nil {
		return fmt.Errorf("create context registry: %w", err)
	}
	unlock, err := acquireLocalContextRegistryLock(r.lockPath())
	if err != nil {
		return err
	}
	defer unlock()
	return atomicWriteFile(r.currentPath(), []byte(name+"\n"), 0o600)
}

func (r localContextRegistry) ClearCurrent() error {
	if err := os.MkdirAll(r.dir(), 0o700); err != nil {
		return fmt.Errorf("create context registry: %w", err)
	}
	unlock, err := acquireLocalContextRegistryLock(r.lockPath())
	if err != nil {
		return err
	}
	defer unlock()
	err = os.Remove(r.currentPath())
	if err == nil || errors.Is(err, os.ErrNotExist) {
		return nil
	}
	return err
}

func (r localContextRegistry) CurrentName() (string, error) {
	raw, err := os.ReadFile(r.currentPath())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return "", nil
		}
		return "", err
	}
	name := strings.TrimSpace(string(raw))
	if name == "" {
		return "", nil
	}
	return normalizeLocalContextName(name)
}

func (r localContextRegistry) ReadDescriptor(name string) (localContextEntry, error) {
	path, err := r.descriptorPath(name)
	if err != nil {
		return localContextEntry{Status: localContextStatusInvalidDescriptor, Detail: err.Error()}, nil
	}
	desc, status, detail := readLocalContextDescriptorFile(path)
	if status != localContextStatusOK {
		if strings.TrimSpace(desc.Name) == "" {
			desc.Name = strings.TrimSuffix(filepath.Base(path), ".json")
		}
		return localContextEntry{Descriptor: desc, Path: path, Status: status, Detail: detail}, nil
	}
	return localContextEntry{Descriptor: desc, Path: path, Status: localContextStatusOK}, nil
}

func (r localContextRegistry) ListDescriptors() ([]localContextEntry, error) {
	entries, err := os.ReadDir(r.dir())
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return []localContextEntry{}, nil
		}
		return nil, err
	}
	out := make([]localContextEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.IsDir() || !strings.HasSuffix(entry.Name(), ".json") {
			continue
		}
		path := filepath.Join(r.dir(), entry.Name())
		desc, status, detail := readLocalContextDescriptorFile(path)
		if strings.TrimSpace(desc.Name) == "" {
			desc.Name = strings.TrimSuffix(entry.Name(), ".json")
		}
		out = append(out, localContextEntry{Descriptor: desc, Path: path, Status: status, Detail: detail})
	}
	sort.Slice(out, func(i, j int) bool {
		return out[i].Descriptor.Name < out[j].Descriptor.Name
	})
	return out, nil
}

func (r localContextRegistry) Inspect(ctx context.Context, caller runtimeIdentityCaller) (localContextRegistryReport, error) {
	entries, err := r.ListDescriptors()
	if err != nil {
		status := localContextStatusPermissionDenied
		if !errors.Is(err, os.ErrPermission) {
			status = localContextStatusInvalidDescriptor
		}
		return localContextRegistryReport{Owner: localContextRegistryOwner, Status: status, Detail: err.Error()}, nil
	}
	for i := range entries {
		entries[i] = validateLocalContextEntry(ctx, entries[i], caller)
	}
	currentName, currentErr := r.CurrentName()
	if currentErr != nil {
		return localContextRegistryReport{
			Owner:   localContextRegistryOwner,
			Status:  localContextStatusCorruptDescriptor,
			Detail:  fmt.Sprintf("current selector is invalid: %v", currentErr),
			Entries: entries,
		}, nil
	}
	report := localContextRegistryReport{
		Owner:   localContextRegistryOwner,
		Status:  localContextStatusOK,
		Entries: entries,
	}
	if currentName == "" {
		if len(entries) == 0 {
			report.Status = "empty"
			report.Detail = "no context descriptors found"
		} else {
			report.Status = "no_current"
			report.Detail = "no selected current context"
		}
		return report, nil
	}
	for i := range entries {
		if entries[i].Descriptor.Name == currentName {
			entry := entries[i]
			report.Current = &entry
			report.Status = entry.Status
			report.Detail = entry.Detail
			return report, nil
		}
	}
	entry, err := r.ReadDescriptor(currentName)
	if err != nil {
		return localContextRegistryReport{Owner: localContextRegistryOwner, Status: localContextStatusInvalidDescriptor, Detail: err.Error(), Entries: entries}, nil
	}
	entry = validateLocalContextEntry(ctx, entry, caller)
	report.Current = &entry
	report.Status = entry.Status
	report.Detail = entry.Detail
	return report, nil
}

func (r localContextRegistry) Prune(ctx context.Context, caller runtimeIdentityCaller) (localContextPruneResult, error) {
	if err := os.MkdirAll(r.dir(), 0o700); err != nil {
		return localContextPruneResult{}, fmt.Errorf("create context registry: %w", err)
	}
	unlock, err := acquireLocalContextRegistryLock(r.lockPath())
	if err != nil {
		return localContextPruneResult{}, err
	}
	defer unlock()
	report, err := r.Inspect(ctx, caller)
	if err != nil {
		return localContextPruneResult{}, err
	}
	currentName, _ := r.CurrentName()
	result := localContextPruneResult{}
	prunedCurrent := false
	currentSeen := false
	for _, entry := range report.Entries {
		if localContextStatusPruneable(entry.Status) {
			if entry.Path != "" {
				if err := os.Remove(entry.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
					return result, err
				}
			}
			result.Removed = append(result.Removed, entry)
			if entry.Descriptor.Name == currentName {
				currentSeen = true
				prunedCurrent = true
			}
			continue
		}
		result.Kept = append(result.Kept, entry)
		if entry.Descriptor.Name == currentName {
			currentSeen = true
		}
	}
	if !currentSeen && report.Current != nil && localContextStatusPruneable(report.Current.Status) {
		if report.Current.Path != "" {
			if err := os.Remove(report.Current.Path); err != nil && !errors.Is(err, os.ErrNotExist) {
				return result, err
			}
		}
		result.Removed = append(result.Removed, *report.Current)
		prunedCurrent = true
	}
	if prunedCurrent {
		if err := os.Remove(r.currentPath()); err != nil && !errors.Is(err, os.ErrNotExist) {
			return result, err
		}
	}
	return result, nil
}

func localContextStatusPruneable(status string) bool {
	switch status {
	case localContextStatusNoServer, localContextStatusStaleDescriptor, localContextStatusIdentityMismatch,
		localContextStatusUnsupportedTransport, localContextStatusCorruptDescriptor, localContextStatusInvalidDescriptor:
		return true
	default:
		return false
	}
}

func readLocalContextDescriptorFile(path string) (localContextDescriptor, string, string) {
	raw, err := os.ReadFile(path)
	if err != nil {
		if errors.Is(err, os.ErrPermission) {
			return localContextDescriptor{}, localContextStatusPermissionDenied, err.Error()
		}
		return localContextDescriptor{}, localContextStatusCorruptDescriptor, err.Error()
	}
	dec := json.NewDecoder(bytes.NewReader(raw))
	dec.DisallowUnknownFields()
	var desc localContextDescriptor
	if err := dec.Decode(&desc); err != nil {
		return localContextDescriptor{}, localContextStatusCorruptDescriptor, err.Error()
	}
	if err := ensureJSONEOF(dec); err != nil {
		return localContextDescriptor{}, localContextStatusCorruptDescriptor, err.Error()
	}
	if err := validateLocalContextDescriptorShape(desc); err != nil {
		return desc, localContextStatusInvalidDescriptor, err.Error()
	}
	return desc, localContextStatusOK, ""
}

func ensureJSONEOF(dec *json.Decoder) error {
	var extra any
	if err := dec.Decode(&extra); err == io.EOF {
		return nil
	} else if err != nil {
		return err
	}
	return fmt.Errorf("descriptor contains trailing JSON")
}

func validateLocalContextDescriptorShape(desc localContextDescriptor) error {
	if desc.Version != localContextDescriptorVersion {
		return fmt.Errorf("descriptor version = %d, want %d", desc.Version, localContextDescriptorVersion)
	}
	name, err := normalizeLocalContextName(desc.Name)
	if err != nil {
		return err
	}
	if name != desc.Name {
		return fmt.Errorf("context name must be canonical")
	}
	if strings.TrimSpace(desc.RuntimeInstanceID) == "" {
		return fmt.Errorf("runtime_instance_id is required")
	}
	switch desc.Transport {
	case localContextTransportTCP:
		if strings.TrimSpace(desc.APIServer) == "" {
			return fmt.Errorf("api_server is required for tcp descriptors")
		}
		if _, err := cliAPIRPCEndpointFromServer(desc.APIServer, "descriptor api_server"); err != nil {
			return err
		}
	case localContextTransportUnix:
		if strings.TrimSpace(desc.SocketPath) == "" {
			return fmt.Errorf("socket_path is required for unix descriptors")
		}
	default:
		return fmt.Errorf("unsupported transport %q", desc.Transport)
	}
	switch desc.Auth.Mode {
	case localContextAuthBuiltinLoopback:
		if desc.Transport != localContextTransportTCP {
			return fmt.Errorf("builtin_loopback auth is only valid for tcp descriptors")
		}
		rpcEndpoint, err := cliAPIRPCEndpointFromServer(desc.APIServer, "descriptor api_server")
		if err != nil {
			return err
		}
		if !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
			return fmt.Errorf("builtin_loopback auth requires numeric loopback api_server")
		}
	case localContextAuthTokenFile:
		if strings.TrimSpace(desc.Auth.TokenFile) == "" {
			return fmt.Errorf("token_file auth requires token_file")
		}
	default:
		return fmt.Errorf("unsupported auth mode %q", desc.Auth.Mode)
	}
	if strings.TrimSpace(desc.CreatedAt) == "" {
		return fmt.Errorf("created_at is required")
	}
	if strings.TrimSpace(desc.UpdatedAt) == "" {
		return fmt.Errorf("updated_at is required")
	}
	return nil
}

func validateLocalContextEntry(ctx context.Context, entry localContextEntry, caller runtimeIdentityCaller) localContextEntry {
	if entry.Status != localContextStatusOK {
		return entry
	}
	desc := entry.Descriptor
	if desc.Transport == localContextTransportUnix {
		entry.Status = localContextStatusUnsupportedTransport
		entry.Detail = "unix descriptor is schema-valid but IPC dialing is split to #1576"
		return entry
	}
	rpcEndpoint, err := cliAPIRPCEndpointFromServer(desc.APIServer, "descriptor api_server")
	if err != nil {
		entry.Status = localContextStatusInvalidDescriptor
		entry.Detail = err.Error()
		return entry
	}
	token, err := localContextDescriptorToken(desc, rpcEndpoint)
	if err != nil {
		entry.Status = classifyLocalContextTokenError(err)
		entry.Detail = err.Error()
		return entry
	}
	if caller == nil {
		caller = cliRuntimeIdentityCaller{httpClient: &http.Client{Timeout: 2 * time.Second}}
	}
	identity, err := caller.callRuntimeIdentity(ctx, rpcEndpoint, token)
	if err != nil {
		entry.Status = classifyLocalContextIdentityError(err)
		entry.Detail = err.Error()
		return entry
	}
	if strings.TrimSpace(identity.RuntimeInstanceID) == "" {
		entry.Status = localContextStatusStaleDescriptor
		entry.Detail = "runtime.identity returned an empty runtime_instance_id"
		return entry
	}
	if identity.RuntimeInstanceID != desc.RuntimeInstanceID {
		entry.Status = localContextStatusIdentityMismatch
		entry.Detail = fmt.Sprintf("descriptor runtime_instance_id=%s, live runtime_instance_id=%s", desc.RuntimeInstanceID, identity.RuntimeInstanceID)
		return entry
	}
	entry.Status = localContextStatusOK
	entry.Detail = ""
	return entry
}

func localContextDescriptorToken(desc localContextDescriptor, rpcEndpoint string) (string, error) {
	switch desc.Auth.Mode {
	case localContextAuthBuiltinLoopback:
		if !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
			return "", fmt.Errorf("builtin_loopback auth requires numeric loopback api_server")
		}
		return apiv1.DefaultLoopbackAPIToken, nil
	case localContextAuthTokenFile:
		return readCLIAPITokenFile(desc.Auth.TokenFile, "descriptor auth.token_file")
	default:
		return "", fmt.Errorf("unsupported auth mode %q", desc.Auth.Mode)
	}
}

func classifyLocalContextTokenError(err error) string {
	if errors.Is(err, os.ErrPermission) {
		return localContextStatusPermissionDenied
	}
	return localContextStatusAuthFailure
}

func classifyLocalContextIdentityError(err error) string {
	var httpErr *cliAPIHTTPError
	if errors.As(err, &httpErr) {
		switch httpErr.statusCode {
		case http.StatusUnauthorized:
			return localContextStatusAuthFailure
		case http.StatusForbidden:
			return localContextStatusPermissionDenied
		default:
			return localContextStatusStaleDescriptor
		}
	}
	var netErr net.Error
	if errors.As(err, &netErr) {
		return localContextStatusNoServer
	}
	if isConnectionRefusedError(err) {
		return localContextStatusNoServer
	}
	var rpcErr *jsonRPCError
	if errors.As(err, &rpcErr) {
		if rpcErr.Code == -32601 || applicationErrorCode(rpcErr.Data) == apiv1.MethodUnavailableCode {
			return localContextStatusStaleDescriptor
		}
		return localContextStatusStaleDescriptor
	}
	return localContextStatusNoServer
}

func isConnectionRefusedError(err error) bool {
	msg := strings.ToLower(err.Error())
	return strings.Contains(msg, "connection refused") ||
		strings.Contains(msg, "connect: cannot assign requested address") ||
		strings.Contains(msg, "no such host")
}

func acquireLocalContextRegistryLock(path string) (func(), error) {
	lock, err := os.OpenFile(path, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
	if err != nil {
		if errors.Is(err, os.ErrExist) {
			return nil, fmt.Errorf("context registry is locked: %w", err)
		}
		return nil, fmt.Errorf("acquire context registry lock: %w", err)
	}
	_, _ = fmt.Fprintf(lock, "pid=%d\n", os.Getpid())
	_ = lock.Close()
	return func() {
		_ = os.Remove(path)
	}, nil
}

func atomicWriteJSON(path string, value any, perm os.FileMode) error {
	var buf bytes.Buffer
	encoder := json.NewEncoder(&buf)
	encoder.SetIndent("", "  ")
	if err := encoder.Encode(value); err != nil {
		return err
	}
	return atomicWriteFile(path, buf.Bytes(), perm)
}

func atomicWriteFile(path string, data []byte, perm os.FileMode) error {
	dir := filepath.Dir(path)
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return err
	}
	tmp, err := os.CreateTemp(dir, "."+filepath.Base(path)+".tmp-*")
	if err != nil {
		return err
	}
	tmpName := tmp.Name()
	cleanup := true
	defer func() {
		if cleanup {
			_ = os.Remove(tmpName)
		}
	}()
	if err := tmp.Chmod(perm); err != nil {
		_ = tmp.Close()
		return err
	}
	if _, err := tmp.Write(data); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Sync(); err != nil {
		_ = tmp.Close()
		return err
	}
	if err := tmp.Close(); err != nil {
		return err
	}
	if err := os.Rename(tmpName, path); err != nil {
		return err
	}
	cleanup = false
	_ = syncLocalContextRegistryDir(dir)
	return nil
}

func syncLocalContextRegistryDir(dir string) error {
	handle, err := os.Open(dir)
	if err != nil {
		return err
	}
	defer handle.Close()
	return handle.Sync()
}
