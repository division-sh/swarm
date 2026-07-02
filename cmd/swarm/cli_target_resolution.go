package main

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"os"
	"path/filepath"
	"regexp"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/apiv1"
)

type cliAPICommandClass string

const (
	cliAPICommandClassReadOnly         cliAPICommandClass = "read_only_inspection"
	cliAPICommandClassMutating         cliAPICommandClass = "mutating_runtime_state"
	cliAPICommandClassControl          cliAPICommandClass = "control_destructive"
	cliAPICommandClassTargetDiagnostic cliAPICommandClass = "target_diagnostic"
)

type cliAPITargetResolution struct {
	rpcEndpoint string
	source      string
	contextName string
	projectRoot string
	descriptor  *localContextDescriptor
}

type cliProjectResolution struct {
	contractsPath        string
	projectRoot          string
	canonicalProjectRoot string
}

func resolveCLIAPITarget(opts rootCommandOptions, cfg cliAPIConfigFile) (cliAPITargetResolution, error) {
	if endpoint := strings.TrimSpace(opts.apiRPCEndpointOverride); endpoint != "" {
		rpc, err := normalizeCLIAPIRPCEndpoint(endpoint, "internal API endpoint")
		return cliAPITargetResolution{rpcEndpoint: rpc, source: "internal API endpoint"}, err
	}
	if server := strings.TrimSpace(opts.apiServer); server != "" {
		rpc, err := cliAPIRPCEndpointFromServer(server, "--api-server")
		return cliAPITargetResolution{rpcEndpoint: rpc, source: "--api-server"}, err
	}
	if contextName := strings.TrimSpace(opts.contextName); contextName != "" {
		return resolveCLIAPIExplicitContextTarget(opts, cfg, contextName)
	}
	if server := strings.TrimSpace(os.Getenv("SWARM_API_SERVER")); server != "" {
		rpc, err := cliAPIRPCEndpointFromServer(server, "SWARM_API_SERVER")
		return cliAPITargetResolution{rpcEndpoint: rpc, source: "SWARM_API_SERVER"}, err
	}
	if server := strings.TrimSpace(cfg.APIServer); server != "" {
		rpc, err := cliAPIRPCEndpointFromServer(server, "config api_server")
		return cliAPITargetResolution{rpcEndpoint: rpc, source: "config api_server"}, err
	}
	if !opts.disableLocalTargeting {
		if project, ok := resolveCLIAPIProject(opts, cfg); ok {
			return resolveCLIAPIProjectTarget(opts, cfg, project)
		}
	}
	return resolveCLIAPISelectedOrDefaultTarget(opts, cfg)
}

func resolveCLIAPIExplicitContextTarget(opts rootCommandOptions, cfg cliAPIConfigFile, contextName string) (cliAPITargetResolution, error) {
	registry, err := cliAPILocalContextRegistry(opts, cfg)
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	entry, err := registry.ReadDescriptor(contextName)
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	entry = validateLocalContextEntry(context.Background(), entry, cliRuntimeIdentityCaller{httpClient: opts.httpClient})
	if entry.Status != localContextStatusOK {
		return cliAPITargetResolution{}, cliAPIContextResolutionError("explicit context", entry)
	}
	return cliAPITargetFromDescriptor(entry, "--context")
}

func resolveCLIAPIProjectTarget(opts rootCommandOptions, cfg cliAPIConfigFile, project cliProjectResolution) (cliAPITargetResolution, error) {
	registry, err := cliAPILocalContextRegistry(opts, cfg)
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	entries, err := registry.ProjectEntries(context.Background(), project.canonicalProjectRoot, cliRuntimeIdentityCaller{httpClient: opts.httpClient})
	if err != nil {
		return cliAPITargetResolution{}, &cliAPIValidationError{message: fmt.Sprintf("inspect project contexts: %v", err)}
	}
	okEntries := make([]localContextEntry, 0, len(entries))
	for _, entry := range entries {
		if entry.Status == localContextStatusOK {
			okEntries = append(okEntries, entry)
		}
	}
	switch {
	case len(okEntries) == 1 && len(entries) == 1:
		target, err := cliAPITargetFromDescriptor(okEntries[0], "project context")
		if err != nil {
			return cliAPITargetResolution{}, err
		}
		target.projectRoot = project.canonicalProjectRoot
		return target, nil
	case len(okEntries) > 1:
		return cliAPITargetResolution{}, &cliAPIValidationError{message: fmt.Sprintf("multiple live project contexts for %s; pass --context to choose one", project.canonicalProjectRoot)}
	case len(entries) > 0:
		return cliAPITargetResolution{}, cliAPIProjectContextError(project, entries)
	case !cliAPICommandClassAllowsProjectlessFallthrough(opts.apiCommandClass):
		commandClass := opts.apiCommandClass
		if commandClass == "" {
			commandClass = cliAPICommandClassMutating
		}
		return cliAPITargetResolution{}, &cliAPIValidationError{message: fmt.Sprintf("no live project context for %s; refusing %s command without explicit --context or --api-server; start `swarm serve --dev` for this project or choose a target explicitly", project.canonicalProjectRoot, commandClass)}
	default:
		return resolveCLIAPISelectedOrDefaultTarget(opts, cfg)
	}
}

func resolveCLIAPISelectedOrDefaultTarget(opts rootCommandOptions, cfg cliAPIConfigFile) (cliAPITargetResolution, error) {
	registry, err := cliAPILocalContextRegistry(opts, cfg)
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	report, err := registry.Inspect(context.Background(), cliRuntimeIdentityCaller{httpClient: opts.httpClient})
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	if report.Current != nil {
		if report.Current.Status != localContextStatusOK {
			return cliAPITargetResolution{}, cliAPIContextResolutionError("selected context", *report.Current)
		}
		return cliAPITargetFromDescriptor(*report.Current, "selected context")
	}
	if report.Status != "" && report.Status != "empty" && report.Status != "no_current" && report.Status != localContextStatusOK {
		return cliAPITargetResolution{}, &cliAPIValidationError{message: fmt.Sprintf("local context registry is %s: %s", report.Status, report.Detail)}
	}
	rpc, err := cliAPIRPCEndpointFromServer(defaultCLIAPIServer, "built-in loopback default")
	return cliAPITargetResolution{rpcEndpoint: rpc, source: "built-in loopback default"}, err
}

func cliAPILocalContextRegistry(opts rootCommandOptions, cfg cliAPIConfigFile) (localContextRegistry, error) {
	swarmDir, err := resolveCLISwarmDirFromConfig(opts.swarmDirResolutionOptions(), cfg)
	if err != nil {
		return localContextRegistry{}, err
	}
	return newLocalContextRegistry(swarmDir.Path), nil
}

func resolveCLIAPIProject(opts rootCommandOptions, cfg cliAPIConfigFile) (cliProjectResolution, bool) {
	contractsPath := firstNonEmpty(
		os.Getenv(cliContractsPathEnv),
		cfg.ContractsPath,
		discoverRepoContractsPath(opts.repoRoot),
	)
	if strings.TrimSpace(contractsPath) == "" {
		return cliProjectResolution{}, false
	}
	contractsPath = resolvePath(opts.repoRoot, contractsPath)
	projectRoot := inferProjectRootFromContractsPath(contractsPath)
	canonical, _ := canonicalizeDoctorTargetPath(projectRoot)
	if strings.TrimSpace(canonical) == "" {
		return cliProjectResolution{}, false
	}
	return cliProjectResolution{
		contractsPath:        filepath.Clean(contractsPath),
		projectRoot:          filepath.Clean(projectRoot),
		canonicalProjectRoot: filepath.Clean(canonical),
	}, true
}

func cliAPITargetFromDescriptor(entry localContextEntry, source string) (cliAPITargetResolution, error) {
	rpc, err := cliAPIRPCEndpointFromServer(entry.Descriptor.APIServer, "descriptor api_server")
	if err != nil {
		return cliAPITargetResolution{}, err
	}
	desc := entry.Descriptor
	return cliAPITargetResolution{
		rpcEndpoint: rpc,
		source:      source,
		contextName: desc.Name,
		projectRoot: desc.ProjectRoot,
		descriptor:  &desc,
	}, nil
}

func resolveCLIAPITokenForTarget(opts rootCommandOptions, cfg cliAPIConfigFile, target cliAPITargetResolution) (cliAPITokenResolution, error) {
	if target.descriptor == nil {
		return resolveCLIAPIToken(opts, cfg, target.rpcEndpoint)
	}
	if tokenFile := strings.TrimSpace(opts.apiTokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "--api-token-file")
	}
	if token := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN")); token != "" {
		return cliAPITokenResolution{token: token, source: string(apiv1.AuthTokenSourceEnvironment), explicit: true}, nil
	}
	if tokenFile := strings.TrimSpace(os.Getenv("SWARM_API_TOKEN_FILE")); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "SWARM_API_TOKEN_FILE")
	}
	if tokenFile := strings.TrimSpace(cfg.APITokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(tokenFile, "config api_token_file")
	}
	token, err := localContextDescriptorToken(*target.descriptor, target.rpcEndpoint)
	if err != nil {
		return cliAPITokenResolution{}, err
	}
	return cliAPITokenResolution{token: token, source: "context descriptor " + target.descriptor.Auth.Mode}, nil
}

func cliAPICommandClassAllowsProjectlessFallthrough(class cliAPICommandClass) bool {
	switch class {
	case "", cliAPICommandClassReadOnly, cliAPICommandClassTargetDiagnostic:
		return true
	default:
		return false
	}
}

func cliAPIContextResolutionError(prefix string, entry localContextEntry) error {
	name := strings.TrimSpace(entry.Descriptor.Name)
	if name == "" {
		name = "<unknown>"
	}
	detail := strings.TrimSpace(entry.Detail)
	if detail != "" {
		detail = ": " + detail
	}
	return &cliAPIValidationError{message: fmt.Sprintf("%s %s is not usable (%s)%s; run `swarm context prune` or pass --context/--api-server explicitly", prefix, name, entry.Status, detail)}
}

func cliAPIProjectContextError(project cliProjectResolution, entries []localContextEntry) error {
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Descriptor.Name)
		if name == "" {
			name = "<unknown>"
		}
		parts = append(parts, fmt.Sprintf("%s=%s", name, entry.Status))
	}
	return &cliAPIValidationError{message: fmt.Sprintf("project context for %s is not usable (%s); run `swarm context prune` or pass --context/--api-server explicitly", project.canonicalProjectRoot, strings.Join(parts, ", "))}
}

func localProjectContextName(canonicalProjectRoot string) string {
	base := filepath.Base(filepath.Clean(canonicalProjectRoot))
	base = sanitizeLocalContextNameComponent(base)
	if base == "" || base == "." || base == string(filepath.Separator) {
		base = "project"
	}
	sum := sha256.Sum256([]byte(filepath.Clean(canonicalProjectRoot)))
	return fmt.Sprintf("%s-%s", base, hex.EncodeToString(sum[:])[:12])
}

var localContextNameComponentRe = regexp.MustCompile(`[^A-Za-z0-9._-]+`)

func sanitizeLocalContextNameComponent(raw string) string {
	out := localContextNameComponentRe.ReplaceAllString(strings.TrimSpace(raw), "-")
	out = strings.Trim(out, "-._")
	if out == "" {
		return ""
	}
	if len(out) > 40 {
		out = out[:40]
		out = strings.Trim(out, "-._")
	}
	return out
}

func localContextTimestamp() string {
	return time.Now().UTC().Format(time.RFC3339Nano)
}
