package cliapp

import (
	"context"
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"path/filepath"
	"regexp"
	"strings"
	"time"
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
	projectRoot          string
	canonicalProjectRoot string
}

func resolveCLIAPITarget(opts rootCommandOptions, cfg cliCommandConfig) (cliAPITargetResolution, error) {
	if opts.apiCommandClass != cliAPICommandClassTargetDiagnostic {
		if err := rejectRemovedClientAPIEnvSources(); err != nil {
			return cliAPITargetResolution{}, err
		}
	}
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
	return resolveCLIAPISelectedConfigOrDefaultTarget(opts, cfg)
}

func resolveCLIAPIExplicitContextTarget(opts rootCommandOptions, cfg cliCommandConfig, contextName string) (cliAPITargetResolution, error) {
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

func resolveCLIAPISelectedConfigOrDefaultTarget(opts rootCommandOptions, cfg cliCommandConfig) (cliAPITargetResolution, error) {
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
	if server := strings.TrimSpace(cfg.Connection.APIServer); server != "" {
		rpc, err := cliAPIRPCEndpointFromServer(server, cliAPIConfigServerSource)
		return cliAPITargetResolution{rpcEndpoint: rpc, source: cliAPIConfigServerSource}, err
	}
	rpc, err := cliAPIRPCEndpointFromServer(defaultCLIAPIServer, "built-in loopback default")
	return cliAPITargetResolution{rpcEndpoint: rpc, source: "built-in loopback default"}, err
}

func cliAPILocalContextRegistry(opts rootCommandOptions, cfg cliCommandConfig) (localContextRegistry, error) {
	swarmDir, err := resolveCLISwarmDirFromConfig(opts.invocationRoot, opts.swarmDirResolutionOptions(), cfg)
	if err != nil {
		return localContextRegistry{}, err
	}
	return newLocalContextRegistry(swarmDir.Path), nil
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

func resolveCLIAPITokenForTarget(opts rootCommandOptions, cfg cliCommandConfig, target cliAPITargetResolution) (cliAPITokenResolution, error) {
	if err := rejectRemovedClientAPIEnvSources(); err != nil {
		return cliAPITokenResolution{}, err
	}
	if tokenFile := strings.TrimSpace(opts.apiTokenFile); tokenFile != "" {
		return readCLIAPIExplicitTokenFile(opts.invocationRoot, tokenFile, "--api-token-file")
	}
	if target.descriptor == nil {
		return resolveCLIAPIToken(opts, cfg, target.rpcEndpoint)
	}
	token, err := localContextDescriptorToken(*target.descriptor, target.rpcEndpoint)
	if err != nil {
		return cliAPITokenResolution{}, err
	}
	return cliAPITokenResolution{token: token, source: "context descriptor " + target.descriptor.Auth.Mode}, nil
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
