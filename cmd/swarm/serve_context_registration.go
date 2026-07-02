package main

import (
	"context"
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/apiv1"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

type serveProjectContextRegistration struct {
	registry    localContextRegistry
	project     cliProjectResolution
	contextName string
	release     func()
	registered  bool
}

func prepareServeProjectContextRegistration(ctx context.Context, repo string, opts serveOptions, resolvedPaths cliContractPlatformSpecPaths) (*serveProjectContextRegistration, error) {
	if !opts.Dev || opts.LocalRun {
		return nil, nil
	}
	swarmDir, err := resolveServeContextRegistrationSwarmDir(opts)
	if err != nil {
		return nil, err
	}
	contractsPath := strings.TrimSpace(resolvedPaths.ContractsPath)
	if contractsPath == "" {
		return nil, fmt.Errorf("serve --dev project context registration requires a resolved contracts path")
	}
	projectRoot := inferProjectRootFromContractsPath(contractsPath)
	canonical, _ := canonicalizeDoctorTargetPath(projectRoot)
	canonical = strings.TrimSpace(canonical)
	if canonical == "" {
		return nil, fmt.Errorf("serve --dev project context registration requires a canonical project root")
	}
	contextName := strings.TrimSpace(opts.ContextName)
	if contextName == "" {
		contextName = localProjectContextName(canonical)
	}
	contextName, err = normalizeLocalContextName(contextName)
	if err != nil {
		return nil, err
	}
	registry := newLocalContextRegistry(swarmDir.Path)
	project := cliProjectResolution{
		contractsPath:        contractsPath,
		projectRoot:          projectRoot,
		canonicalProjectRoot: canonical,
	}
	if err := guardServeProjectContext(ctx, registry, project, contextName, opts.ContextNameSet); err != nil {
		return nil, err
	}
	release, err := registry.AcquireProjectClaim(canonical, contextName)
	if err != nil {
		return nil, err
	}
	return &serveProjectContextRegistration{
		registry:    registry,
		project:     project,
		contextName: contextName,
		release:     release,
	}, nil
}

func resolveServeContextRegistrationSwarmDir(opts serveOptions) (cliSwarmDirResolution, error) {
	if opts.SwarmDirSet {
		path, err := normalizeCLISwarmDir(opts.SwarmDir, "--swarm-dir")
		return cliSwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	cfg, err := loadCLIAPIConfigFile()
	if err != nil {
		return cliSwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(cliSwarmDirOptions{}, cfg)
}

func guardServeProjectContext(ctx context.Context, registry localContextRegistry, project cliProjectResolution, contextName string, explicitContext bool) error {
	entries, err := registry.ProjectEntries(ctx, project.canonicalProjectRoot, cliRuntimeIdentityCaller{})
	if err != nil {
		return fmt.Errorf("inspect project contexts: %w", err)
	}
	if explicitContext {
		for _, entry := range entries {
			if entry.Descriptor.Name == contextName {
				return fmt.Errorf("context %s already exists for project %s (%s); run `swarm context prune` after confirming stale entries, or choose another --context", contextName, project.canonicalProjectRoot, entry.Status)
			}
		}
		return nil
	}
	if len(entries) == 0 {
		return nil
	}
	parts := make([]string, 0, len(entries))
	for _, entry := range entries {
		name := strings.TrimSpace(entry.Descriptor.Name)
		if name == "" {
			name = "<unknown>"
		}
		parts = append(parts, fmt.Sprintf("%s=%s", name, entry.Status))
	}
	return fmt.Errorf("project %s already has context descriptors (%s); refusing bare `swarm serve --dev` to avoid orphaning a runtime; run `swarm context prune` for stale entries or pass --context for an intentional second runtime", project.canonicalProjectRoot, strings.Join(parts, ", "))
}

func (r *serveProjectContextRegistration) Release() {
	if r == nil || r.release == nil {
		return
	}
	r.release()
	r.release = nil
}

func (r *serveProjectContextRegistration) Unregister() {
	if r == nil || !r.registered {
		return
	}
	if err := r.registry.DeleteDescriptor(r.contextName); err != nil {
		log.Printf("unregister local project context %s: %v", r.contextName, err)
	}
	r.registered = false
}

func (r *serveProjectContextRegistration) WriteFinal(runtimeInstanceID string, apiAddr net.Addr, apiAuth apiv1.AuthTokenResolution, resolvedPaths cliContractPlatformSpecPaths, storeSelection storebackend.Selection, mountSources workspaceMountSources) error {
	if r == nil {
		return nil
	}
	if !apiAuth.UsesDefaultLoopbackToken() {
		return fmt.Errorf("serve --dev project context registration currently supports only built-in loopback auth; explicit SWARM_API_TOKEN cannot be snapshotted into a safe context descriptor")
	}
	apiServer, err := serveProjectContextAPIServer(apiAddr)
	if err != nil {
		return err
	}
	rpcEndpoint, err := cliAPIRPCEndpointFromServer(apiServer, "serve api listener")
	if err != nil {
		return err
	}
	if !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return fmt.Errorf("serve --dev project context registration requires a numeric loopback API listener, got %s", apiServer)
	}
	now := localContextTimestamp()
	desc := localContextDescriptor{
		Version:           localContextDescriptorVersion,
		Name:              r.contextName,
		RuntimeInstanceID: runtimeInstanceID,
		Transport:         localContextTransportTCP,
		APIServer:         apiServer,
		Auth:              localContextDescriptorAuth{Mode: localContextAuthBuiltinLoopback},
		ProjectRoot:       r.project.canonicalProjectRoot,
		ContractsPath:     strings.TrimSpace(resolvedPaths.ContractsPath),
		StorePath:         serveDescriptorStorePath(storeSelection),
		DataDir:           strings.TrimSpace(mountSources.DataSource),
		PID:               currentProcessID(),
		CreatedAt:         now,
		UpdatedAt:         now,
	}
	if err := r.registry.WriteDescriptor(desc); err != nil {
		return err
	}
	if err := r.registry.SetCurrent(desc.Name); err != nil {
		if path, pathErr := r.registry.descriptorPath(desc.Name); pathErr == nil {
			_ = os.Remove(path)
		}
		return err
	}
	r.registered = true
	return nil
}

func serveProjectContextAPIServer(addr net.Addr) (string, error) {
	if addr == nil {
		return "", fmt.Errorf("api listener address is required")
	}
	host, port, err := net.SplitHostPort(addr.String())
	if err != nil {
		return "", fmt.Errorf("api listener address %q is not host:port: %w", addr.String(), err)
	}
	host = strings.TrimSpace(strings.Trim(host, "[]"))
	port = strings.TrimSpace(port)
	if host == "" || port == "" {
		return "", fmt.Errorf("api listener address %q is incomplete", addr.String())
	}
	return "http://" + net.JoinHostPort(host, port), nil
}

func serveDescriptorStorePath(selection storebackend.Selection) string {
	if selection.Backend != storebackend.BackendSQLite {
		return ""
	}
	return strings.TrimSpace(selection.SQLitePath)
}

func currentProcessID() int {
	return os.Getpid()
}
