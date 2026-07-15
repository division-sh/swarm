package cliapp

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

type ServeProjectContextRegistration struct {
	registry    localContextRegistry
	project     cliProjectResolution
	contextName string
	release     func()
	registered  bool
}

func PrepareServeProjectContextRegistration(ctx context.Context, repo string, opts ServeOptions, resolvedPaths CLIContractPlatformSpecPaths) (*ServeProjectContextRegistration, error) {
	if !opts.Dev || opts.LocalRun {
		return nil, nil
	}
	swarmDir, err := ResolveServeContextRegistrationSwarmDir(opts)
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
	return &ServeProjectContextRegistration{
		registry:    registry,
		project:     project,
		contextName: contextName,
		release:     release,
	}, nil
}

func ResolveServeContextRegistrationSwarmDir(opts ServeOptions) (CLISwarmDirResolution, error) {
	if opts.SwarmDirSet {
		path, err := normalizeCLISwarmDir(opts.SwarmDir, "--swarm-dir")
		return CLISwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLISwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(cliSwarmDirOptions{}, cfg)
}

func (r *ServeProjectContextRegistration) Release() {
	if r == nil || r.release == nil {
		return
	}
	r.release()
	r.release = nil
}

func (r *ServeProjectContextRegistration) Unregister() {
	if r == nil || !r.registered {
		return
	}
	if err := r.registry.DeleteDescriptor(r.contextName); err != nil {
		log.Printf("unregister local project context %s: %v", r.contextName, err)
	}
	r.registered = false
}

func (r *ServeProjectContextRegistration) WriteFinal(runtimeInstanceID string, apiAddr net.Addr, apiAuth apiv1.AuthTokenResolution, resolvedPaths CLIContractPlatformSpecPaths, storeSelection storebackend.Selection, mountSources WorkspaceMountSources) error {
	if r == nil {
		return nil
	}
	apiServer, err := serveProjectContextAPIServer(apiAddr)
	if err != nil {
		return err
	}
	rpcEndpoint, err := cliAPIRPCEndpointFromServer(apiServer, "serve api listener")
	if err != nil {
		return err
	}
	auth := localContextDescriptorAuth{Mode: localContextAuthBuiltinLoopback}
	if apiAuth.UsesDefaultLoopbackToken() {
		if !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
			return fmt.Errorf("serve --dev project context registration requires a numeric loopback API listener for built-in loopback auth, got %s", apiServer)
		}
	} else {
		tokenFile := strings.TrimSpace(apiAuth.TokenFile)
		if tokenFile == "" {
			return fmt.Errorf("serve --dev project context registration requires token-file auth for explicit API auth source %s", apiAuth.Source)
		}
		auth = localContextDescriptorAuth{Mode: localContextAuthTokenFile, TokenFile: tokenFile}
	}
	if auth.Mode == localContextAuthBuiltinLoopback && !cliAPIRPCEndpointAllowsDefaultToken(rpcEndpoint) {
		return fmt.Errorf("serve --dev project context registration requires a numeric loopback API listener, got %s", apiServer)
	}
	now := localContextTimestamp()
	desc := localContextDescriptor{
		Version:           localContextDescriptorVersion,
		Name:              r.contextName,
		RuntimeInstanceID: runtimeInstanceID,
		Transport:         localContextTransportTCP,
		APIServer:         apiServer,
		Auth:              auth,
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
