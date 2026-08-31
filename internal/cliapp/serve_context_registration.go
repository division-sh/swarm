package cliapp

import (
	"fmt"
	"log"
	"net"
	"os"
	"strings"

	"github.com/division-sh/swarm/internal/apiv1"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/devscratch"
)

type ServeProjectContextRegistration struct {
	registry    localContextRegistry
	project     cliProjectResolution
	contextName string
	registered  bool
}

func PrepareServeProjectContextRegistration(root InvocationRoot, opts ServeOptions, resolvedPaths CLISourcePlatformSpecPaths, grant devscratch.RegistrationGrant) (*ServeProjectContextRegistration, error) {
	if !opts.Dev || opts.LocalRun {
		return nil, nil
	}
	swarmDir, err := ResolveServeContextRegistrationSwarmDir(root, opts)
	if err != nil {
		return nil, err
	}
	selectedRoot := strings.TrimSpace(resolvedPaths.SourceRoot)
	if selectedRoot == "" {
		return nil, fmt.Errorf("serve --dev project context registration requires a selected source root")
	}
	projectRoot := selectedRoot
	canonical, _ := canonicalizeDoctorTargetPath(projectRoot)
	canonical = strings.TrimSpace(canonical)
	if canonical == "" {
		return nil, fmt.Errorf("serve --dev project context registration requires a canonical project root")
	}
	if err := grant.ValidateProject(canonical); err != nil {
		return nil, err
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
		projectRoot:          projectRoot,
		canonicalProjectRoot: canonical,
	}
	if err := reconcileServeProjectContextRegistration(registry, project, contextName); err != nil {
		return nil, err
	}
	return &ServeProjectContextRegistration{
		registry:    registry,
		project:     project,
		contextName: contextName,
	}, nil
}

func reconcileServeProjectContextRegistration(registry localContextRegistry, project cliProjectResolution, contextName string) error {
	entries, err := registry.ListDescriptors()
	if err != nil {
		return fmt.Errorf("inspect context registry: %w", err)
	}
	for _, entry := range entries {
		entryName := strings.TrimSpace(entry.Descriptor.Name)
		entryProject := strings.TrimSpace(entry.Descriptor.ProjectRoot)
		canonicalEntryProject, _ := canonicalizeDoctorTargetPath(entryProject)
		if strings.TrimSpace(canonicalEntryProject) == project.canonicalProjectRoot {
			if err := registry.DeleteDescriptor(entryName); err != nil {
				return fmt.Errorf("replace predecessor project context %s: %w", entryName, err)
			}
			continue
		}
		if entryName == contextName {
			if entryProject == "" {
				entryProject = "<unknown>"
			}
			return fmt.Errorf("context %s already exists for project %s; context names are global, choose another --context", contextName, entryProject)
		}
	}
	return nil
}

func ResolveServeContextRegistrationSwarmDir(root InvocationRoot, opts ServeOptions) (CLISwarmDirResolution, error) {
	if opts.SwarmDirSet {
		path, err := normalizeCLISwarmDir(root, opts.SwarmDir, "--swarm-dir")
		return CLISwarmDirResolution{Path: path, Source: "--swarm-dir"}, err
	}
	cfg, err := loadCLICommandConfigWithOptions(unifiedConfigLoadOptions{RepoRoot: root.Path(), ExplicitPath: opts.ConfigPath})
	if err != nil {
		return CLISwarmDirResolution{}, err
	}
	return resolveCLISwarmDirFromConfig(root, cliSwarmDirOptions{}, cfg)
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

func (r *ServeProjectContextRegistration) WriteFinal(runtimeInstanceID string, apiAddr net.Addr, apiAuth apiv1.AuthTokenResolution, resolvedPaths CLISourcePlatformSpecPaths, storeSelection storebackend.Selection, mountSources WorkspaceMountSources) error {
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
		StorePath:         serveDescriptorStorePath(storeSelection),
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
