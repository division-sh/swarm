package cliapp

import (
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/devscratch"
)

func TestServeProjectContextRegistrationWritesFinalDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	reg, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	storePath := filepath.Join(t.TempDir(), "dev.db")
	if err := reg.WriteFinal("runtime-1", listener.Addr(), defaultLoopbackAuthResolution(), CLISourcePlatformSpecPaths{SourceRoot: project.contracts}, storebackend.Selection{
		Backend:    storebackend.BackendSQLite,
		SQLitePath: storePath,
	}, WorkspaceMountSources{}); err != nil {
		t.Fatalf("write final: %v", err)
	}

	registry := newLocalContextRegistry(swarmDir)
	entry, err := registry.ReadDescriptor(localProjectContextName(project.contracts))
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	if entry.Status != localContextStatusOK {
		t.Fatalf("descriptor status = %s detail=%s", entry.Status, entry.Detail)
	}
	desc := entry.Descriptor
	if desc.RuntimeInstanceID != "runtime-1" || desc.ProjectRoot != project.contracts {
		t.Fatalf("descriptor = %#v, want runtime/project metadata", desc)
	}
	if desc.StorePath != storePath || desc.Auth.Mode != localContextAuthBuiltinLoopback {
		t.Fatalf("descriptor = %#v, want store/builtin auth metadata", desc)
	}
	if current, err := registry.CurrentName(); err != nil || current != desc.Name {
		t.Fatalf("current = %q err=%v, want %q", current, err, desc.Name)
	}
}

func TestServeDevEpochAuthorityReplacesSameProjectDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.contracts), "runtime-live", server.URL, project.contracts)
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	_, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare registration under epoch authority: %v", err)
	}
	path, err := registry.descriptorPath(localProjectContextName(project.contracts))
	if err != nil {
		t.Fatal(err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("predecessor same-project descriptor survived epoch reconciliation: %v", err)
	}
}

func TestServeProjectContextRegistrationReclaimsDeadProjectDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	contextName := localProjectContextName(project.contracts)
	writeCLIAPITestContext(t, registry, contextName, "runtime-dead", "http://127.0.0.1:1", project.contracts)
	if err := registry.SetCurrent(contextName); err != nil {
		t.Fatalf("set current context: %v", err)
	}
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	_, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	path, err := registry.descriptorPath(contextName)
	if err != nil {
		t.Fatalf("descriptor path: %v", err)
	}
	if _, err := os.Stat(path); !os.IsNotExist(err) {
		t.Fatalf("dead descriptor stat err = %v, want removed", err)
	}
	if current, err := registry.CurrentName(); err != nil || current != "" {
		t.Fatalf("current = %q err=%v, want cleared", current, err)
	}
}

func TestServeProjectContextRegistrationTreatsExplicitNameAsDescriptorLabel(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.contracts), "runtime-live", server.URL, project.contracts)
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	opts.ContextName = "second"
	opts.ContextNameSet = true

	_, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare explicit context: %v", err)
	}
}

func TestServeProjectContextRegistrationRejectsCrossProjectExplicitContextName(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	otherProject := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, "shared", "runtime-other", "http://127.0.0.1:1", otherProject.contracts)
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	opts.ContextName = "shared"
	opts.ContextNameSet = true

	_, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err == nil {
		t.Fatal("prepare explicit context returned nil error")
	}
	if !strings.Contains(err.Error(), "context shared already exists for project "+otherProject.contracts) || !strings.Contains(err.Error(), "context names are global") {
		t.Fatalf("err = %q, want cross-project name collision", err.Error())
	}
}

func TestServeProjectContextRegistrationRequiresMatchingEpochGrant(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	otherProject := writeCLIAPIProjectFixture(t)
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = t.TempDir()
	opts.SwarmDirSet = true

	paths := CLISourcePlatformSpecPaths{SourceRoot: project.contracts}
	if _, err := PrepareServeProjectContextRegistration(mustInvocationRootForTest(project.root), opts, paths, devscratch.RegistrationGrant{}); err == nil || !strings.Contains(err.Error(), "registration grant is required") {
		t.Fatalf("zero grant error = %v, want fail-closed epoch admission", err)
	}

	coordinate, err := devscratch.Resolve(otherProject.canonicalRoot)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	defer authority.AbortBeforeStoreOpen()
	grant, err := authority.RegistrationGrant()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PrepareServeProjectContextRegistration(mustInvocationRootForTest(project.root), opts, paths, grant); err == nil || !strings.Contains(err.Error(), "belongs to another canonical project") {
		t.Fatalf("foreign grant error = %v, want project-bound epoch admission", err)
	}
}

func TestServeProjectContextRegistrationRejectsUnsafeAuthDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	reg, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	err = reg.WriteFinal("runtime-1", listener.Addr(), apiv1.AuthTokenResolution{
		Tokens:   []string{"secret"},
		Source:   apiv1.AuthTokenSource("explicit-without-token-file"),
		Explicit: true,
	}, CLISourcePlatformSpecPaths{SourceRoot: project.contracts}, storebackend.Selection{Backend: storebackend.BackendSQLite}, WorkspaceMountSources{})
	if err == nil || !strings.Contains(err.Error(), "requires token-file auth") {
		t.Fatalf("WriteFinal err = %v, want safe-auth rejection", err)
	}
}

func TestServeProjectContextRegistrationWritesTokenFileAuthDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := DefaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	reg, err := prepareServeProjectContextRegistrationForTest(t, project, opts)
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	tokenFile := writeCLIAPITokenFile(t, "serve-secret")
	err = reg.WriteFinal("runtime-1", listener.Addr(), apiv1.AuthTokenResolution{
		Tokens:    []string{"serve-secret"},
		Source:    apiv1.AuthTokenSource(serveAPITokenFileFlagSource),
		Explicit:  true,
		TokenFile: tokenFile,
	}, CLISourcePlatformSpecPaths{SourceRoot: project.contracts}, storebackend.Selection{Backend: storebackend.BackendSQLite}, WorkspaceMountSources{})
	if err != nil {
		t.Fatalf("WriteFinal: %v", err)
	}

	registry := newLocalContextRegistry(swarmDir)
	entry, err := registry.ReadDescriptor(localProjectContextName(project.contracts))
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	desc := entry.Descriptor
	if desc.Auth.Mode != localContextAuthTokenFile || desc.Auth.TokenFile != tokenFile {
		t.Fatalf("descriptor auth = %#v, want token_file %q", desc.Auth, tokenFile)
	}
	rpcEndpoint, err := cliAPIRPCEndpointFromServer(desc.APIServer, "descriptor api_server")
	if err != nil {
		t.Fatalf("rpc endpoint: %v", err)
	}
	token, err := localContextDescriptorToken(desc, rpcEndpoint)
	if err != nil {
		t.Fatalf("descriptor token: %v", err)
	}
	if token != "serve-secret" {
		t.Fatalf("descriptor token = %q, want serve-secret", token)
	}
}

func prepareServeProjectContextRegistrationForTest(t *testing.T, project cliAPITestProject, opts ServeOptions) (*ServeProjectContextRegistration, error) {
	t.Helper()
	coordinate, err := devscratch.Resolve(project.contracts)
	if err != nil {
		t.Fatal(err)
	}
	authority, err := devscratch.Acquire(coordinate)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = authority.AbortBeforeStoreOpen() })
	grant, err := authority.RegistrationGrant()
	if err != nil {
		t.Fatal(err)
	}
	return PrepareServeProjectContextRegistration(mustInvocationRootForTest(project.root), opts, CLISourcePlatformSpecPaths{SourceRoot: project.contracts}, grant)
}

func listenLoopbackTestListener(t *testing.T) net.Listener {
	t.Helper()
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("listen: %v", err)
	}
	return listener
}

func defaultLoopbackAuthResolution() apiv1.AuthTokenResolution {
	return apiv1.AuthTokenResolution{
		Tokens: []string{apiv1.DefaultLoopbackAPIToken},
		Source: apiv1.AuthTokenSourceBuiltInLoopbackToken,
	}
}
