package main

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/division-sh/swarm/internal/apiv1"
	storebackend "github.com/division-sh/swarm/internal/store/backendselection"
)

func TestServeProjectContextRegistrationWritesFinalDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	defer reg.Release()
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	storePath := filepath.Join(t.TempDir(), "dev.db")
	if err := reg.WriteFinal("runtime-1", listener.Addr(), defaultLoopbackAuthResolution(), cliContractPlatformSpecPaths{ContractsPath: project.contracts}, storebackend.Selection{
		Backend:    storebackend.BackendSQLite,
		SQLitePath: storePath,
	}, workspaceMountSources{DataSource: filepath.Join(project.root, ".swarm", "data")}); err != nil {
		t.Fatalf("write final: %v", err)
	}

	registry := newLocalContextRegistry(swarmDir)
	entry, err := registry.ReadDescriptor(localProjectContextName(project.canonicalRoot))
	if err != nil {
		t.Fatalf("read descriptor: %v", err)
	}
	if entry.Status != localContextStatusOK {
		t.Fatalf("descriptor status = %s detail=%s", entry.Status, entry.Detail)
	}
	desc := entry.Descriptor
	if desc.RuntimeInstanceID != "runtime-1" || desc.ProjectRoot != project.canonicalRoot || desc.ContractsPath != project.contracts {
		t.Fatalf("descriptor = %#v, want runtime/project/contracts metadata", desc)
	}
	if desc.StorePath != storePath || desc.DataDir == "" || desc.Auth.Mode != localContextAuthBuiltinLoopback {
		t.Fatalf("descriptor = %#v, want store/data/builtin auth metadata", desc)
	}
	if current, err := registry.CurrentName(); err != nil || current != desc.Name {
		t.Fatalf("current = %q err=%v, want %q", current, err, desc.Name)
	}
}

func TestServeProjectContextRegistrationGuardsBareDoubleServe(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-live", server.URL, project.canonicalRoot)
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err == nil {
		reg.Release()
		t.Fatal("prepare registration returned nil error")
	}
	if !strings.Contains(err.Error(), "already has context descriptors") {
		t.Fatalf("err = %q, want double-serve guard", err.Error())
	}
}

func TestServeProjectContextRegistrationReclaimsDeadProjectDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	contextName := localProjectContextName(project.canonicalRoot)
	writeCLIAPITestContext(t, registry, contextName, "runtime-dead", "http://127.0.0.1:1", project.canonicalRoot)
	if err := registry.SetCurrent(contextName); err != nil {
		t.Fatalf("set current context: %v", err)
	}
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true

	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	defer reg.Release()
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

func TestServeProjectContextRegistrationAllowsExplicitSecondContext(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	server := startCLIAPIRuntimeIdentityServer(t, "runtime-live")
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, localProjectContextName(project.canonicalRoot), "runtime-live", server.URL, project.canonicalRoot)
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	opts.ContextName = "second"
	opts.ContextNameSet = true

	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err != nil {
		t.Fatalf("prepare explicit context: %v", err)
	}
	defer reg.Release()
}

func TestServeProjectContextRegistrationRejectsCrossProjectExplicitContextName(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	otherProject := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	registry := newLocalContextRegistry(swarmDir)
	writeCLIAPITestContext(t, registry, "shared", "runtime-other", "http://127.0.0.1:1", otherProject.canonicalRoot)
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	opts.ContextName = "shared"
	opts.ContextNameSet = true

	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err == nil {
		reg.Release()
		t.Fatal("prepare explicit context returned nil error")
	}
	if !strings.Contains(err.Error(), "context shared already exists for project "+otherProject.canonicalRoot) || !strings.Contains(err.Error(), "context names are global") {
		t.Fatalf("err = %q, want cross-project name collision", err.Error())
	}
}

func TestServeProjectContextRegistrationRejectsUnsafeAuthDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	defer reg.Release()
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	err = reg.WriteFinal("runtime-1", listener.Addr(), apiv1.AuthTokenResolution{
		Tokens:   []string{"secret"},
		Source:   apiv1.AuthTokenSource("explicit-without-token-file"),
		Explicit: true,
	}, cliContractPlatformSpecPaths{ContractsPath: project.contracts}, storebackend.Selection{Backend: storebackend.BackendSQLite}, workspaceMountSources{})
	if err == nil || !strings.Contains(err.Error(), "requires token-file auth") {
		t.Fatalf("WriteFinal err = %v, want safe-auth rejection", err)
	}
}

func TestServeProjectContextRegistrationWritesTokenFileAuthDescriptor(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	project := writeCLIAPIProjectFixture(t)
	swarmDir := t.TempDir()
	opts := defaultServeOptions()
	opts.Dev = true
	opts.SwarmDir = swarmDir
	opts.SwarmDirSet = true
	reg, err := prepareServeProjectContextRegistration(context.Background(), project.root, opts, cliContractPlatformSpecPaths{ContractsPath: project.contracts})
	if err != nil {
		t.Fatalf("prepare registration: %v", err)
	}
	defer reg.Release()
	listener := listenLoopbackTestListener(t)
	defer listener.Close()
	tokenFile := writeCLIAPITokenFile(t, "serve-secret")
	err = reg.WriteFinal("runtime-1", listener.Addr(), apiv1.AuthTokenResolution{
		Tokens:    []string{"serve-secret"},
		Source:    apiv1.AuthTokenSource(serveAPITokenFileFlagSource),
		Explicit:  true,
		TokenFile: tokenFile,
	}, cliContractPlatformSpecPaths{ContractsPath: project.contracts}, storebackend.Selection{Backend: storebackend.BackendSQLite}, workspaceMountSources{})
	if err != nil {
		t.Fatalf("WriteFinal: %v", err)
	}

	registry := newLocalContextRegistry(swarmDir)
	entry, err := registry.ReadDescriptor(localProjectContextName(project.canonicalRoot))
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
