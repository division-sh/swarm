package cliapp

import (
	"net"
	"runtime"

	"github.com/division-sh/swarm/internal/runtime/workspace"
)

type ListenerAddressSource string

const (
	ListenerAddressDefault ListenerAddressSource = "default"
	ListenerAddressConfig  ListenerAddressSource = "config"
	ListenerAddressFlag    ListenerAddressSource = "flag"
)

// An address with no recorded default provenance is explicit, including
// internal callers. Equality to a default address does not establish origin.
func WorkspaceMCPListenAddr(addr string, source ListenerAddressSource, backend WorkspaceBackendSelection) string {
	return workspaceMCPListenAddr(addr, source, backend.Backend, runtime.GOOS)
}

func workspaceMCPListenAddr(addr string, source ListenerAddressSource, backend, hostOS string) string {
	if source != ListenerAddressDefault || backend != workspace.BackendDocker || hostOS != "linux" {
		return addr
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return addr // The listener validator owns malformed-address refusal.
	}
	return net.JoinHostPort("0.0.0.0", port)
}
