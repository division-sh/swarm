package cliapp

import "testing"

func TestWorkspaceMCPListenerChangesOnlyRecordedLinuxDockerDefault(t *testing.T) {
	for _, tc := range []struct {
		name, addr, backend, os, want string
		source                        ListenerAddressSource
	}{
		{"linux_docker_default", "127.0.0.1:8082", "docker", "linux", "0.0.0.0:8082", ListenerAddressDefault},
		{"linux_docker_private", "127.0.0.1:0", "docker", "linux", "0.0.0.0:0", ListenerAddressDefault},
		{"host", "127.0.0.1:8082", "host", "linux", "127.0.0.1:8082", ListenerAddressDefault},
		{"desktop", "127.0.0.1:8082", "docker", "darwin", "127.0.0.1:8082", ListenerAddressDefault},
		{"explicit_equal_default", "127.0.0.1:8082", "docker", "linux", "127.0.0.1:8082", ListenerAddressFlag},
		{"config_equal_default", "127.0.0.1:8082", "docker", "linux", "127.0.0.1:8082", ListenerAddressConfig},
		{"no_provenance", "127.0.0.1:8082", "docker", "linux", "127.0.0.1:8082", ""},
		{"explicit_ipv6", "[::1]:8082", "docker", "linux", "[::1]:8082", ListenerAddressFlag},
		{"explicit_network", "172.18.0.1:8082", "docker", "linux", "172.18.0.1:8082", ListenerAddressConfig},
		{"explicit_wildcard", "0.0.0.0:8082", "docker", "linux", "0.0.0.0:8082", ListenerAddressFlag},
		{"invalid", "invalid", "docker", "linux", "invalid", ListenerAddressDefault},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if got := workspaceMCPListenAddr(tc.addr, tc.source, tc.backend, tc.os); got != tc.want {
				t.Fatalf("listener = %q, want %q", got, tc.want)
			}
		})
	}
}
