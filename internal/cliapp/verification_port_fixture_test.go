package cliapp

import (
	"net"
	"testing"
)

func TestVerificationFixtureListenersAreEphemeralAndIndependent(t *testing.T) {
	isolateCLIAPIConfigEnv(t)
	addresses := map[string]bool{}
	for i := 0; i < 2; i++ {
		path := writeTestVerifyRuntimeConfig(t)
		cfg, err := LoadRuntimeConfigWithOptions(RuntimeConfigLoadOptions{RepoRoot: RepoRoot(), ExplicitPath: path})
		if err != nil {
			t.Fatal(err)
		}
		api, mcp, err := cfg.ResolveServeListeners(ServeOptions{}, false, false)
		if err != nil {
			t.Fatal(err)
		}
		for _, address := range []string{api, mcp} {
			if address != "127.0.0.1:0" {
				t.Fatalf("verification fixture inherited a production listener default: %q", address)
			}
			listener, err := net.Listen("tcp", address)
			if err != nil {
				t.Fatal(err)
			}
			t.Cleanup(func() {
				if err := listener.Close(); err != nil {
					t.Error(err)
				}
			})
			actual := listener.Addr().String()
			if addresses[actual] || listener.Addr().(*net.TCPAddr).Port == 0 {
				t.Fatalf("simultaneously live verification fixtures share a listener: %s", actual)
			}
			addresses[actual] = true
		}
	}
	if len(addresses) != 4 {
		t.Fatalf("listener evidence count=%d, want four live distinct addresses", len(addresses))
	}
}
