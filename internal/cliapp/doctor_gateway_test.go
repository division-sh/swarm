package cliapp

import (
	"context"
	"net"
	"os"
	"path/filepath"
	"strings"
	"sync"
	"testing"

	"github.com/division-sh/swarm/internal/config"

	"github.com/division-sh/swarm/internal/runtime/failures"
)

func TestDoctorFreeGatewayProbeHostHasOnlyNetworkCredit(t *testing.T) {
	report := LocalPreflightReport{Mode: "doctor"}
	report.add(localPreflightGatewayPrerequisite, "workspace_gateway_not_probed", LocalPreflightSeverityInfo, LocalPreflightStatusSkipped, "unprobed", "")
	applyDoctorGatewayProbe(context.Background(), &report, nil, WorkspaceBackendSelection{Backend: "host"}, "127.0.0.1:0")
	if len(report.Findings) != 1 || report.Findings[0].Code != "workspace_gateway_probed" || report.Findings[0].Status != LocalPreflightStatusOK {
		t.Fatalf("report = %#v", report)
	}
	for _, text := range []string{"no model call", "tool admission", "credential validity"} {
		if !strings.Contains(report.Findings[0].Message, text) {
			t.Fatalf("missing %q in %q", text, report.Findings[0].Message)
		}
	}
}

func TestDoctorGatewayConcurrentHostProbesJoinOwnedResources(t *testing.T) {
	root := t.TempDir()
	t.Setenv("TMPDIR", root)
	var joined sync.WaitGroup
	errors := make(chan error, 4)
	for range 4 {
		joined.Add(1)
		go func() {
			defer joined.Done()
			errors <- probeDoctorGateway(context.Background(), nil, WorkspaceBackendSelection{Backend: "host"}, "127.0.0.1:0")
		}()
	}
	joined.Wait()
	close(errors)
	for err := range errors {
		if err != nil {
			t.Fatal(err)
		}
	}
	entries, err := os.ReadDir(root)
	if err != nil || len(entries) != 0 {
		t.Fatalf("joined host probes retained temporary resources: %v %v", entries, err)
	}
}

func TestDoctorGatewayPartialDockerCreationOwnsCleanupAndReportsFailure(t *testing.T) {
	for _, cut := range []string{"create", "start", "cleanup"} {
		t.Run(cut, func(t *testing.T) {
			root := t.TempDir()
			binary := filepath.Join(root, "docker")
			// This is a cleanup failure-injection control, not Docker transport
			// evidence. A real gateway listener is still owned and joined.
			script := `#!/bin/sh
set -eu
root='` + strings.ReplaceAll(root, "'", "'\"'\"'") + `'
printf '%s\n' "$*" >> "$root/calls"
case "$1" in
version) echo 25.0.0;;
network|image) exit 0;;
inspect)
  if [ -f "$root/created" ] && [ "$3" = '{{.Id}}' ]; then echo owned-doctor-object; exit 0; fi
  echo 'Error: No such object' >&2; exit 1;;
create)
  touch "$root/created"
  if [ '` + cut + `' = create ]; then echo injected-create-failure >&2; exit 1; fi;;
start) echo injected-start-failure >&2; exit 1;;
rm)
  [ "$2" = --force ] && [ "$3" = owned-doctor-object ]
  if [ '` + cut + `' = cleanup ]; then echo injected-cleanup-failure >&2; exit 1; fi
  rm "$root/created";;
*) echo unexpected-docker-command >&2; exit 2;;
esac
`
			if err := os.WriteFile(binary, []byte(script), 0o700); err != nil {
				t.Fatal(err)
			}
			cfg := &config.Config{}
			cfg.Workspace.DockerBin = binary
			err := probeDoctorGateway(context.Background(), cfg, WorkspaceBackendSelection{Backend: "docker"}, "127.0.0.1:0")
			if err == nil || !strings.Contains(err.Error(), "injected-") {
				t.Fatalf("failed partial construction was acknowledged: %v", err)
			}
			if cut == "cleanup" && !strings.Contains(err.Error(), "injected-cleanup-failure") {
				t.Fatalf("cleanup failure was erased: %v", err)
			}
			calls, readErr := os.ReadFile(filepath.Join(root, "calls"))
			if readErr != nil || strings.Count(string(calls), "rm --force owned-doctor-object") != 1 || strings.Contains(string(calls), "unexpected") {
				t.Fatalf("partial construction did not join exact cleanup: %s %v", calls, readErr)
			}
			if cut != "cleanup" {
				if _, statErr := os.Stat(filepath.Join(root, "created")); !os.IsNotExist(statErr) {
					t.Fatalf("failed doctor retained owned container: %v", statErr)
				}
			}
		})
	}
}

func TestDoctorGatewayProbeRefusesOccupiedListenerWithoutDisposingIt(t *testing.T) {
	listener, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	err = probeDoctorGateway(context.Background(), nil, WorkspaceBackendSelection{Backend: "host"}, listener.Addr().String())
	if err == nil || failures.Normalize(err, "test", "probe").Detail.Code != "workspace_gateway_unreachable" {
		t.Fatalf("probe error = %v", err)
	}
	conn, err := net.Dial("tcp", listener.Addr().String())
	if err != nil {
		t.Fatalf("probe disposed unrelated listener: %v", err)
	}
	_ = conn.Close()
}

func TestDoctorGatewayProbeCancellationRetainsCancellation(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	err := probeDoctorGateway(ctx, nil, WorkspaceBackendSelection{Backend: "host"}, "127.0.0.1:0")
	if err == nil || !strings.Contains(err.Error(), "context canceled") {
		t.Fatalf("error = %v", err)
	}
}
