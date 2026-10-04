package workspace

import (
	"context"
	"errors"
	"os"
	"path/filepath"
	"slices"
	"strings"
	"testing"
	"time"
)

const admissionProbeTestImageID = "sha256:aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa"
const admissionProbeTestContainerID = "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb"

func TestCLIImageAdmissionProbeRestrictionsAndCleanup(t *testing.T) {
	for _, outcome := range []string{"success", "create_failed", "start_failed", "canceled", "deadline", "malformed_state", "failed_state", "cleanup_failed"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithCancel(context.Background())
			defer cancel()
			manager := NewDockerManager()
			owned, started, removed := false, false, false
			var createArgs []string
			var cleanupContexts []error
			manager.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
				switch args[0] {
				case "image":
					return "null", nil
				case "create":
					createArgs = slices.Clone(args)
					owned = true // A failed acknowledgement may still have created it.
					if outcome == "create_failed" {
						return "", errors.New("create acknowledgement unavailable")
					}
					return admissionProbeTestContainerID, nil
				case "start":
					started = true
					switch outcome {
					case "start_failed":
						return "", errors.New("version unavailable")
					case "canceled":
						cancel()
						return "", ctx.Err()
					case "deadline":
						return "", context.DeadlineExceeded
					}
					return "configured-cli 1.0", nil
				case "inspect":
					if outcome == "malformed_state" {
						return `{}`, nil
					}
					if outcome == "failed_state" {
						return `{"Status":"exited","Running":false,"ExitCode":7,"OOMKilled":false}`, nil
					}
					return `{"Status":"exited","Running":false,"ExitCode":0,"OOMKilled":false}`, nil
				case "container":
					cleanupContexts = append(cleanupContexts, ctx.Err())
					if outcome == "cleanup_failed" {
						return "", errors.New("daemon unavailable during cleanup")
					}
					if owned {
						return `"` + admissionProbeTestContainerID + `"`, nil
					}
					return "", nil
				case "rm":
					if !owned || !slices.Equal(args, []string{"rm", "--force", admissionProbeTestContainerID}) {
						t.Fatalf("removing a container without exact owned identity: %v", args)
					}
					removed, owned = true, false
					return "", nil
				default:
					t.Fatalf("unexpected preparation or command: %v", args)
					return "", nil
				}
			})
			err := manager.ProbeWorkspaceCLICommand(ctx, admissionProbeTestImageID, "/usr/bin/env")
			if (err == nil) != (outcome == "success") {
				t.Fatalf("outcome=%s err=%v", outcome, err)
			}
			if outcome == "canceled" && !errors.Is(err, context.Canceled) {
				t.Fatalf("cancellation lost: %v", err)
			}
			if outcome == "cleanup_failed" {
				if !owned || removed || !strings.Contains(err.Error(), "removal unconfirmed") {
					t.Fatalf("failed cleanup was presented as removal: owned=%v removed=%v err=%v", owned, removed, err)
				}
			} else if owned || !removed || len(cleanupContexts) != 2 {
				t.Fatalf("cleanup incomplete: owned=%v removed=%v observations=%v", owned, removed, cleanupContexts)
			}
			for _, ctxErr := range cleanupContexts {
				if ctxErr != nil {
					t.Fatalf("caller cancellation leaked into cleanup: %v", ctxErr)
				}
			}
			if started == (outcome == "create_failed") {
				t.Fatalf("incorrect launch chronology: started=%v", started)
			}
			for _, required := range [][]string{
				{"--pull=never"}, {"--read-only"}, {"--network", "none"}, {"--cap-drop", "ALL"}, {"--no-healthcheck"}, {"--log-driver", "none"},
				{"--security-opt", "no-new-privileges:true"}, {"--memory", "256m"}, {"--cpus", "1"}, {"--pids-limit", "64"},
				{"--entrypoint", "/usr/bin/env", admissionProbeTestImageID, "--version"},
			} {
				if !strings.Contains(strings.Join(createArgs, "\x00"), strings.Join(required, "\x00")) {
					t.Fatalf("missing restriction %v in %v", required, createArgs)
				}
			}
			for _, forbidden := range []string{"run", "-v", "--volume", "--mount", "-p", "--publish", "-e", "--env", "--privileged", "sh", "-lc"} {
				if slices.Contains(createArgs, forbidden) {
					t.Fatalf("unsafe probe argument %q: %v", forbidden, createArgs)
				}
			}
		})
	}
}

func TestCLIImageAdmissionProbeRejectsUnobservedIdentityBeforeEffects(t *testing.T) {
	manager := NewDockerManager()
	manager.SetRunDockerFnForTest(func(context.Context, ...string) (string, error) {
		t.Fatal("invalid image identity reached Docker")
		return "", nil
	})
	for _, image := range []string{"postgres:16", "sha256:nope", strings.ToUpper(admissionProbeTestImageID), " " + admissionProbeTestImageID} {
		if err := manager.ProbeWorkspaceCLICommand(context.Background(), image, "/usr/bin/env"); err == nil {
			t.Fatalf("unobserved/malformed image admitted: %s", image)
		}
	}
}

func TestVerifyCLIImageProbeLifecycleRealDocker(t *testing.T) {
	actual := NewDockerManager()
	available, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	if err := actual.InspectDockerAvailable(available); err != nil {
		t.Skipf("real Docker protocol not qualified: %v", err)
	}
	cfg := DefaultDockerConfig()
	cfg.WorkspaceImage = "golang:1.25-bookworm"
	actual.SetConfig(cfg)
	imageID, err := actual.InspectWorkspaceImage(available)
	if err != nil {
		t.Skipf("real Docker protocol requires already-existing golang:1.25-bookworm image; no pull performed: %v", err)
	}
	for _, outcome := range []string{"success", "missing_command", "cancel_after_create"} {
		t.Run(outcome, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
			defer cancel()
			probe := NewDockerManager()
			probe.SetConfig(cfg)
			var name, id string
			created, started, cancellationInjected := false, false, false
			probe.SetRunDockerFnForTest(func(ctx context.Context, args ...string) (string, error) {
				if args[0] == "create" {
					for i := range args {
						if args[i] == "--name" {
							name = args[i+1]
						}
						if args[i] == "--label" {
							id = strings.TrimPrefix(args[i+1], workspaceAdmissionProbeLabel+"=")
						}
					}
				}
				if args[0] == "start" {
					started = true
				}
				began := time.Now()
				out, err := actual.runDockerObservation(ctx, args...)
				t.Logf("Docker %s elapsed=%s error=%v", args[0], time.Since(began), err)
				if args[0] == "create" && err == nil {
					created = true
				}
				if args[0] == "create" && err == nil && outcome == "cancel_after_create" {
					cancellationInjected = true
					cancel()
				}
				return out, err
			})
			command := "/usr/bin/env"
			if outcome == "missing_command" {
				command = "/no-such-swarmed-command"
			}
			err := probe.ProbeWorkspaceCLICommand(ctx, imageID, command)
			if !created {
				t.Fatalf("required real create step did not complete; %s witness not reached: %v", outcome, err)
			}
			if started != (outcome != "cancel_after_create") {
				t.Fatalf("incorrect start chronology for %s: started=%v", outcome, started)
			}
			if (err == nil) != (outcome == "success") {
				t.Fatalf("probe outcome %s: %v", outcome, err)
			}
			if outcome == "cancel_after_create" && (!cancellationInjected || !errors.Is(err, context.Canceled)) {
				t.Fatalf("lost caller cancellation: %v", err)
			}
			if name == "" || id == "" {
				t.Fatal("probe omitted its unique cleanup identity")
			}
			readback, stop := context.WithTimeout(context.Background(), 5*time.Second)
			defer stop()
			remaining, err := actual.admissionProbeContainers(readback, name, id)
			if err != nil || len(remaining) != 0 {
				t.Fatalf("real probe container remains: %v err=%v", remaining, err)
			}
		})
	}
}

func TestCLIImageAdmissionProbeRejectsImplicitVolumesBeforeCreation(t *testing.T) {
	for _, declaration := range []string{`{"/data":{}}`, `7`, `{broken`, `[]`} {
		manager := NewDockerManager()
		manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
			if !slices.Equal(args, []string{"image", "inspect", "--format", "{{json .Config.Volumes}}", admissionProbeTestImageID}) {
				t.Fatalf("inadmissible image reached container creation/cleanup: %v", args)
			}
			return declaration, nil
		})
		if err := manager.ProbeWorkspaceCLICommand(context.Background(), admissionProbeTestImageID, "/usr/bin/env"); err == nil {
			t.Fatalf("inadmissible image volumes passed: %s", declaration)
		}
	}
}

func TestCLIImageAdmissionProbeUnknownCreationCannotConfirmRemoval(t *testing.T) {
	manager := NewDockerManager()
	var cleanupReads, starts int
	manager.SetRunDockerFnForTest(func(_ context.Context, args ...string) (string, error) {
		switch args[0] {
		case "image":
			return "null", nil
		case "create":
			return "", context.DeadlineExceeded
		case "container":
			cleanupReads++
			return "", nil // The daemon may still be completing the create.
		case "start":
			starts++
		default:
			t.Fatalf("unexpected probe operation: %v", args)
		}
		return "", nil
	})
	err := manager.ProbeWorkspaceCLICommand(context.Background(), admissionProbeTestImageID, "/usr/bin/env")
	if !errors.Is(err, context.DeadlineExceeded) || !strings.Contains(err.Error(), "removal unconfirmed") || !strings.Contains(err.Error(), "swarm-admission-probe-") {
		t.Fatalf("unknown creation was presented as settled cleanup: %v", err)
	}
	if cleanupReads != 1 || starts != 0 {
		t.Fatalf("unknown creation reached execution or false readback: reads=%d starts=%d", cleanupReads, starts)
	}
}

func TestDockerAdmissionObservationBoundsOutputAndDiscardsVersion(t *testing.T) {
	if os.PathSeparator != '/' {
		t.Skip("configured executable fixture requires a Unix shell")
	}
	command := filepath.Join(t.TempDir(), "configured-docker")
	if err := os.WriteFile(command, []byte("#!/bin/sh\nyes 'untrusted-output-secret' | head -c 131072\n"), 0700); err != nil {
		t.Fatal(err)
	}
	manager := NewDockerManager()
	cfg := DefaultDockerConfig()
	cfg.DockerBin = command
	manager.SetConfig(cfg)
	for _, operation := range []string{"inspect", "start", "rm"} {
		t.Run(operation, func(t *testing.T) {
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			out, err := manager.runDockerObservation(ctx, operation)
			if operation == "inspect" {
				if err == nil || len(out) != 64*1024 || strings.Contains(err.Error(), "untrusted-output-secret") {
					t.Fatalf("unbounded/leaked metadata: size=%d err=%v", len(out), err)
				}
			} else if out != "" || err != nil {
				t.Fatalf("unused command output retained: size=%d err=%v", len(out), err)
			}
		})
	}
}
