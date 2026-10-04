package workspace

import (
	"errors"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	"github.com/division-sh/swarm/internal/runtime/failures"
	"github.com/division-sh/swarm/internal/runtime/workspace/worker"
)

func TestDockerWorkerObservationUsesExactInvocation(t *testing.T) {
	argument, _ := worker.InvocationArgument("0123456789abcdef0123456789abcdef")
	other, _ := worker.InvocationArgument("ffffffffffffffffffffffffffffffff")
	for _, test := range []struct {
		name, raw string
		present   bool
		invalid   bool
	}{
		{"exact", "PID COMMAND\n42 " + WorkerContainerPath + " " + argument, true, false},
		{"sibling", "PID COMMAND\n42 " + WorkerContainerPath + " " + other, false, false},
		{"not_argument", "PID COMMAND\n42 echo " + WorkerContainerPath + " " + argument, false, false},
		{"not_worker", "PID COMMAND\n42 /bin/echo " + argument, false, false},
		{"substring", "PID COMMAND\n42 " + WorkerContainerPath + " " + argument + "more", false, false},
		{"ordinary_container", "PID COMMAND\n1 sleep infinity", false, false},
		{"empty", "", false, true},
		{"missing_header", "42 " + WorkerContainerPath + " " + argument, false, true},
		{"missing_command", "PID COMMAND\n42", false, true},
		{"corrupt_coordinate", "PID COMMAND\ninvalid sleep infinity", false, true},
	} {
		t.Run(test.name, func(t *testing.T) {
			present, err := dockerWorkerPresent([]byte(test.raw), argument)
			if present != test.present || (err != nil) != test.invalid {
				t.Fatalf("observation: present=%t err=%v", present, err)
			}
		})
	}
}

func TestDockerWorkerCleanupFailureRemainsUnproven(t *testing.T) {
	argument, _ := worker.InvocationArgument("0123456789abcdef0123456789abcdef")
	if err := observeDockerWorkerGone("", "", argument, false); err == nil {
		t.Fatal("an unacknowledged launch was declared joined")
	}
	binary := filepath.Join(t.TempDir(), "failed-process-observation")
	if err := os.WriteFile(binary, []byte("#!/bin/sh\nexit 73\n"), 0o700); err != nil {
		t.Fatal(err)
	}
	err := observeDockerWorkerGone(binary, "immutable-container", argument, true)
	var exit *exec.ExitError
	if !errors.As(err, &exit) || exit.ExitCode() != 73 || failures.FromError(err, "test", "join").Failure.Detail.Code != "workspace_worker_cleanup_unproven" {
		t.Fatalf("cleanup error was hidden or misclassified: %v", err)
	}
}
