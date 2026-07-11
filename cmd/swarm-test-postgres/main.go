package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"os"
	"os/exec"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testpostgres"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "--internal-create" {
		return runCreator(args[1:])
	}
	if len(args) > 0 && args[0] == "--" {
		args = args[1:]
	}
	if len(args) == 0 {
		fmt.Fprintln(os.Stderr, "usage: go run ./cmd/swarm-test-postgres -- <command> [args...]")
		return 2
	}
	registry, err := testpostgres.DefaultServiceRegistry()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	executable, err := os.Executable()
	if err != nil {
		fmt.Fprintf(os.Stderr, "resolve runner executable: %v\n", err)
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 3*time.Minute)
	service, err := registry.Provision(ctx, executable)
	cancel()
	if err != nil {
		fmt.Fprintf(os.Stderr, "provision runner-owned Postgres: %v\n", err)
		return 1
	}
	childEnv, err := testpostgres.ChildEnvironment(os.Environ(), service.Connection)
	if err != nil {
		_ = service.Close(context.Background())
		fmt.Fprintf(os.Stderr, "build child Postgres environment: %v\n", err)
		return 1
	}
	if err := service.MarkChildRunning(); err != nil {
		_ = service.Close(context.Background())
		fmt.Fprintf(os.Stderr, "record child launch: %v\n", err)
		return 1
	}
	cmd := exec.Command(args[0], args[1:]...)
	cmd.Env = childEnv
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := cmd.Start(); err != nil {
		_ = service.Close(context.Background())
		fmt.Fprintf(os.Stderr, "start child command: %v\n", err)
		return 1
	}
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	done := make(chan struct{})
	go func() {
		select {
		case sig := <-signals:
			_ = cmd.Process.Signal(sig)
		case <-done:
		}
	}()
	waitErr := cmd.Wait()
	close(done)
	signal.Stop(signals)
	cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
	cleanupErr := service.Close(cleanupCtx)
	cleanupCancel()
	if cleanupErr != nil {
		fmt.Fprintf(os.Stderr, "remove runner-owned Postgres: %v (child result: %v)\n", cleanupErr, waitErr)
		return 1
	}
	if waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return exitErr.ExitCode()
	}
	fmt.Fprintf(os.Stderr, "wait for child command: %v\n", waitErr)
	return 1
}

func runCreator(args []string) int {
	flags := flag.NewFlagSet("internal-create", flag.ContinueOnError)
	stateRoot := flags.String("state-root", "", "private state root")
	leaseID := flags.String("lease-id", "", "service lease ID")
	creatorFD := flags.Int("creator-fd", 0, "inherited creator fence descriptor")
	if err := flags.Parse(args); err != nil || *stateRoot == "" || *leaseID == "" || *creatorFD < 3 {
		return 2
	}
	dockerBin, err := exec.LookPath("docker")
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	registry := testpostgres.NewServiceRegistry(*stateRoot, dockerBin)
	if err := registry.RunCreator(context.Background(), *leaseID, uintptr(*creatorFD)); err != nil {
		fmt.Fprintf(os.Stderr, "creator %s failed (fd %s): %v\n", *leaseID, strconv.Itoa(*creatorFD), err)
		return 1
	}
	return 0
}
