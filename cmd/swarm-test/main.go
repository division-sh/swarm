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
	"sync"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

func main() {
	os.Exit(run(os.Args[1:]))
}

func run(args []string) int {
	if len(args) > 0 && args[0] == "--internal-create" {
		return runCreator(args[1:])
	}
	testArgs, err := parseTestArgs(args)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 2
	}
	command := append([]string{"go", "test"}, testArgs...)
	capacity, err := testpostgres.RunCapacityFromEnvironment()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	admission, err := testpostgres.DefaultRunAdmission(os.Stderr)
	if err != nil {
		fmt.Fprintf(os.Stderr, "configure test run admission: %v\n", err)
		return 1
	}

	queueCtx, cancelQueue := context.WithCancel(context.Background())
	defer cancelQueue()
	signals := make(chan os.Signal, 2)
	forwardSignals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	stopSignalRelay := make(chan struct{})
	var signalRelay sync.WaitGroup
	signalRelay.Add(1)
	defer func() {
		close(stopSignalRelay)
		signal.Stop(signals)
		signalRelay.Wait()
	}()
	go func() {
		defer signalRelay.Done()
		for {
			select {
			case sig := <-signals:
				cancelQueue()
				select {
				case forwardSignals <- sig:
				default:
				}
			case <-stopSignalRelay:
				return
			}
		}
	}()

	lease, err := admission.Acquire(queueCtx, testpostgres.RunCommand{
		Args: command, FallbackDuration: timingFallback(testArgs),
	}, capacity)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, context.Canceled) {
			return 130
		}
		return 1
	}
	completeLease := func(success bool) int {
		if err := lease.Complete(success); err != nil {
			fmt.Fprintf(os.Stderr, "release test slot: %v\n", err)
			return 1
		}
		return 0
	}
	if err := queueCtx.Err(); err != nil {
		if code := completeLease(false); code != 0 {
			return code
		}
		return 130
	}

	connection, explicit, err := testpostgres.ConnectionFromEnvironmentIfSet()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		_ = lease.Complete(false)
		return 1
	}
	var service *testpostgres.Service
	if !explicit {
		registry, err := testpostgres.DefaultServiceRegistry()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			_ = lease.Complete(false)
			return 1
		}
		executable, err := os.Executable()
		if err != nil {
			fmt.Fprintf(os.Stderr, "resolve runner executable: %v\n", err)
			_ = lease.Complete(false)
			return 1
		}
		provisionCtx, cancelProvision := context.WithTimeout(queueCtx, 3*time.Minute)
		service, err = registry.Provision(provisionCtx, executable)
		cancelProvision()
		if err != nil {
			fmt.Fprintf(os.Stderr, "provision runner-owned Postgres: %v\n", err)
			_ = lease.Complete(false)
			return 1
		}
		connection = service.Connection
	}

	closeService := func() error {
		if service == nil {
			return nil
		}
		cleanupCtx, cleanupCancel := context.WithTimeout(context.Background(), 30*time.Second)
		defer cleanupCancel()
		return service.Close(cleanupCtx)
	}
	failBeforeStart := func(message string, err error) int {
		cleanupErr := closeService()
		_ = lease.Complete(false)
		if cleanupErr != nil {
			fmt.Fprintf(os.Stderr, "%s: %v (Postgres cleanup: %v)\n", message, err, cleanupErr)
		} else {
			fmt.Fprintf(os.Stderr, "%s: %v\n", message, err)
		}
		return 1
	}

	childEnv, err := testpostgres.ChildEnvironment(os.Environ(), connection)
	if err != nil {
		return failBeforeStart("build child Postgres environment", err)
	}
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = childEnv
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := lease.InheritTo(cmd); err != nil {
		return failBeforeStart("attach test slot to child", err)
	}
	if service != nil {
		if err := service.InheritLeaseTo(cmd); err != nil {
			return failBeforeStart("attach Postgres service lease to child", err)
		}
		if err := service.MarkChildRunning(); err != nil {
			return failBeforeStart("record child launch", err)
		}
	}
	if err := queueCtx.Err(); err != nil {
		return failBeforeStart("start go test", err)
	}
	if err := cmd.Start(); err != nil {
		return failBeforeStart("start go test", err)
	}

	done := make(chan struct{})
	forwardingDone := make(chan struct{})
	go func() {
		defer close(forwardingDone)
		for {
			select {
			case sig := <-forwardSignals:
				_ = cmd.Process.Signal(sig)
			case <-done:
				return
			}
		}
	}()
	waitErr := cmd.Wait()
	close(done)
	<-forwardingDone
	cleanupErr := closeService()
	success := waitErr == nil && cleanupErr == nil
	leaseErr := lease.Complete(success)
	if cleanupErr != nil {
		fmt.Fprintf(os.Stderr, "remove runner-owned Postgres: %v (child result: %v)\n", cleanupErr, waitErr)
		return 1
	}
	if leaseErr != nil {
		fmt.Fprintf(os.Stderr, "release test slot: %v (child result: %v)\n", leaseErr, waitErr)
		return 1
	}
	if waitErr == nil {
		return 0
	}
	var exitErr *exec.ExitError
	if errors.As(waitErr, &exitErr) {
		return exitErr.ExitCode()
	}
	fmt.Fprintf(os.Stderr, "wait for go test: %v\n", waitErr)
	return 1
}

func parseTestArgs(args []string) ([]string, error) {
	if len(args) == 0 {
		return []string{"./..."}, nil
	}
	if args[0] != "--" || len(args) == 1 {
		return nil, fmt.Errorf("usage: go run ./cmd/swarm-test [-- <go test args...>]")
	}
	return append([]string(nil), args[1:]...), nil
}

func timingFallback(testArgs []string) time.Duration {
	fullSuite := false
	for _, arg := range testArgs {
		if arg == "./..." {
			fullSuite = true
			break
		}
	}
	if !fullSuite {
		return 0
	}
	file, err := os.Open(testplanning.GeneratedWeightModelPath)
	if err != nil {
		return 0
	}
	defer file.Close()
	model, err := testplanning.LoadWeightModel(file)
	if err != nil {
		return 0
	}
	var criticalPath float64
	for _, seconds := range model.Packages {
		if seconds > criticalPath {
			criticalPath = seconds
		}
	}
	return time.Duration(criticalPath * float64(time.Second))
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
