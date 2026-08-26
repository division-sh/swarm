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
	"sync/atomic"
	"syscall"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
	"github.com/division-sh/swarm/internal/testpostgres"
)

var authoritySettlementTimeout = 30 * time.Second

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
	var receivedSignal atomic.Int32
	signals := make(chan os.Signal, 2)
	forwardSignals := make(chan os.Signal, 2)
	signal.Notify(signals, os.Interrupt, syscall.SIGTERM)
	signalRelayStop := make(chan struct{})
	var signalRelay sync.WaitGroup
	handleSignal := func(sig os.Signal) {
		if value, ok := sig.(syscall.Signal); ok {
			receivedSignal.CompareAndSwap(0, int32(value))
		}
		cancelQueue()
		select {
		case forwardSignals <- sig:
		default:
		}
	}
	signalRelay.Add(1)
	var stopSignalRelayOnce sync.Once
	stopSignalRelay := func() {
		stopSignalRelayOnce.Do(func() {
			signal.Stop(signals)
			close(signalRelayStop)
			signalRelay.Wait()
		})
	}
	defer stopSignalRelay()
	go func() {
		defer signalRelay.Done()
		for {
			select {
			case sig := <-signals:
				handleSignal(sig)
			case <-signalRelayStop:
				// signal.Stop guarantees no new sends. Drain signals already
				// accepted by os/signal before freezing result publication.
				for {
					select {
					case sig := <-signals:
						handleSignal(sig)
					default:
						return
					}
				}
			}
		}
	}()

	lease, err := admission.Acquire(queueCtx, testpostgres.RunCommand{
		Args: command, FallbackDuration: timingFallback(testArgs),
	}, capacity)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		if errors.Is(err, context.Canceled) {
			return receivedSignalExitCode(receivedSignal.Load())
		}
		return 1
	}
	var service *testpostgres.Service
	settle := func(success bool, beforeComplete func()) (serviceErr, leaseErr error) {
		settlementCtx, cancelSettlement := context.WithTimeout(context.Background(), authoritySettlementTimeout)
		defer cancelSettlement()
		leaseErr = lease.Join(settlementCtx)
		if service != nil {
			serviceErr = service.Close(settlementCtx)
		}
		if beforeComplete != nil {
			beforeComplete()
		}
		if leaseErr == nil {
			leaseErr = lease.Complete(settlementCtx, success && serviceErr == nil && receivedSignal.Load() == 0)
		}
		return serviceErr, leaseErr
	}
	completeLease := func(success bool) int {
		serviceErr, leaseErr := settle(success, nil)
		if serviceErr != nil {
			fmt.Fprintf(os.Stderr, "remove runner-owned Postgres: %v\n", serviceErr)
			return 1
		}
		if leaseErr != nil {
			fmt.Fprintf(os.Stderr, "release test slot: %v\n", leaseErr)
			return 1
		}
		return 0
	}
	if err := queueCtx.Err(); err != nil {
		if code := completeLease(false); code != 0 {
			return code
		}
		return receivedSignalExitCode(receivedSignal.Load())
	}

	connection, explicit, err := testpostgres.ConnectionFromEnvironmentIfSet()
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		_, _ = settle(false, nil)
		return 1
	}
	if !explicit {
		registry, err := testpostgres.DefaultServiceRegistry()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			_, _ = settle(false, nil)
			return 1
		}
		executable, err := os.Executable()
		if err != nil {
			fmt.Fprintf(os.Stderr, "resolve runner executable: %v\n", err)
			_, _ = settle(false, nil)
			return 1
		}
		provisionCtx, cancelProvision := context.WithTimeout(queueCtx, 3*time.Minute)
		service, err = registry.Provision(provisionCtx, executable)
		cancelProvision()
		if err != nil {
			fmt.Fprintf(os.Stderr, "provision runner-owned Postgres: %v\n", err)
			_, _ = settle(false, nil)
			return 1
		}
		connection = service.Connection
	}

	failBeforeStart := func(message string, err error) int {
		serviceErr, leaseErr := settle(false, nil)
		settlementErr := errors.Join(serviceErr, leaseErr)
		if settlementErr != nil {
			fmt.Fprintf(os.Stderr, "%s: %v (test settlement: %v)\n", message, err, settlementErr)
			return 1
		} else {
			fmt.Fprintf(os.Stderr, "%s: %v\n", message, err)
		}
		if value := receivedSignal.Load(); value != 0 {
			return receivedSignalExitCode(value)
		}
		return 1
	}

	childEnv, err := testpostgres.ChildEnvironment(os.Environ(), connection)
	if err != nil {
		return failBeforeStart("build child Postgres environment", err)
	}
	childEnv = append(childEnv, testpostgres.RunWrapperEnv+"=1")
	cmd := exec.Command(command[0], command[1:]...)
	cmd.Env = childEnv
	cmd.Stdin = os.Stdin
	cmd.Stdout = os.Stdout
	cmd.Stderr = os.Stderr
	if err := prepareChildProcessTree(cmd); err != nil {
		return failBeforeStart("prepare test process tree", err)
	}
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

	forwardingDone := make(chan struct{})
	go func() {
		defer close(forwardingDone)
		for sig := range forwardSignals {
			if err := signalChildProcessTree(cmd, sig); err != nil && !errors.Is(err, os.ErrProcessDone) {
				fmt.Fprintf(os.Stderr, "signal test process tree: %v\n", err)
			}
		}
	}()
	waitErr := cmd.Wait()
	serviceErr, leaseErr := settle(waitErr == nil, func() {
		stopSignalRelay()
		close(forwardSignals)
		<-forwardingDone
	})
	if serviceErr != nil {
		fmt.Fprintf(os.Stderr, "remove runner-owned Postgres: %v (child result: %v)\n", serviceErr, waitErr)
		return 1
	}
	if leaseErr != nil {
		fmt.Fprintf(os.Stderr, "release test slot: %v (child result: %v)\n", leaseErr, waitErr)
		return 1
	}
	if value := receivedSignal.Load(); value != 0 {
		return receivedSignalExitCode(value)
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

func receivedSignalExitCode(value int32) int {
	if value <= 0 {
		return 1
	}
	return 128 + int(value)
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
