//go:build linux

package main

import (
	"bufio"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"os"
	"os/exec"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"testing"
	"time"

	"github.com/division-sh/swarm/internal/cliapp"
	runtimepkg "github.com/division-sh/swarm/internal/runtime"
	"github.com/division-sh/swarm/internal/runtime/startupownership"
	"github.com/division-sh/swarm/internal/store/backendselection"
	"github.com/division-sh/swarm/internal/store/selected/selectedtest"
	"github.com/division-sh/swarm/internal/testpostgres"
	"github.com/division-sh/swarm/internal/testutil"
)

type servingExitRequest struct {
	Repo, Config, SwarmDir string
	Stop, Work             string
}

func TestServingInternalAndExternalStopKeepLeaseUntilJoinOrDeathBothStores(t *testing.T) {
	if raw := os.Getenv("AGENT_D_R53_PROCESS_REQUEST"); raw != "" {
		var request servingExitRequest
		if err := json.Unmarshal([]byte(raw), &request); err != nil {
			t.Fatal(err)
		}
		root, err := cliapp.NewInvocationRoot(request.Repo)
		if err != nil {
			t.Fatal(err)
		}
		ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGTERM)
		defer stop()
		opts := cliapp.DefaultServeOptions()
		opts.ConfigPath, opts.SourceRoot = request.Config, filepath.Join(request.Repo, "tests/tier8-boot-verification/test-boot-success")
		opts.SwarmDir, opts.SwarmDirSet = request.SwarmDir, true
		opts.APIListenAddr, opts.MCPListenAddr = "127.0.0.1:0", "127.0.0.1:0"
		opts.Output, opts.ErrorOutput = os.Stdout, os.Stderr
		opts.Verbose = true
		opts.ShutdownGrace = 2 * time.Second
		opts.TestServingStop = func(cancelChild context.CancelFunc) {
			if request.Stop == "internal" {
				go func() {
					scanner := bufio.NewScanner(os.Stdin)
					if scanner.Scan() && scanner.Text() == "stop" {
						cancelChild()
						fmt.Println("STOPPED")
					}
				}()
			} else {
				go func() {
					<-ctx.Done()
					fmt.Println("STOPPED")
				}()
			}
		}
		opts.TestAPIListenerBound = func(addr net.Addr) { fmt.Println("PROBE " + addr.String()) }
		opts.TestRuntimeReadyHook = func(rt *runtimepkg.Runtime) {
			if request.Work == "holding" {
				lease, err := rt.WorkOccurrence().Begin(context.Background())
				if err != nil {
					t.Fatal(err)
				}
				// An accepted producer deliberately never acknowledges Done. The
				// process must die without releasing possession or claiming a join.
				_ = lease
			}
			fmt.Println("SERVING")
		}
		code := runServingProcess(ctx, root, opts)
		fmt.Println("JOINED")
		os.Exit(code)
	}
	repo, err := filepath.Abs(filepath.Join("..", ".."))
	if err != nil {
		t.Fatal(err)
	}
	for _, backend := range []backendselection.Backend{backendselection.BackendSQLite, backendselection.BackendPostgres} {
		for _, work := range []string{"waiting", "holding", "graceful"} {
			for _, stop := range []string{"internal", "external_signal"} {
				t.Run(string(backend)+"/"+work+"/"+stop, func(t *testing.T) {
					dir := t.TempDir()
					scope := backendselection.Selection{Backend: backend}
					var dsn string
					var postgresWait func(context.Context) (bool, error)
					configText := "runtime:\n  recovery_on_startup: false\nllm:\n  backend: anthropic\nstore:\n  backend: " + string(backend) + "\n"
					if backend == backendselection.BackendSQLite {
						scope.SQLitePath = filepath.Join(dir, "runtime.db")
						configText += fmt.Sprintf("  sqlite:\n    path: %q\n", scope.SQLitePath)
					} else {
						var p testpostgres.Parameters
						dsn, p, postgresWait = testutil.StartEmptyPostgresProcessLeaseProof(t)
						t.Setenv("AGENT_D_R53_DB_PASSWORD", p.Password)
						configText += fmt.Sprintf("database:\n  host: %q\n  port: %d\n  name: %q\n  user: %q\n  password_env: AGENT_D_R53_DB_PASSWORD\n  sslmode: %q\n", p.Host, p.Port, p.Database, p.User, p.SSLMode)
					}
					configPath := filepath.Join(dir, "swarm.yaml")
					if err := os.WriteFile(configPath, []byte(configText), 0o600); err != nil {
						t.Fatal(err)
					}
					var holder *selectedtest.ProcessLeaseFixture
					if work == "waiting" {
						holder, err = selectedtest.OpenProcessLeaseFixture(context.Background(), scope, dsn)
						if err != nil {
							t.Fatal(err)
						}
						defer func() {
							if err := holder.Close(); err != nil {
								t.Error(err)
							}
						}()
					}
					childRequest, err := json.Marshal(servingExitRequest{Repo: repo, Config: configPath, SwarmDir: filepath.Join(dir, "swarm"), Stop: stop, Work: work})
					if err != nil {
						t.Fatal(err)
					}
					deadline, cancel := context.WithTimeout(context.Background(), 30*time.Second)
					defer cancel()
					cmd := exec.CommandContext(deadline, os.Args[0], "-test.run=^TestServingInternalAndExternalStopKeepLeaseUntilJoinOrDeathBothStores$")
					cmd.Env = append(os.Environ(), "GOMAXPROCS=2", "AGENT_D_R53_PROCESS_REQUEST="+string(childRequest))
					logPath := filepath.Join(dir, "process.log")
					output, err := os.Create(logPath)
					if err != nil {
						t.Fatal(err)
					}
					defer output.Close()
					cmd.Stdout, cmd.Stderr = output, output
					input, err := cmd.StdinPipe()
					if err != nil {
						t.Fatal(err)
					}
					if err := cmd.Start(); err != nil {
						t.Fatal(err)
					}
					ended := make(chan error, 1)
					observedEnd := false
					go func() { ended <- cmd.Wait() }()
					defer func() {
						_ = input.Close()
						cancel()
						if !observedEnd {
							<-ended
						}
					}()
					readOutput := func() string {
						body, err := os.ReadFile(logPath)
						if err != nil {
							t.Fatal(err)
						}
						return string(body)
					}
					awaitOutput := func(marker string) string {
						for {
							body := readOutput()
							if strings.Contains(body, marker) {
								return body
							}
							select {
							case childErr := <-ended:
								observedEnd = true
								t.Fatalf("serving did not reach %q: %v: %s", marker, childErr, readOutput())
							case <-deadline.Done():
								t.Fatalf("serving did not reach %q: %s", marker, readOutput())
							case <-time.After(5 * time.Millisecond):
							}
						}
					}
					marker := "SERVING\n"
					if work == "waiting" {
						marker = "PROBE "
					}
					transcript := awaitOutput(marker)
					var probeAddr string
					for _, line := range strings.Split(transcript, "\n") {
						if strings.HasPrefix(line, "PROBE ") {
							probeAddr = strings.TrimPrefix(line, "PROBE ")
						}
					}
					proofDeadline := time.Now().Add(5 * time.Second)
					for {
						proved := false
						if work == "waiting" {
							if backend == backendselection.BackendSQLite {
								locks, err := os.ReadFile("/proc/locks")
								if err != nil {
									t.Fatal(err)
								}
								for _, line := range strings.Split(string(locks), "\n") {
									fields := strings.Fields(line)
									proved = proved || (len(fields) > 5 && fields[1] == "->" && fields[2] == "FLOCK" && fields[5] == strconv.Itoa(cmd.Process.Pid))
								}
							} else {
								var err error
								proved, err = postgresWait(deadline)
								if err != nil {
									t.Fatal(err)
								}
							}
						} else {
							client := &http.Client{Timeout: time.Second}
							response, err := client.Get("http://" + probeAddr + "/readyz")
							if err == nil {
								proved = response.StatusCode == http.StatusOK
								_ = response.Body.Close()
							}
						}
						if proved {
							break
						}
						if time.Now().After(proofDeadline) {
							t.Fatalf("native wait/readiness was not observed before stop: %s", readOutput())
						}
						time.Sleep(5 * time.Millisecond)
					}
					inspection, err := selectedtest.OpenProcessLeaseInspection(deadline, scope, dsn)
					if err != nil {
						t.Fatal(err)
					}
					defer func() {
						if err := inspection.Close(); err != nil {
							t.Error(err)
						}
					}()
					possession, err := inspection.ProbePossession(deadline)
					busy := &startupownership.AcquisitionError{Failure: startupownership.AcquisitionTakeoverRequired, Detail: "selected store is held by another process"}
					if possession.Available || (err != nil && (backend != backendselection.BackendSQLite || err.Error() != busy.Error())) {
						t.Fatalf("live process/holder failed successor exclusion: %+v %v", possession, err)
					}
					if stop == "internal" {
						if _, err := io.WriteString(input, "stop\n"); err != nil {
							t.Fatal(err)
						}
					} else if err := cmd.Process.Signal(syscall.SIGTERM); err != nil {
						t.Fatal(err)
					}
					if work == "holding" || (work == "waiting" && backend == backendselection.BackendSQLite) {
						transcript = awaitOutput("STOPPED\n")
						if strings.Contains(transcript, "JOINED\n") {
							t.Fatal("stuck work returned before native exclusion could be proved after stop")
						}
						possession, err := inspection.ProbePossession(deadline)
						if possession.Available || (err != nil && (backend != backendselection.BackendSQLite || err.Error() != busy.Error())) {
							t.Fatalf("stop unlocked while accepted work/native wait survives: %+v %v", possession, err)
						}
					}
					var childErr error
					select {
					case childErr = <-ended:
					case <-time.After(4 * time.Second):
						t.Fatalf("serving did not exit within the 2s grace plus race/process-observation budget: %s", readOutput())
					}
					observedEnd = true
					transcript = readOutput()
					joined := strings.Contains(transcript, "JOINED\n")
					if deadline.Err() != nil {
						t.Fatalf("actual serving stop did not bound process exit: %s", transcript)
					}
					wantJoined := work == "graceful" || (work == "waiting" && backend == backendselection.BackendPostgres)
					if joined != wantJoined {
						t.Fatalf("process join=%t want=%t err=%v: %s", joined, wantJoined, childErr, transcript)
					}
					if work == "graceful" && childErr != nil {
						t.Fatalf("graceful served stop returned an error: %v: %s", childErr, transcript)
					}
					if !joined {
						var exit *exec.ExitError
						if !errors.As(childErr, &exit) || exit.ExitCode() != 1 {
							t.Fatalf("stuck process did not exit at its own deadline: %v", childErr)
						}
					}
					if holder != nil {
						possession, err := inspection.ProbePossession(deadline)
						if possession.Available || (err != nil && (backend != backendselection.BackendSQLite || err.Error() != busy.Error())) {
							t.Fatalf("waiter death stole/released incumbent possession: %+v %v", possession, err)
						}
						if err := holder.Close(); err != nil {
							t.Fatal(err)
						}
					}
					available, err := selectedtest.OpenProcessLeaseFixture(deadline, scope, dsn)
					if err != nil {
						t.Fatalf("native process disposal leaked possession: %v", err)
					}
					if err := available.Close(); err != nil {
						t.Fatal(err)
					}
				})
			}
		}
	}
}
