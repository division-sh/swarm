package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"time"

	"github.com/division-sh/swarm/internal/testplanning"
)

type completionWorkerReceipt struct {
	UnitID    string    `json:"unit_id"`
	Status    string    `json:"status"`
	StartedAt time.Time `json:"started_at,omitempty"`
	EndedAt   time.Time `json:"ended_at,omitempty"`
	ExitCode  int       `json:"exit_code"`
}

type completionWorkersReceipt struct {
	HeadSHA   string                    `json:"head_sha"`
	Plan      string                    `json:"plan_digest"`
	Capacity  int                       `json:"host_capacity"`
	StartedAt time.Time                 `json:"started_at"`
	EndedAt   time.Time                 `json:"ended_at"`
	Workers   []completionWorkerReceipt `json:"workers"`
}

func executeCompletionWorkers(ctx context.Context, plan testplanning.RunPlan, receipts, executable string, env []string, explicit bool, capacity int) int {
	if capacity < 1 {
		fmt.Fprintln(os.Stderr, "completion capacity must be positive")
		return 1
	}
	ctx, cancel := context.WithCancel(ctx)
	defer cancel()
	report := completionWorkersReceipt{HeadSHA: plan.HeadSHA, Plan: plan.Digest, Capacity: capacity, StartedAt: time.Now().UTC()}
	for _, unit := range plan.Units {
		report.Workers = append(report.Workers, completionWorkerReceipt{UnitID: unit.ID, Status: "not_started", ExitCode: -1})
	}
	type result struct{ index, code int }
	results := make(chan result, capacity)
	next, active, code := 0, 0, 0
	order := completionExecutionOrder(plan)
	for next < len(plan.Units) || active != 0 {
		for next < len(plan.Units) && active < capacity && ctx.Err() == nil {
			index := order[next]
			next++
			active++
			report.Workers[index].Status = "started"
			report.Workers[index].StartedAt = time.Now().UTC()
			fmt.Fprintf(os.Stderr, "swarm-test %s: starting %s (capacity %d)\n", plan.Profile, plan.Units[index].ID, capacity)
			go func() {
				results <- result{index: index, code: runCompletionWorker(ctx, executable, env, filepath.Join(receipts, "proof-plan.json"), plan.Units[index].ID, receipts, explicit)}
			}()
		}
		if active == 0 {
			break
		}
		finished := <-results
		active--
		row := &report.Workers[finished.index]
		row.EndedAt, row.ExitCode = time.Now().UTC(), finished.code
		row.Status = "passed"
		if finished.code != 0 {
			row.Status = "failed_or_interrupted"
			if code == 0 {
				code = finished.code
			}
			cancel()
		}
	}
	// Every started worker has returned from its existing descendant/service join.
	// A canceled sibling or an unstarted unit never earns passing evidence.
	if ctx.Err() != nil && code == 0 {
		code = 1
	}
	report.EndedAt = time.Now().UTC()
	raw, err := json.MarshalIndent(report, "", "  ")
	if err == nil {
		err = os.WriteFile(filepath.Join(receipts, "workers.json"), raw, 0600)
	}
	if err != nil {
		fmt.Fprintf(os.Stderr, "retain completion worker receipt: %v\n", err)
		return 1
	}
	return code
}

func completionExecutionOrder(plan testplanning.RunPlan) []int {
	order := make([]int, len(plan.Units))
	for i := range order {
		order[i] = i
	}
	sort.SliceStable(order, func(i, j int) bool {
		return modeledDuration(plan.Units[order[i]]) < modeledDuration(plan.Units[order[j]])
	})
	return order
}

func runCompletionWorker(ctx context.Context, executable string, env []string, planFile, unit, receipts string, explicit bool) int {
	log, err := os.Create(filepath.Join(receipts, unit+"-runner.log"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		return 1
	}
	defer log.Close()
	args := []string{"--planned", planFile, unit, "--receipts", receipts}
	if !explicit {
		args = append(args, "--feedback")
	}
	cmd := exec.Command(executable, args...)
	cmd.Env = env
	cmd.Stdout, cmd.Stderr = io.MultiWriter(os.Stdout, log), io.MultiWriter(os.Stderr, log)
	if err := cmd.Start(); err != nil {
		fmt.Fprintf(os.Stderr, "start planned worker %s: %v\n", unit, err)
		return 1
	}
	joined := make(chan struct{})
	stop := make(chan struct{})
	defer func() {
		close(stop)
		<-joined
	}()
	go func() {
		defer close(joined)
		select {
		case <-ctx.Done():
			// The worker owns forwarding, cancellation and joining its process tree.
			_ = cmd.Process.Signal(os.Interrupt)
		case <-stop:
		}
	}()
	if err := cmd.Wait(); err != nil {
		if failure, ok := err.(*exec.ExitError); ok && failure.ExitCode() > 0 {
			return failure.ExitCode()
		}
		fmt.Fprintf(os.Stderr, "planned worker %s: %v\n", unit, err)
		return 1
	}
	return 0
}
