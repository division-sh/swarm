//go:build aix || darwin || dragonfly || freebsd || hurd || illumos || linux || netbsd || openbsd || solaris

package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"os"
	"os/exec"
	"syscall"
	"time"
)

// The extra descriptor follows launched work even when its stdio is redirected.
// EOF, not the process-group leader's exit, proves its lifetime has ended.
func runPreflightProcess(ctx context.Context, args []string, out, stderr io.Writer) (preflightProcessTiming, error) {
	var timing preflightProcessTiming
	if len(args) == 0 {
		return timing, fmt.Errorf("preflight command is required")
	}
	started := time.Now()
	command := exec.CommandContext(ctx, args[0], args[1:]...)
	command.Stdout, command.Stderr = out, stderr
	if err := prepareChildProcessTree(command); err != nil {
		return timing, err
	}
	reader, writer, err := os.Pipe()
	if err != nil {
		return timing, err
	}
	command.ExtraFiles = append(command.ExtraFiles, writer)
	command.Cancel = func() error { return signalChildProcessTree(command, syscall.SIGTERM) }
	startErr := command.Start()
	dropErr := writer.Close()
	var waitErr error
	if startErr == nil {
		waitErr = command.Wait()
	}
	timing.ExecutionSeconds = time.Since(started).Seconds()
	cleanup := time.Now()
	settlement, cancel := context.WithTimeout(context.Background(), authoritySettlementTimeout)
	defer cancel()
	joinErr := joinPreflightDescendants(settlement, reader)
	timing.CleanupSeconds = time.Since(cleanup).Seconds()
	return timing, errors.Join(startErr, dropErr, waitErr, joinErr, ctx.Err())
}

func joinPreflightDescendants(ctx context.Context, reader *os.File) error {
	joined := make(chan error, 1)
	go func() { _, err := io.Copy(io.Discard, reader); joined <- err }()
	select {
	case err := <-joined:
		return errors.Join(err, reader.Close())
	case <-ctx.Done():
		closeErr := reader.Close()
		return errors.Join(fmt.Errorf("preflight descendant lifetime unresolved: %w", ctx.Err()), closeErr, <-joined)
	}
}
