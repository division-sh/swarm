//go:build darwin || linux

package main

import (
	"fmt"

	"golang.org/x/sys/unix"
)

func checkCompletionScratchSpace(path string, capacity int) error {
	var state unix.Statfs_t
	if err := unix.Statfs(path, &state); err != nil {
		return fmt.Errorf("observe test scratch filesystem: %w", err)
	}
	available := uint64(state.Bavail) * uint64(state.Bsize)
	return completionScratchBudget(available, uint64(state.Ffree), capacity)
}

func completionScratchBudget(available, inodes uint64, capacity int) error {
	if capacity < 1 || available < uint64(capacity)*(2<<30) || inodes < uint64(capacity)*10000 {
		return fmt.Errorf("test scratch preflight: %d bytes/%d inodes free; need at least 2GiB/10000 inodes per planned worker (%d)", available, inodes, capacity)
	}
	return nil
}
