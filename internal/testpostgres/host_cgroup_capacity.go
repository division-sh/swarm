package testpostgres

import (
	"os"
	"path/filepath"
	"strconv"
	"strings"
)

func linuxCgroupResourceBudget(cpu int, memory uint64, readFile func(string) ([]byte, error)) (int, uint64) {
	membership, err := readFile("/proc/self/cgroup")
	if err != nil {
		return 1, 0
	}
	rows := strings.Split(strings.TrimSpace(string(membership)), "\n")
	if len(rows) != 1 || !strings.HasPrefix(rows[0], "0::/") {
		// v1, hybrid and unavailable hierarchies cannot prove spare capacity.
		return 1, 0
	}
	group := strings.TrimPrefix(rows[0], "0::")
	if filepath.Clean(group) != group || strings.Contains(group, "\\") {
		return 1, 0
	}
	mounts, err := readFile("/proc/self/mountinfo")
	if err != nil {
		return 1, 0
	}
	root := unifiedCgroupMount(string(mounts))
	if root == "" {
		return 1, 0
	}
	for current := filepath.Join(root, strings.TrimPrefix(group, "/")); ; current = filepath.Dir(current) {
		cpu, memory = cgroupDirectoryBudget(cpu, memory, current, current == root, readFile)
		if memory == 0 || current == root {
			return cpu, memory
		}
	}
}

func unifiedCgroupMount(mounts string) string {
	var root string
	for _, row := range strings.Split(mounts, "\n") {
		before, after, ok := strings.Cut(row, " - ")
		filesystem := strings.Fields(after)
		if !ok || len(filesystem) == 0 || filesystem[0] != "cgroup2" {
			continue
		}
		fields := strings.Fields(before)
		if root != "" || len(fields) < 6 || fields[3] != "/" || !filepath.IsAbs(fields[4]) || filepath.Clean(fields[4]) != fields[4] || strings.Contains(fields[4], "\\") {
			// A partial or ambiguous mount cannot expose every ancestor limit.
			return ""
		}
		root = fields[4]
	}
	return root
}

func cgroupDirectoryBudget(cpu int, memory uint64, path string, root bool, readFile func(string) ([]byte, error)) (int, uint64) {
	data, err := readFile(filepath.Join(path, "memory.max"))
	if err != nil && !(root && os.IsNotExist(err)) {
		return 1, 0
	}
	if err == nil && strings.TrimSpace(string(data)) != "max" {
		limit, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		if err != nil {
			return 1, 0
		}
		memory = min(memory, limit)
	}
	data, err = readFile(filepath.Join(path, "cpu.max"))
	if err != nil && !(root && os.IsNotExist(err)) {
		return 1, 0
	}
	if err == nil {
		cpu = constrainedHostCPU(cpu, string(data))
	}
	return cpu, memory
}
