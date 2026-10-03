package testpostgres

import (
	"os"
	"os/exec"
	"runtime"
	"strconv"
	"strings"
)

func hostRunCapacity() int {
	cpu, memory := hostResourceBudget()
	_, shared, err := ConnectionFromEnvironmentIfSet()
	if err != nil || shared {
		// 300 is an admission floor, not measured headroom. Independent invocations
		// on a shared server must not each allocate its whole connection budget.
		return 1
	}
	return conservativeRunCapacity(cpu, memory)
}

func conservativeRunCapacity(cpu int, memory uint64) int {
	const perUnitMemory = uint64(8 << 30)
	if cpu < 4 || memory < perUnitMemory {
		return 1
	}
	return min(4, cpu/4, int(memory/perUnitMemory))
}

func hostResourceBudget() (int, uint64) {
	cpu := runtime.NumCPU()
	var memory uint64
	switch runtime.GOOS {
	case "linux":
		data, err := os.ReadFile("/proc/meminfo")
		if err != nil {
			return cpu, 0
		}
		for _, line := range strings.Split(string(data), "\n") {
			fields := strings.Fields(line)
			if len(fields) == 3 && fields[0] == "MemTotal:" && fields[2] == "kB" {
				memory, _ = strconv.ParseUint(fields[1], 10, 64)
				memory *= 1024
			}
		}
		data, err = os.ReadFile("/sys/fs/cgroup/memory.max")
		if err != nil && !os.IsNotExist(err) {
			return cpu, 0
		}
		if err == nil && strings.TrimSpace(string(data)) != "max" {
			limit, err := strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
			if err != nil {
				return cpu, 0
			}
			memory = min(memory, limit)
		}
		data, err = os.ReadFile("/sys/fs/cgroup/cpu.max")
		if err != nil && !os.IsNotExist(err) {
			return 1, memory
		}
		if err == nil {
			cpu = constrainedHostCPU(cpu, string(data))
		}
	case "darwin":
		data, err := exec.Command("sysctl", "-n", "hw.memsize").Output()
		if err == nil {
			memory, _ = strconv.ParseUint(strings.TrimSpace(string(data)), 10, 64)
		}
	}
	return cpu, memory
}

func constrainedHostCPU(host int, budget string) int {
	fields := strings.Fields(budget)
	if len(fields) != 2 {
		return 1
	}
	if fields[0] == "max" {
		return host
	}
	quota, qerr := strconv.Atoi(fields[0])
	period, perr := strconv.Atoi(fields[1])
	if qerr != nil || perr != nil || period < 1 || quota < 1 {
		return 1
	}
	return min(host, max(1, quota/period))
}
