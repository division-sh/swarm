package testpostgres

import (
	"os"
	"strings"
	"testing"
)

func proveEffectiveCgroupCapacity(t *testing.T) {
	const mount = "32 24 0:27 / /sys/fs/cgroup rw - cgroup2 cgroup2 rw\n"
	for _, row := range []struct {
		name string
		edit func(map[string]string)
		want int
	}{
		{"unlimited_nested", func(map[string]string) {}, 4},
		{"leaf_cpu", func(f map[string]string) { f["/sys/fs/cgroup/service/worker/cpu.max"] = "800000 100000" }, 2},
		{"parent_cpu", func(f map[string]string) { f["/sys/fs/cgroup/service/cpu.max"] = "400000 100000" }, 1},
		{"parent_memory", func(f map[string]string) { f["/sys/fs/cgroup/service/memory.max"] = "17179869184" }, 2},
		{"root_memory", func(f map[string]string) { f["/sys/fs/cgroup/memory.max"] = "8589934592" }, 1},
		{"fractional_cpu", func(f map[string]string) { f["/sys/fs/cgroup/service/worker/cpu.max"] = "50000 100000" }, 1},
		{"malformed_cpu", func(f map[string]string) { f["/sys/fs/cgroup/service/worker/cpu.max"] = "max broken" }, 1},
		{"missing_parent_cpu", func(f map[string]string) { delete(f, "/sys/fs/cgroup/service/cpu.max") }, 1},
		{"missing_leaf_memory", func(f map[string]string) { delete(f, "/sys/fs/cgroup/service/worker/memory.max") }, 1},
		{"malformed_memory", func(f map[string]string) { f["/sys/fs/cgroup/service/memory.max"] = "broken" }, 1},
		{"unreadable_parent", func(f map[string]string) { f["/sys/fs/cgroup/service/cpu.max"] = "permission_error" }, 1},
		{"v1_unknown", func(f map[string]string) { f["/proc/self/cgroup"] = "2:cpu:/service\n3:memory:/service\n" }, 1},
		{"hybrid_unknown", func(f map[string]string) { f["/proc/self/cgroup"] += "2:cpu:/service\n" }, 1},
		{"missing_membership", func(f map[string]string) { delete(f, "/proc/self/cgroup") }, 1},
		{"membership_traversal", func(f map[string]string) { f["/proc/self/cgroup"] = "0::/../service\n" }, 1},
		{"missing_mountinfo", func(f map[string]string) { delete(f, "/proc/self/mountinfo") }, 1},
		{"partial_mount_unknown", func(f map[string]string) {
			f["/proc/self/mountinfo"] = strings.Replace(mount, "0:27 / /sys", "0:27 /service /sys", 1)
		}, 1},
		{"duplicate_mount_unknown", func(f map[string]string) { f["/proc/self/mountinfo"] = mount + mount }, 1},
		{"missing_unified_mount", func(f map[string]string) { f["/proc/self/mountinfo"] = strings.Replace(mount, "cgroup2", "cgroup", -1) }, 1},
		{"namespace_root_limit", func(f map[string]string) {
			f["/proc/self/cgroup"] = "0::/\n"
			f["/sys/fs/cgroup/cpu.max"] = "800000 100000"
			f["/sys/fs/cgroup/memory.max"] = "17179869184"
		}, 2},
	} {
		t.Run(row.name, func(t *testing.T) {
			files := map[string]string{
				"/proc/self/cgroup":                        "0::/service/worker\n",
				"/proc/self/mountinfo":                     mount,
				"/sys/fs/cgroup/service/cpu.max":           "max 100000",
				"/sys/fs/cgroup/service/memory.max":        "max",
				"/sys/fs/cgroup/service/worker/cpu.max":    "max 100000",
				"/sys/fs/cgroup/service/worker/memory.max": "max",
			}
			row.edit(files)
			cpu, memory := linuxCgroupResourceBudget(16, 64<<30, func(path string) ([]byte, error) {
				value, ok := files[path]
				if !ok {
					return nil, os.ErrNotExist
				}
				if value == "permission_error" {
					return nil, os.ErrPermission
				}
				return []byte(value), nil
			})
			if got := conservativeRunCapacity(cpu, memory); got != row.want {
				t.Fatalf("effective capacity = %d (cpu=%d bytes=%d), want %d", got, cpu, memory, row.want)
			}
		})
	}
}
