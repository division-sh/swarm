package cliapp

import (
	"encoding/json"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"testing"
)

func TestCLIAPPTestMainPreservesCachesAndRemovesHome(t *testing.T) {
	if expected := os.Getenv("SWARM_TEST_CLIAPP_HOME_PROOF"); expected != "" {
		var caches map[string]string
		if err := json.Unmarshal([]byte(expected), &caches); err != nil {
			t.Fatal(err)
		}
		data, err := exec.Command("go", "env", "-json", "GOPATH", "GOMODCACHE", "GOCACHE").Output()
		if err != nil {
			t.Fatal(err)
		}
		var actual map[string]string
		if err := json.Unmarshal(data, &actual); err != nil {
			t.Fatal(err)
		}
		for key, value := range caches {
			if got := os.Getenv(key); got != value || actual[key] != value {
				t.Errorf("%s env=%q go env=%q, want pre-isolation %q", key, got, actual[key], value)
			}
		}
		if data, err := exec.Command("go", "env", "GOTELEMETRY").Output(); err != nil || strings.TrimSpace(string(data)) != "off" {
			t.Fatalf("private test telemetry is not off: %q, %v", data, err)
		}
		home := os.Getenv("HOME")
		if !strings.HasPrefix(filepath.Base(home), "swarm-cliapp-test-home-") || os.Getenv("XDG_CONFIG_HOME") != home {
			t.Fatalf("Swarm config is not isolated: HOME=%q XDG_CONFIG_HOME=%q", home, os.Getenv("XDG_CONFIG_HOME"))
		}
		if data, err := os.ReadFile(filepath.Join(home, "swarm", "swarm.yaml")); err != nil || string(data) != "{}\n" {
			t.Fatalf("isolated Swarm config: %q, %v", data, err)
		}
		module := filepath.Join(home, "go", "pkg", "mod", "example.com", "module@v1.0.0")
		if err := os.MkdirAll(module, 0o755); err != nil {
			t.Fatal(err)
		}
		if err := os.WriteFile(filepath.Join(module, "readonly.go"), []byte("package example\n"), 0o444); err != nil {
			t.Fatal(err)
		}
		if err := os.Chmod(module, 0o555); err != nil {
			t.Fatal(err)
		}
		return
	}

	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	for _, explicit := range []bool{false, true} {
		name := "home-derived"
		if explicit {
			name = "explicit"
		}
		t.Run(name, func(t *testing.T) {
			root := t.TempDir()
			originalHome := filepath.Join(root, "original-home")
			if err := os.MkdirAll(originalHome, 0o755); err != nil {
				t.Fatal(err)
			}
			var env []string
			for _, item := range os.Environ() {
				key, _, _ := strings.Cut(item, "=")
				switch key {
				case "HOME", "XDG_CONFIG_HOME", "GOPATH", "GOMODCACHE", "GOCACHE", "GOENV", "TMPDIR", "TEST_TELEMETRY_DIR", "SWARM_TEST_CLIAPP_HOME_PROOF":
					continue
				}
				env = append(env, item)
			}
			env = append(env, "HOME="+originalHome, "XDG_CONFIG_HOME="+originalHome, "TMPDIR="+root, "GOENV=off")
			if explicit {
				for _, key := range []string{"GOPATH", "GOMODCACHE", "GOCACHE"} {
					env = append(env, key+"="+os.Getenv(key))
				}
			}
			telemetry := exec.Command("go", "telemetry", "off")
			telemetry.Env, telemetry.Dir = env, root
			if output, err := telemetry.CombinedOutput(); err != nil {
				t.Fatalf("disable fixture telemetry: %v\n%s", err, output)
			}
			query := exec.Command("go", "env", "-json", "GOPATH", "GOMODCACHE", "GOCACHE")
			query.Env, query.Dir = env, root
			values, err := query.Output()
			if err != nil {
				t.Fatal(err)
			}
			child := exec.Command(binary, "-test.run=^TestCLIAPPTestMainPreservesCachesAndRemovesHome$")
			child.Env, child.Dir = append(env, "SWARM_TEST_CLIAPP_HOME_PROOF="+string(values)), root
			if output, err := child.CombinedOutput(); err != nil {
				t.Fatalf("cliapp test subprocess: %v\n%s", err, output)
			}
			leftovers, err := filepath.Glob(filepath.Join(root, "swarm-cliapp-test-home-*"))
			if err != nil || len(leftovers) != 0 {
				t.Fatalf("test-home leftovers: %v, %v", leftovers, err)
			}
		})
	}
}

func TestCLIAPPTestHomeCleanupDoesNotFollowLinks(t *testing.T) {
	home, outside := t.TempDir(), t.TempDir()
	file := filepath.Join(outside, "keep")
	if err := os.WriteFile(file, []byte("shared-cache"), 0o444); err != nil {
		t.Fatal(err)
	}
	if err := os.Symlink(outside, filepath.Join(home, "external-cache")); err != nil {
		t.Fatal(err)
	}
	if err := removeCLIAPPTestHome(home); err != nil {
		t.Fatal(err)
	}
	if data, err := os.ReadFile(file); err != nil || string(data) != "shared-cache" {
		t.Fatalf("cleanup altered external cache: %q, %v", data, err)
	}
	if info, err := os.Stat(file); err != nil || info.Mode().Perm() != 0o444 {
		t.Fatalf("cleanup changed external file permissions: %v, %v", info, err)
	}
	if _, err := os.Stat(home); !os.IsNotExist(err) {
		t.Fatalf("test home still exists after cleanup: %v", err)
	}
	if err := removeCLIAPPTestHome(filepath.Join(outside, "missing")); err == nil {
		t.Fatal("cleanup error was silently discarded")
	}
}
