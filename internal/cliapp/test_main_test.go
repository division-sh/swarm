package cliapp

import (
	"encoding/json"
	"errors"
	"fmt"
	"io/fs"
	"os"
	"os/exec"
	"path/filepath"
	"testing"
)

func TestMain(m *testing.M) {
	if err := preserveCLIAPPGoCaches(); err != nil {
		fmt.Fprintf(os.Stderr, "preserve cliapp Go caches: %v\n", err)
		os.Exit(1)
	}
	home, err := os.MkdirTemp("", "swarm-cliapp-test-home-")
	if err != nil {
		panic(err)
	}
	code := func() (code int) {
		defer func() {
			if err := removeCLIAPPTestHome(home); err != nil {
				fmt.Fprintf(os.Stderr, "remove cliapp test home %s: %v\n", home, err)
				code = 1
			}
		}()
		if err := os.Setenv("HOME", home); err != nil {
			panic(err)
		}
		if err := os.Setenv("XDG_CONFIG_HOME", home); err != nil {
			panic(err)
		}
		// A telemetry sidecar can outlive go commands and race private-home removal.
		telemetry := exec.Command("go", "telemetry", "off")
		telemetry.Env = append(os.Environ(), "TEST_TELEMETRY_DIR=")
		if output, err := telemetry.CombinedOutput(); err != nil {
			fmt.Fprintf(os.Stderr, "disable private test-home Go telemetry: %v\n%s", err, output)
			return 1
		}
		writeCLIAPITestExecutionPosture(testMainTB{})
		return m.Run()
	}()
	os.Exit(code)
}

func preserveCLIAPPGoCaches() error {
	keys := []string{"GOPATH", "GOMODCACHE", "GOCACHE"}
	data, err := exec.Command("go", append([]string{"env", "-json"}, keys...)...).Output()
	if err != nil {
		return err
	}
	var values map[string]string
	if err := json.Unmarshal(data, &values); err != nil {
		return err
	}
	for _, key := range keys {
		if values[key] == "" {
			return fmt.Errorf("go env returned empty %s", key)
		}
		if err := os.Setenv(key, values[key]); err != nil {
			return err
		}
	}
	return nil
}

func removeCLIAPPTestHome(home string) error {
	// Module cache files may be read-only; only their parent directories need write access.
	err := filepath.WalkDir(home, func(path string, entry fs.DirEntry, err error) error {
		if err != nil {
			return err
		}
		if entry.IsDir() {
			info, err := entry.Info()
			if err != nil {
				return err
			}
			return os.Chmod(path, info.Mode()|0o700)
		}
		return nil
	})
	return errors.Join(err, os.RemoveAll(home))
}

type testMainTB struct{}

func (testMainTB) Helper() {}

func (testMainTB) Fatalf(format string, args ...any) {
	panic(fmt.Sprintf(format, args...))
}
