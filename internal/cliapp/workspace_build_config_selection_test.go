package cliapp

import (
	"bytes"
	"context"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestWorkspaceBuildConsumesSingleSelectedConfig(t *testing.T) {
	for _, mode := range []string{"defaults", "deployment", "locator", "local_flag", "root_flag"} {
		for _, override := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/override=%t", mode, override), func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				stub := configureWorkspaceBuildDockerStub(t)
				t.Setenv("PATH", t.TempDir())
				base := t.TempDir()
				root := filepath.Join(base, "invocation")
				if err := os.MkdirAll(root, 0o700); err != nil {
					t.Fatal(err)
				}
				for _, path := range []string{filepath.Join(base, ".swarm", "swarm.yaml"), filepath.Join(root, "swarm.yaml"), ignoredUserGlobalConfigPath(t)} {
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					writeRuntimeConfigText(t, path, "runtime: null\n")
				}
				args := []string{"workspace", "build", "--backend", "claude_cli"}
				wantImage := "swarm-workspace:latest"
				if mode == "defaults" {
					args = append(args, "--docker-bin", stub.dockerPath)
				} else {
					path := filepath.Join(root, ".swarm", "swarm.yaml")
					if mode != "deployment" {
						if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
							t.Fatal(err)
						}
						writeRuntimeConfigText(t, path, "runtime: null\n")
						path = filepath.Join(base, "selected", "operator.yaml")
						switch mode {
						case "locator":
							t.Setenv("SWARM_CONFIG", "../selected/operator.yaml")
						case "local_flag":
							args = append(args, "--config", "../selected/operator.yaml")
							t.Setenv("SWARM_CONFIG", "missing-unselected.yaml")
						case "root_flag":
							args = append([]string{"--config", "../selected/operator.yaml"}, args...)
							t.Setenv("SWARM_CONFIG", "missing-unselected.yaml")
						}
					}
					if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
						t.Fatal(err)
					}
					wantImage = "selected-config:test"
					dockerBin := stub.dockerPath
					if override {
						dockerBin = filepath.Join(base, "unselected-docker")
					}
					writeRuntimeConfigText(t, path, fmt.Sprintf("workspace:\n  image: %s\n  docker_bin: %q\n", wantImage, dockerBin))
				}
				if override {
					wantImage = "flag-selected:test"
					args = append(args, "--image", wantImage, "--docker-bin", stub.dockerPath)
				}
				var out, errOut bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), root, args, &out, &errOut, defaultRootCommandOptions())
				if code != cliExitOK || errOut.Len() != 0 {
					t.Fatalf("selected workspace build: code=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
				}
				calls := readWorkspaceBuildDockerCalls(t, stub.callsPath)
				if len(calls) != 5 {
					t.Fatalf("build/validate/publish/cleanup calls = %v", calls)
				}
				image := workspaceBuildTaggedImageFromCall(t, calls[1])
				if calls[0] != "version --format {{.Server.Version}}" || !strings.HasPrefix(calls[2], "run --rm --entrypoint sh "+image+" ") || calls[3] != "tag "+image+" "+wantImage || calls[4] != "image rm "+image {
					t.Fatalf("wrong validated image lifecycle: %v", calls)
				}
			})
		}
	}
}

func TestWorkspaceBuildRejectsSelectedConfigBeforeDockerOverrides(t *testing.T) {
	for _, mode := range []string{"deployment", "locator", "local_flag", "root_flag"} {
		for _, invalid := range []struct{ name, text, want string }{
			{"null_container", "workspace: null\n", "workspace"},
			{"unknown_key", "paths:\n  tooling_lock_file: null\n", `unknown config key "paths.tooling_lock_file"`},
			{"malformed", "workspace: [\n", "parse swarm.yaml config"},
			{"invalid_backend", "llm:\n  backend: no-such-provider\n", "no-such-provider"},
		} {
			t.Run(mode+"/"+invalid.name, func(t *testing.T) {
				isolateCLIAPIConfigEnv(t)
				stub := configureWorkspaceBuildDockerStub(t)
				t.Setenv("PATH", t.TempDir())
				root := t.TempDir()
				path := filepath.Join(root, ".swarm", "swarm.yaml")
				args := []string{"workspace", "build", "--backend", "claude_cli", "--image", "flag-selected:test", "--docker-bin", stub.dockerPath}
				if mode != "deployment" {
					path = filepath.Join(root, "operator.yaml")
					switch mode {
					case "locator":
						t.Setenv("SWARM_CONFIG", "operator.yaml")
					case "local_flag":
						args = append(args, "--config", "operator.yaml")
					case "root_flag":
						args = append([]string{"--config", "operator.yaml"}, args...)
					}
				}
				if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil {
					t.Fatal(err)
				}
				writeRuntimeConfigText(t, path, invalid.text)
				var out, errOut bytes.Buffer
				code := executeRootCommandWithOptions(context.Background(), root, args, &out, &errOut, defaultRootCommandOptions())
				if code != CLIExitRuntime || out.Len() != 0 || !strings.Contains(errOut.String(), path) || !strings.Contains(errOut.String(), invalid.want) {
					t.Fatalf("want selected-file refusal before build: code=%d stdout=%s stderr=%s", code, out.String(), errOut.String())
				}
				if _, err := os.Stat(stub.callsPath); !os.IsNotExist(err) {
					t.Fatalf("Docker reached before config admission: %v", err)
				}
			})
		}
	}
}
