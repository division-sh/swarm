package testplanning

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
)

type productInputs struct {
	Version     int               `json:"version"`
	Source      string            `json:"source"`
	Profile     string            `json:"profile"`
	Environment json.RawMessage   `json:"environment"`
	Inputs      map[string]string `json:"inputs"`
	Arguments   []string          `json:"arguments"`
	Compiler    string            `json:"compiler,omitempty"`
	RunnerImage string            `json:"runner_image,omitempty"`
}

type productManifest struct {
	Inputs productInputs `json:"inputs"`
	SHA256 string        `json:"sha256"`
}

// BuildGoProduct reuses compilation only. Outputs are copied into the caller's
// private workspace; no process, store, or test result survives with the product.
func BuildGoProduct(ctx context.Context, root, cache, output, profile string, args ...string) error {
	if len(args) < 2 {
		return fmt.Errorf("build product requires a Go command and package")
	}
	if cache == "" {
		return compileProduct(ctx, root, output, args)
	}
	inputs, err := goProductInputs(ctx, root, profile, args)
	if err != nil {
		return compileProduct(ctx, root, output, args)
	}
	encoded, _ := json.Marshal(inputs)
	key := digestBytes(encoded)
	binary := filepath.Join(cache, key)
	if raw, err := os.ReadFile(binary + ".json"); err == nil {
		var manifest productManifest
		if json.Unmarshal(raw, &manifest) == nil {
			actual, _ := json.Marshal(manifest.Inputs)
			if data, err := os.ReadFile(binary); err == nil && bytes.Equal(actual, encoded) && digestBytes(data) == manifest.SHA256 {
				return os.WriteFile(output, data, 0700)
			}
		}
	}
	if err := compileProduct(ctx, root, output, args); err != nil {
		return err
	}
	data, err := os.ReadFile(output)
	if err != nil {
		return err
	}
	manifest, _ := json.Marshal(productManifest{Inputs: inputs, SHA256: digestBytes(data)})
	// Cache publication is advisory; a failed save cannot invalidate a fresh build.
	if os.MkdirAll(cache, 0700) == nil && os.WriteFile(binary, data, 0700) == nil {
		_ = os.WriteFile(binary+".json", manifest, 0600)
	}
	return nil
}

func goProductInputs(ctx context.Context, root, profile string, args []string) (productInputs, error) {
	git := func(arguments ...string) ([]byte, error) {
		cmd := exec.CommandContext(ctx, "git", arguments...)
		cmd.Dir = root
		return cmd.Output()
	}
	dirty, err := git("status", "--porcelain", "--untracked-files=all", "--", "cmd", "internal", "go.mod", "go.sum", "platform-spec.yaml", ".github/test-proof-plan.yaml")
	if err != nil || len(dirty) != 0 {
		return productInputs{}, fmt.Errorf("build product source is not a clean committed snapshot")
	}
	head, err := git("rev-parse", "HEAD")
	if err != nil {
		return productInputs{}, err
	}
	command := exec.CommandContext(ctx, "go", "env", "-json", "GOVERSION", "GOTOOLCHAIN", "GOOS", "GOARCH", "GOFLAGS", "CGO_ENABLED", "GOEXPERIMENT", "GOAMD64", "GOARM", "GOARM64", "GOWORK", "CC", "CGO_CFLAGS", "CGO_CPPFLAGS", "CGO_CXXFLAGS", "CGO_LDFLAGS")
	command.Dir = root
	environment, err := command.Output()
	if err != nil {
		return productInputs{}, err
	}
	var compact bytes.Buffer
	if err := json.Compact(&compact, environment); err != nil {
		return productInputs{}, err
	}
	inputs := productInputs{Version: 1, Source: strings.TrimSpace(string(head)), Profile: profile,
		Environment: compact.Bytes(), Inputs: map[string]string{}, Arguments: append([]string(nil), args...), RunnerImage: os.Getenv("ImageOS") + "/" + os.Getenv("ImageVersion")}
	var effective map[string]string
	if err := json.Unmarshal(environment, &effective); err != nil {
		return productInputs{}, err
	}
	if effective["CGO_ENABLED"] == "1" {
		compiler := strings.Fields(effective["CC"])
		if len(compiler) != 1 {
			return productInputs{}, fmt.Errorf("compound compiler identity is not cacheable")
		}
		version, err := exec.CommandContext(ctx, compiler[0], "--version").Output()
		if err != nil {
			return productInputs{}, err
		}
		inputs.Compiler = string(version)
	}
	for _, path := range []string{"go.mod", "go.sum", ".github/test-proof-plan.yaml"} {
		raw, err := os.ReadFile(filepath.Join(root, path))
		if err != nil {
			return productInputs{}, err
		}
		inputs.Inputs[path] = digestBytes(raw)
	}
	return inputs, nil
}

func compileProduct(ctx context.Context, root, output string, args []string) error {
	commandArgs := append(append([]string(nil), args[:len(args)-1]...), "-o", output, args[len(args)-1])
	command := exec.CommandContext(ctx, "go", commandArgs...)
	command.Dir = root
	if log, err := command.CombinedOutput(); err != nil {
		return fmt.Errorf("build Go product: %w\n%s", err, log)
	}
	return nil
}

func digestBytes(raw []byte) string {
	sum := sha256.Sum256(raw)
	return hex.EncodeToString(sum[:])
}

func WarmReleaseProducts(ctx context.Context, root, cache, profile string) error {
	for _, race := range []bool{false, true} {
		if race && profile != ProfileFull {
			continue
		}
		for _, target := range []struct{ args []string }{
			{[]string{"build", "./cmd/swarm"}}, {[]string{"test", "-c", "./internal/serveapp"}},
		} {
			args := append([]string(nil), target.args...)
			if race {
				args = append(args[:len(args)-1], "-race", target.args[len(target.args)-1])
			}
			file, err := os.CreateTemp("", "swarm-build-product-*")
			if err != nil {
				return err
			}
			path := file.Name()
			_ = file.Close()
			err = BuildGoProduct(ctx, root, cache, path, profile, args...)
			_ = os.Remove(path)
			if err != nil {
				return err
			}
		}
	}
	return nil
}
