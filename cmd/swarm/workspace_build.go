package main

import (
	"bytes"
	"context"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"strings"
	"time"

	"github.com/division-sh/swarm/internal/config"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/spf13/cobra"
)

const workspaceBuildClaudeCommand = "claude"

type workspaceBuildOptions struct {
	backend    string
	configPath string
	repoRoot   string
	image      string
	dockerBin  string
}

func newWorkspaceCommand(ctx context.Context, repoRoot string) *cobra.Command {
	cmd := &cobra.Command{
		Use:   "workspace",
		Short: "Manage local workspace setup.",
	}
	cmd.AddCommand(newWorkspaceBuildCommand(ctx, repoRoot))
	return cmd
}

func newWorkspaceBuildCommand(ctx context.Context, repoRoot string) *cobra.Command {
	opts := workspaceBuildOptions{repoRoot: strings.TrimSpace(repoRoot)}
	cmd := &cobra.Command{
		Use:   "build --backend claude_cli [--image <tag>]",
		Short: "Build and validate a local workspace image.",
		Args:  cobra.NoArgs,
		PreRunE: func(cmd *cobra.Command, args []string) error {
			backend := strings.ToLower(strings.TrimSpace(opts.backend))
			if backend == "" {
				return fmt.Errorf("swarm workspace build requires --backend claude_cli")
			}
			if backend != "claude_cli" {
				return fmt.Errorf("unsupported workspace build backend %q; only claude_cli is supported", strings.TrimSpace(opts.backend))
			}
			opts.backend = backend
			if cmd.Flags().Changed("docker-bin") {
				dockerBin, err := normalizeWorkspaceBuildDockerBin(opts.dockerBin, "--docker-bin")
				if err != nil {
					return err
				}
				opts.dockerBin = dockerBin
			}
			if cmd.Flags().Changed("image") {
				image, err := normalizeWorkspaceBuildImage(opts.image, "--image")
				if err != nil {
					return err
				}
				opts.image = image
			}
			if path, set, err := effectiveCommandConfigPath(cmd, opts.configPath, cmd.Flags().Changed("config")); err != nil {
				return err
			} else if set {
				opts.configPath = path
			}
			return nil
		},
		RunE: func(cmd *cobra.Command, args []string) error {
			if err := runWorkspaceBuildCommand(ctx, cmd.OutOrStdout(), opts); err != nil {
				fmt.Fprintln(cmd.ErrOrStderr(), err)
				return commandExitError{code: cliExitRuntime}
			}
			return nil
		},
	}
	cmd.Flags().StringVar(&opts.backend, "backend", opts.backend, "Workspace image build backend to materialize: claude_cli")
	cmd.Flags().StringVar(&opts.configPath, "config", opts.configPath, "Optional path to Swarm runtime config for workspace.image/workspace.docker_bin")
	cmd.Flags().StringVar(&opts.image, "image", opts.image, "Workspace image tag to build; defaults to workspace.image or swarm-workspace:latest")
	cmd.Flags().StringVar(&opts.dockerBin, "docker-bin", opts.dockerBin, "Docker-compatible CLI binary; defaults to workspace.docker_bin or docker")
	return cmd
}

func runWorkspaceBuildCommand(ctx context.Context, out io.Writer, opts workspaceBuildOptions) error {
	var cfg *config.Config
	if configPath := strings.TrimSpace(opts.configPath); configPath != "" {
		cfgResult, err := loadRuntimeConfigWithOptions(runtimeConfigLoadOptions{RepoRoot: opts.repoRoot, ExplicitPath: configPath})
		if err != nil {
			return fmt.Errorf("workspace image build failed: load config: %w", err)
		}
		cfg = cfgResult.Config
	}
	image := strings.TrimSpace(opts.image)
	if image == "" {
		var err error
		image, err = workspaceImageFromRuntimeConfigOrDefault(cfg)
		if err != nil {
			return fmt.Errorf("workspace image build failed: %w", err)
		}
		image, err = normalizeWorkspaceBuildImage(image, "workspace.image")
		if err != nil {
			return err
		}
	}
	dockerfile, err := platform.MaterializeWorkspaceDockerfile()
	if err != nil {
		return fmt.Errorf("workspace image build failed: %w", err)
	}
	contextDir, cleanup, err := materializeWorkspaceBuildContext(dockerfile)
	if err != nil {
		return fmt.Errorf("workspace image build failed: %w", err)
	}
	defer cleanup()

	dockerBin := strings.TrimSpace(opts.dockerBin)
	if dockerBin == "" {
		var err error
		dockerBin, err = workspaceDockerBinFromRuntimeConfigOrDefault(cfg)
		if err != nil {
			return fmt.Errorf("workspace image build failed: %w", err)
		}
	}
	if _, err := runWorkspaceBuildDocker(ctx, dockerBin, "version", "--format", "{{.Server.Version}}"); err != nil {
		return fmt.Errorf("workspace image build failed: Docker is not available via %q; start Docker, set workspace.docker_bin, or pass --docker-bin: %w", dockerBin, err)
	}

	if out != nil {
		fmt.Fprintf(out, "Building workspace image %s for backend claude_cli\n", image)
	}
	tempImage := temporaryWorkspaceBuildImageTag()
	if _, err := runWorkspaceBuildDocker(ctx, dockerBin,
		"build",
		"-t", tempImage,
		"-f", dockerfile,
		"--build-arg", "INSTALL_CLAUDE_CLI=true",
		"--build-arg", "INSTALL_CODEX_CLI=false",
		contextDir,
	); err != nil {
		return fmt.Errorf("workspace image build failed for image %q: %w", image, err)
	}
	defer func() {
		_, _ = runWorkspaceBuildDocker(ctx, dockerBin, "image", "rm", tempImage)
	}()

	if out != nil {
		fmt.Fprintf(out, "Validating workspace image %s can execute %s\n", image, workspaceBuildClaudeCommand)
	}
	if _, err := runWorkspaceBuildDocker(ctx, dockerBin,
		"run", "--rm", "--entrypoint", "sh", tempImage,
		"-lc", `command -v -- "$1" >/dev/null && "$1" --version >/dev/null`,
		"swarm-cli-proof", workspaceBuildClaudeCommand,
	); err != nil {
		return fmt.Errorf("workspace image validation failed: configured Claude CLI command %q cannot execute in workspace image %q; build or pull a workspace image that includes a runnable Claude CLI, set workspace.image, or pass --image with a compatible image: %w", workspaceBuildClaudeCommand, image, err)
	}
	if _, err := runWorkspaceBuildDocker(ctx, dockerBin, "tag", tempImage, image); err != nil {
		return fmt.Errorf("workspace image build failed to publish validated image %q: %w", image, err)
	}
	if out != nil {
		fmt.Fprintf(out, "Workspace image %s is ready for claude_cli\n", image)
	}
	return nil
}

func normalizeWorkspaceBuildDockerBin(raw, source string) (string, error) {
	dockerBin := strings.TrimSpace(raw)
	if dockerBin == "" {
		return "", fmt.Errorf("workspace docker binary from %s must be non-empty", source)
	}
	if strings.ContainsAny(dockerBin, "\r\n") {
		return "", fmt.Errorf("workspace docker binary from %s must not contain newlines", source)
	}
	return dockerBin, nil
}

func temporaryWorkspaceBuildImageTag() string {
	return fmt.Sprintf("swarm-workspace-build-%d:%d", os.Getpid(), time.Now().UnixNano())
}

func normalizeWorkspaceBuildImage(raw, source string) (string, error) {
	image := strings.TrimSpace(raw)
	if image == "" {
		return "", fmt.Errorf("workspace image from %s must be non-empty", source)
	}
	if strings.ContainsAny(image, " \t\r\n") {
		return "", fmt.Errorf("workspace image from %s must not contain whitespace", source)
	}
	return image, nil
}

func materializeWorkspaceBuildContext(dockerfile string) (string, func(), error) {
	dir, err := os.MkdirTemp("", "swarm-workspace-build-context-*")
	if err != nil {
		return "", func() {}, err
	}
	cleanup := func() {
		_ = os.RemoveAll(dir)
	}
	data, err := os.ReadFile(dockerfile)
	if err != nil {
		cleanup()
		return "", func() {}, err
	}
	if err := os.WriteFile(filepath.Join(dir, platform.DefaultWorkspaceDockerfilePath), data, 0o644); err != nil {
		cleanup()
		return "", func() {}, err
	}
	return dir, cleanup, nil
}

func runWorkspaceBuildDocker(ctx context.Context, dockerBin string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, dockerBin, args...)
	var raw bytes.Buffer
	cmd.Stdout = &raw
	cmd.Stderr = &raw
	if err := cmd.Run(); err != nil {
		out := strings.TrimSpace(raw.String())
		if out == "" {
			return "", err
		}
		return "", fmt.Errorf("%w: %s", err, out)
	}
	return strings.TrimSpace(raw.String()), nil
}
