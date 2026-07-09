package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/testchanged"
)

func main() {
	var base string
	var repo string
	var dryRun bool
	var includeUncommitted bool
	flag.StringVar(&base, "base", "origin/master", "git ref used to find the merge-base for committed branch changes")
	flag.StringVar(&repo, "repo", ".", "repository root")
	flag.BoolVar(&dryRun, "dry-run", false, "print the selected packages and go test command without running tests")
	flag.BoolVar(&includeUncommitted, "include-uncommitted", true, "include staged, unstaged, and untracked files")
	flag.Parse()

	if err := run(context.Background(), runConfig{
		base:               base,
		repo:               repo,
		dryRun:             dryRun,
		includeUncommitted: includeUncommitted,
		extraGoTestArgs:    flag.Args(),
		stdout:             os.Stdout,
		stderr:             os.Stderr,
	}); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

type runConfig struct {
	base               string
	repo               string
	dryRun             bool
	includeUncommitted bool
	extraGoTestArgs    []string
	stdout             io.Writer
	stderr             io.Writer
}

func run(ctx context.Context, cfg runConfig) error {
	repoRoot, err := gitOutput(ctx, cfg.repo, "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	repoRoot = strings.TrimSpace(repoRoot)
	packages, err := loadPackages(ctx, repoRoot)
	if err != nil {
		return err
	}
	files, err := changedFiles(ctx, repoRoot, cfg.base, cfg.includeUncommitted)
	if err != nil {
		return err
	}
	plan, err := testchanged.PlanChanged(repoRoot, packages, files)
	if err != nil {
		return err
	}
	printPlan(cfg.stdout, plan, cfg.extraGoTestArgs)
	command := testchanged.TestCommand(plan, cfg.extraGoTestArgs)
	if cfg.dryRun || len(command) == 0 {
		return nil
	}
	cmd := exec.CommandContext(ctx, command[0], command[1:]...)
	cmd.Dir = repoRoot
	cmd.Stdout = cfg.stdout
	cmd.Stderr = cfg.stderr
	cmd.Stdin = os.Stdin
	return cmd.Run()
}

func printPlan(w io.Writer, plan testchanged.Plan, extraArgs []string) {
	fmt.Fprintln(w, "changed files:")
	if len(plan.ChangedFiles) == 0 {
		fmt.Fprintln(w, "  <none>")
	} else {
		for _, file := range plan.ChangedFiles {
			if file.Status == "" {
				fmt.Fprintf(w, "  %s\n", file.Path)
			} else {
				fmt.Fprintf(w, "  %s %s\n", file.Status, file.Path)
			}
		}
	}
	if plan.FullSuite {
		fmt.Fprintln(w, "full suite required:")
		for _, reason := range plan.FullSuiteReasons {
			fmt.Fprintf(w, "  - %s\n", reason)
		}
	} else {
		fmt.Fprintln(w, "seed packages:")
		printPackages(w, plan.SeedPackages)
		fmt.Fprintln(w, "dependent packages:")
		printPackages(w, plan.DependentPackages)
		if len(plan.UnownedFiles) > 0 {
			fmt.Fprintln(w, "unowned files:")
			for _, file := range plan.UnownedFiles {
				fmt.Fprintf(w, "  %s\n", file.Path)
			}
		}
	}
	command := testchanged.TestCommand(plan, extraArgs)
	if len(command) == 0 {
		fmt.Fprintln(w, "go test command:")
		fmt.Fprintln(w, "  <none>")
		return
	}
	fmt.Fprintln(w, "go test command:")
	fmt.Fprintf(w, "  %s\n", shellCommand(command))
}

func printPackages(w io.Writer, packages []testchanged.Package) {
	if len(packages) == 0 {
		fmt.Fprintln(w, "  <none>")
		return
	}
	for _, pkg := range packages {
		fmt.Fprintf(w, "  %s\n", pkg.Pattern())
	}
}

func changedFiles(ctx context.Context, repoRoot, base string, includeUncommitted bool) ([]testchanged.ChangedFile, error) {
	mergeBase := strings.TrimSpace(base)
	if base != "" {
		if out, err := gitOutput(ctx, repoRoot, "merge-base", "HEAD", base); err == nil {
			mergeBase = strings.TrimSpace(out)
		}
	}
	var files []testchanged.ChangedFile
	if mergeBase != "" {
		out, err := gitOutput(ctx, repoRoot, "diff", "--name-status", "--find-renames", mergeBase+"...HEAD")
		if err != nil {
			return nil, err
		}
		files = append(files, parseNameStatus(out)...)
	}
	if includeUncommitted {
		for _, args := range [][]string{
			{"diff", "--name-status", "--find-renames"},
			{"diff", "--cached", "--name-status", "--find-renames"},
		} {
			out, err := gitOutput(ctx, repoRoot, args...)
			if err != nil {
				return nil, err
			}
			files = append(files, parseNameStatus(out)...)
		}
		out, err := gitOutput(ctx, repoRoot, "ls-files", "--others", "--exclude-standard")
		if err != nil {
			return nil, err
		}
		scanner := bufio.NewScanner(strings.NewReader(out))
		for scanner.Scan() {
			path := strings.TrimSpace(scanner.Text())
			if path != "" {
				files = append(files, testchanged.ChangedFile{Path: path, Status: "A"})
			}
		}
		if err := scanner.Err(); err != nil {
			return nil, err
		}
	}
	return dedupeFiles(files), nil
}

func parseNameStatus(out string) []testchanged.ChangedFile {
	var files []testchanged.ChangedFile
	scanner := bufio.NewScanner(strings.NewReader(out))
	for scanner.Scan() {
		line := strings.TrimSpace(scanner.Text())
		if line == "" {
			continue
		}
		parts := strings.Split(line, "\t")
		status := parts[0]
		switch {
		case strings.HasPrefix(status, "R"), strings.HasPrefix(status, "C"):
			if len(parts) >= 3 {
				files = append(files, testchanged.ChangedFile{Path: parts[2], Status: status})
			}
		case len(parts) >= 2:
			files = append(files, testchanged.ChangedFile{Path: parts[1], Status: status})
		}
	}
	return files
}

func dedupeFiles(files []testchanged.ChangedFile) []testchanged.ChangedFile {
	byPath := map[string]testchanged.ChangedFile{}
	for _, file := range files {
		path := filepath.ToSlash(filepath.Clean(strings.TrimSpace(file.Path)))
		if path == "." || path == "" {
			continue
		}
		file.Path = strings.TrimPrefix(path, "./")
		byPath[file.Path] = file
	}
	paths := make([]string, 0, len(byPath))
	for path := range byPath {
		paths = append(paths, path)
	}
	sort.Strings(paths)
	out := make([]testchanged.ChangedFile, 0, len(paths))
	for _, path := range paths {
		out = append(out, byPath[path])
	}
	return out
}

func loadPackages(ctx context.Context, repoRoot string) ([]testchanged.Package, error) {
	out, err := commandOutput(ctx, repoRoot, "go", "list", "-json", "./...")
	if err != nil {
		return nil, err
	}
	decoder := json.NewDecoder(bytes.NewReader([]byte(out)))
	var packages []testchanged.Package
	for decoder.More() {
		var pkg struct {
			ImportPath   string
			Dir          string
			Imports      []string
			TestImports  []string
			XTestImports []string
		}
		if err := decoder.Decode(&pkg); err != nil {
			return nil, fmt.Errorf("decode go list package: %w", err)
		}
		packages = append(packages, testchanged.Package{
			ImportPath:   pkg.ImportPath,
			Dir:          pkg.Dir,
			Imports:      pkg.Imports,
			TestImports:  pkg.TestImports,
			XTestImports: pkg.XTestImports,
		})
	}
	return packages, nil
}

func gitOutput(ctx context.Context, dir string, args ...string) (string, error) {
	return commandOutput(ctx, dir, "git", args...)
}

func commandOutput(ctx context.Context, dir, name string, args ...string) (string, error) {
	cmd := exec.CommandContext(ctx, name, args...)
	cmd.Dir = dir
	out, err := cmd.CombinedOutput()
	if err != nil {
		return "", fmt.Errorf("%s %s: %v\n%s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return string(out), nil
}

func shellCommand(args []string) string {
	parts := make([]string, 0, len(args))
	for _, arg := range args {
		parts = append(parts, shellQuote(arg))
	}
	return strings.Join(parts, " ")
}

func shellQuote(arg string) string {
	if arg == "" {
		return "''"
	}
	if strings.IndexFunc(arg, func(r rune) bool {
		return !(r == '_' || r == '-' || r == '.' || r == '/' || r == ':' || r == '=' || r == '+' || r == ',' || r >= '0' && r <= '9' || r >= 'A' && r <= 'Z' || r >= 'a' && r <= 'z')
	}) == -1 {
		return arg
	}
	return "'" + strings.ReplaceAll(arg, "'", "'\\''") + "'"
}
