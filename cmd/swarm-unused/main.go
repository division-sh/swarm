// swarm-unused delegates reachability and variant union to pinned upstream Staticcheck.
package main

import (
	"bytes"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"go/scanner"
	"go/token"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strings"
)

const (
	staticcheck = "honnef.co/go/tools/cmd/staticcheck@v0.8.1"
	toolchain   = "go1.26.8"
	// issue2438 is the pre-existing noncompiling parked oracle owned by #2438/#2496,
	// explicitly outside Gate A's executable matrix. No packages are excluded.
	buildMatrix = "default:\nrace: -race\nissue2413: -tags=issue2413\n"
	resultFile  = "staticcheck.bin"
	metaFile    = "metadata.json"
)

func main() {
	if err := run(os.Args[1:], os.Stdout, os.Stderr); err != nil {
		fmt.Fprintln(os.Stderr, "swarm-unused:", err)
		os.Exit(1)
	}
}

func run(args []string, stdout, stderr io.Writer) error {
	fs := flag.NewFlagSet("swarm-unused", flag.ContinueOnError)
	fs.SetOutput(stderr)
	collect := fs.Bool("collect", false, "collect native evidence without deciding platform-local U1000")
	merge := fs.Bool("merge", false, "merge required native Linux/Darwin evidence for current Git HEAD")
	out := fs.String("out", "", "native evidence directory (required with -collect)")
	in := fs.String("in", "", "directory containing linux/ and darwin/ evidence (required with -merge)")
	if err := fs.Parse(args); err != nil {
		return err
	}
	if fs.NArg() != 0 || (*collect && *merge) || (*collect && (*out == "" || *in != "")) ||
		(*merge && (*in == "" || *out != "")) || (!*collect && !*merge && (*in != "" || *out != "")) {
		return errors.New("use no flags, -collect -out DIR, or -merge -in DIR")
	}
	root, err := gitOutput(".", "rev-parse", "--show-toplevel")
	if err != nil {
		return err
	}
	g := guard{root: root, stdout: stdout, stderr: stderr}
	switch {
	case *collect:
		path, err := filepath.Abs(*out)
		if err != nil {
			return err
		}
		return g.collect(path)
	case *merge:
		path, err := filepath.Abs(*in)
		if err != nil {
			return err
		}
		return g.merge(path)
	default:
		fmt.Fprintln(stderr, "swarm-unused: local native-only default/race/issue2413 matrix; NOT Linux/Darwin union (issue2438 parked)")
		dir, err := os.MkdirTemp("", "swarm-unused-local-")
		if err != nil {
			return err
		}
		defer os.RemoveAll(dir)
		if _, err := g.analyze(filepath.Join(dir, resultFile)); err != nil {
			return err
		}
		return g.decide([]string{filepath.Join(dir, resultFile)})
	}
}

type guard struct {
	root           string
	stdout, stderr io.Writer
}

func analysisEnv() []string {
	var env []string
	for _, value := range os.Environ() {
		key, _, _ := strings.Cut(value, "=")
		switch key {
		case "GOTOOLCHAIN", "GOENV", "GOFLAGS", "GOWORK", "GOROOT":
			continue
		}
		env = append(env, value)
	}
	return append(env, "GOTOOLCHAIN="+toolchain, "GOENV=off", "GOFLAGS=", "GOWORK=off")
}

func (g guard) command(args ...string) *exec.Cmd {
	c := exec.Command("go", args...)
	c.Dir = g.root
	c.Env = analysisEnv()
	return c
}

type native struct {
	GOOS, GOARCH, CGO_ENABLED, GOHOSTOS, GOHOSTARCH, GOVERSION, GOEXPERIMENT string
}

func (g guard) native() (native, error) {
	var n native
	c := g.command("env", "-json", "GOOS", "GOARCH", "CGO_ENABLED", "GOHOSTOS", "GOHOSTARCH", "GOVERSION", "GOEXPERIMENT")
	c.Stderr = g.stderr
	b, err := c.Output()
	if err != nil {
		return n, fmt.Errorf("analysis environment: %w", err)
	}
	if err := json.Unmarshal(b, &n); err != nil {
		return n, err
	}
	if err := validateNative(n, n.GOOS); err != nil {
		return n, err
	}
	if n.GOOS != runtime.GOOS || n.GOARCH != runtime.GOARCH {
		return n, errors.New("cross-analysis is not native evidence")
	}
	return n, nil
}

func (g guard) staticcheck(args ...string) *exec.Cmd {
	base := []string{"run", staticcheck, "-checks=U1000", "-tests=true", "-go=module", "-show-ignored"}
	return g.command(append(base, args...)...)
}

func (g guard) analyze(path string) (native, error) {
	if err := g.rejectSuppressions(); err != nil {
		return native{}, err
	}
	n, err := g.native()
	if err != nil {
		return n, err
	}
	f, err := os.Create(path)
	if err != nil {
		return n, err
	}
	c := g.staticcheck("-matrix", "-f=binary", "./...")
	c.Stdin = strings.NewReader(buildMatrix)
	c.Stdout = f
	var stderr bytes.Buffer
	c.Stderr = io.MultiWriter(g.stderr, &stderr)
	runErr := c.Run()
	closeErr := f.Close()
	if runErr != nil {
		return n, fmt.Errorf("native matrix failed: %w", runErr)
	}
	if closeErr != nil {
		return n, closeErr
	}
	// Upstream warns on unmatched patterns and skipped oversized packages.
	if strings.Contains(stderr.String(), "warning:") {
		return n, errors.New("native matrix incomplete: upstream warning")
	}
	if _, _, err := hashResult(path); err != nil {
		return n, err
	}
	return n, g.validateDiagnostics(path)
}

func (g guard) rejectSuppressions() error {
	files, err := gitOutput(g.root, "ls-files", "--cached", "--others", "-z", "--", "*.go")
	if err != nil {
		return err
	}
	for _, name := range strings.Split(files, "\x00") {
		if name == "" {
			continue
		}
		b, err := os.ReadFile(filepath.Join(g.root, filepath.FromSlash(name)))
		if os.IsNotExist(err) {
			continue // Dirty local mode permits tracked source deletions.
		}
		if err != nil {
			return err
		}
		var scan scanner.Scanner
		file := token.NewFileSet().AddFile(name, -1, len(b))
		scan.Init(file, b, nil, scanner.ScanComments)
		for {
			pos, kind, text := scan.Scan()
			if kind == token.EOF {
				break
			}
			if kind != token.COMMENT {
				continue
			}
			fields := strings.Fields(text)
			if len(fields) < 2 || (fields[0] != "//lint:ignore" && fields[0] != "//lint:file-ignore") {
				continue
			}
			for _, check := range strings.Split(fields[1], ",") {
				if check == "U1000" {
					// Upstream marks these declarations used; -show-ignored cannot undo it.
					return fmt.Errorf("U1000 suppression forbidden at %s", file.Position(pos))
				}
			}
		}
	}
	return nil
}

func (g guard) validateDiagnostics(path string) error {
	// Binary mode exits zero even on compile errors. Let upstream decode and merge
	// the native binary, but defer U1000 failure until the required platform union.
	c := g.staticcheck("-merge", "-f=json", "-fail=", path)
	c.Stderr = g.stderr
	b, runErr := c.Output()
	dec := json.NewDecoder(bytes.NewReader(b))
	for {
		var diagnostic struct {
			Code    string `json:"code"`
			Message string `json:"message"`
		}
		err := dec.Decode(&diagnostic)
		if err == io.EOF {
			break
		}
		if err != nil {
			return fmt.Errorf("invalid upstream diagnostics: %w", err)
		}
		if diagnostic.Code != "U1000" {
			return fmt.Errorf("native evidence has %s diagnostic: %s\n%s", diagnostic.Code, diagnostic.Message, b)
		}
	}
	if runErr != nil {
		return fmt.Errorf("upstream binary validation failed: %w", runErr)
	}
	return nil
}

func (g guard) decide(paths []string) error {
	c := g.staticcheck(append([]string{"-merge", "-f=text", "-fail=U1000"}, paths...)...)
	c.Stdout, c.Stderr = g.stdout, g.stderr
	if err := c.Run(); err != nil {
		return fmt.Errorf("upstream U1000 merge failed: %w", err)
	}
	return nil
}
