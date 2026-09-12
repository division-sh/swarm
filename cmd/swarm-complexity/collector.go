package main

import (
	"bytes"
	"context"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"os"
	"os/exec"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
)

type analyze func(context.Context, string, string) ([]byte, error)
type expectedRow struct {
	id        identity
	name, pkg string
}
type population map[string]expectedRow

func upstream(ctx context.Context, dir, metric string) ([]byte, error) {
	args := []string{"run", toolModules[metric]}
	if metric == "cognit" {
		args = append(args, "-over=-1")
	}
	args = append(args, dir)
	c := exec.CommandContext(ctx, "go", args...)
	c.Dir = dir
	c.Env = append(os.Environ(), "GOWORK=off", "GOFLAGS=")
	var stderr bytes.Buffer
	c.Stderr = &stderr
	b, err := c.Output()
	if err != nil {
		return nil, fmt.Errorf("%s: %w: %s", toolModules[metric], err, stderr.String())
	}
	// go run may announce first-time module downloads; any analyzer diagnostic is fatal.
	for _, line := range strings.Split(strings.TrimSpace(stderr.String()), "\n") {
		if line != "" && !strings.HasPrefix(line, "go: downloading ") {
			return nil, fmt.Errorf("%s diagnostics: %s", metric, stderr.String())
		}
	}
	return b, nil
}

func measure(ctx context.Context, repo, sha string, analyzer analyze) (baseline, error) {
	sources, files, err := inventory(ctx, repo, sha)
	if err != nil {
		return baseline{}, err
	}
	dir, err := os.MkdirTemp("", "swarm-complexity-")
	if err != nil {
		return baseline{}, err
	}
	defer os.RemoveAll(dir)
	b := baseline{Policy: currentPolicy(), Files: files, Metrics: map[string][]score{}}
	pops := map[string]population{"cyclo": {}, "cognit": {}}
	admitted := 0
	for i, source := range sources {
		staged := filepath.Join(dir, fmt.Sprintf("f%06d.go", i))
		generated, err := stageSource(source, staged, pops)
		if err != nil {
			return baseline{}, fmt.Errorf("%s: %w", source.path, err)
		}
		class := "authored"
		if generated {
			class = "generated"
		} else {
			admitted++
		}
		b.Files = append(b.Files, fileRecord{source.path, class})
	}
	if admitted == 0 {
		return baseline{}, fmt.Errorf("empty authored Go inventory")
	}
	sort.Slice(b.Files, func(i, j int) bool { return b.Files[i].Path < b.Files[j].Path })
	for _, metric := range []string{"cyclo", "cognit"} {
		raw, err := analyzer(ctx, dir, metric)
		if err != nil {
			return baseline{}, err
		}
		rows, err := normalize(metric, raw, pops[metric])
		if err != nil {
			return baseline{}, err
		}
		b.Metrics[metric] = rows
	}
	return b, nil
}

func stageSource(source sourceFile, staged string, pops map[string]population) (bool, error) {
	fset := token.NewFileSet()
	f, err := parser.ParseFile(fset, staged, source.data, parser.ParseComments)
	if err != nil {
		return false, err
	}
	if ast.IsGenerated(f) {
		return true, nil
	}
	if err := sourceDirectives(f); err != nil {
		return false, err
	}
	occurrences := map[string]int{}
	add := func(node ast.Node, kind, name string, metrics ...string) {
		key := kind + ":" + name
		occurrences[key]++
		id := identity{source.path, f.Name.Name, kind, name, occurrences[key]}
		position := fset.PositionFor(node.Pos(), false).String()
		for _, metric := range metrics {
			pops[metric][position] = expectedRow{id, name, f.Name.Name}
		}
	}
	for _, d := range f.Decls {
		switch n := d.(type) {
		case *ast.FuncDecl:
			name, err := functionName(n)
			if err != nil {
				return false, err
			}
			add(n, "declaration", name, "cyclo", "cognit")
		case *ast.GenDecl:
			for _, spec := range n.Specs {
				v, ok := spec.(*ast.ValueSpec)
				if !ok {
					continue
				}
				for _, value := range v.Values {
					if fn, ok := value.(*ast.FuncLit); ok {
						add(fn, "variable-literal", v.Names[0].Name, "cyclo")
					}
				}
			}
		}
	}
	return false, os.WriteFile(staged, source.data, 0600)
}

func sourceDirectives(f *ast.File) error {
	for _, group := range f.Comments {
		for _, c := range group.List {
			text := c.Text
			if strings.HasPrefix(text, "//line ") || strings.HasPrefix(text, "/*line ") {
				return fmt.Errorf("unsupported line-directive provenance")
			}
			for _, tool := range []string{"gocyclo", "gocognit"} {
				if strings.HasPrefix(text, "//"+tool+":") && strings.TrimSpace(strings.TrimPrefix(text, "//"+tool+":")) == "ignore" {
					return fmt.Errorf("%s suppression is not admitted", tool)
				}
			}
		}
	}
	return nil
}

func functionName(f *ast.FuncDecl) (string, error) {
	if f.Recv == nil {
		return f.Name.Name, nil
	}
	r, err := receiverName(f.Recv.List[0].Type)
	return "(" + r + ")." + f.Name.Name, err
}

func receiverName(e ast.Expr) (string, error) {
	switch n := e.(type) {
	case *ast.Ident:
		return n.Name, nil
	case *ast.StarExpr:
		s, err := receiverName(n.X)
		return "*" + s, err
	case *ast.IndexExpr:
		return receiverName(n.X)
	case *ast.IndexListExpr:
		return receiverName(n.X)
	default:
		return "", fmt.Errorf("unsupported receiver syntax %T", e)
	}
}

func normalize(metric string, raw []byte, expected population) ([]score, error) {
	rows := []score{}
	seen := map[string]bool{}
	for _, line := range strings.Split(strings.TrimSpace(string(raw)), "\n") {
		if line == "" {
			continue
		}
		parts := strings.SplitN(line, " ", 4)
		if len(parts) != 4 {
			return nil, fmt.Errorf("%s malformed output %q", metric, line)
		}
		value, err := strconv.Atoi(parts[0])
		if err != nil || value < 0 || metric == "cyclo" && value == 0 {
			return nil, fmt.Errorf("%s invalid score %q", metric, parts[0])
		}
		row, ok := expected[parts[3]]
		if !ok || row.pkg != parts[1] || row.name != parts[2] {
			return nil, fmt.Errorf("%s unexpected provenance: %s", metric, line)
		}
		if seen[parts[3]] {
			return nil, fmt.Errorf("%s duplicate analyzer output: %s", metric, line)
		}
		seen[parts[3]] = true
		rows = append(rows, score{row.id, value})
	}
	if len(seen) != len(expected) {
		return nil, fmt.Errorf("%s incomplete output: got %d expected %d", metric, len(seen), len(expected))
	}
	sort.Slice(rows, func(i, j int) bool { return identityKey(rows[i].Identity) < identityKey(rows[j].Identity) })
	return rows, nil
}
