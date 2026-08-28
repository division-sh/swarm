package cliapp

import (
	"embed"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strings"

	swarmassets "github.com/division-sh/swarm"
	"github.com/division-sh/swarm/internal/cli/argcount"
	"github.com/spf13/cobra"
)

//go:embed archetypes archetypes/zero-agent-automation/.swarm/swarm.yaml
var archetypeFiles embed.FS

type admittedArchetype struct {
	Files      fs.FS
	SourceRoot string
	WorkingDir string
}

var admittedArchetypes = map[string]admittedArchetype{
	"zero-agent-automation": {
		Files: archetypeFiles, SourceRoot: "archetypes/zero-agent-automation", WorkingDir: ".",
	},
	"webhook-responder": {
		Files: swarmassets.EmbeddedTelegramAgentExample(), SourceRoot: ".", WorkingDir: "./bot",
	},
}

func newArchetypeCommand(root InvocationRoot) *cobra.Command {
	var output string
	cmd := &cobra.Command{
		Use:   "new <archetype>",
		Short: "Create a proven flow starter.",
		Args:  argcount.ExactArgs(1),
		RunE: func(cmd *cobra.Command, args []string) error {
			return scaffoldArchetype(root, cmd.OutOrStdout(), args[0], output)
		},
	}
	cmd.Flags().StringVarP(&output, "output", "o", "", "Destination directory (defaults to the archetype name)")
	return cmd
}

func scaffoldArchetype(root InvocationRoot, out io.Writer, rawName, rawOutput string) error {
	name := strings.TrimSpace(rawName)
	source, ok := admittedArchetypes[name]
	if !ok {
		ids := make([]string, 0, len(admittedArchetypes))
		for id := range admittedArchetypes {
			ids = append(ids, id)
		}
		sort.Strings(ids)
		return fmt.Errorf("unknown archetype %q; admitted archetypes: %s", name, strings.Join(ids, ", "))
	}
	destination := strings.TrimSpace(rawOutput)
	if destination == "" {
		destination = name
	}
	absDestination := root.Resolve(destination)
	if _, err := os.Stat(absDestination); err == nil {
		return fmt.Errorf("scaffold destination already exists: %s", absDestination)
	} else if !os.IsNotExist(err) {
		return fmt.Errorf("inspect scaffold destination: %w", err)
	}
	if err := os.MkdirAll(absDestination, 0o755); err != nil {
		return fmt.Errorf("create scaffold destination: %w", err)
	}
	complete := false
	defer func() {
		if !complete {
			_ = os.RemoveAll(absDestination)
		}
	}()
	if err := fs.WalkDir(source.Files, source.SourceRoot, func(path string, entry fs.DirEntry, walkErr error) error {
		if walkErr != nil {
			return walkErr
		}
		relative, err := filepath.Rel(filepath.FromSlash(source.SourceRoot), filepath.FromSlash(path))
		if err != nil || relative == "." {
			return err
		}
		target := filepath.Join(absDestination, relative)
		if entry.IsDir() {
			return os.MkdirAll(target, 0o755)
		}
		body, err := fs.ReadFile(source.Files, path)
		if err != nil {
			return err
		}
		return os.WriteFile(target, body, 0o644)
	}); err != nil {
		return fmt.Errorf("write %s scaffold: %w", name, err)
	}
	complete = true
	display := filepath.Clean(destination)
	fmt.Fprintf(out, "Created %s in %s\n\nNext:\n", name, display)
	fmt.Fprintf(out, "  cd %s\n", display)
	if source.WorkingDir != "." {
		fmt.Fprintf(out, "  cd %s\n", source.WorkingDir)
	}
	fmt.Fprintln(out, "  swarm verify --config ./swarm.yaml --contracts .")
	fmt.Fprintln(out, "  swarm serve --config ./swarm.yaml --contracts .")
	fmt.Fprintln(out, "  swarm test --config ./swarm.yaml --contracts . ./tests/smoke.yaml")
	return nil
}
