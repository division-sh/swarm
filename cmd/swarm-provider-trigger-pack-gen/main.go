package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
)

func main() {
	root := flag.String("root", "packs/provider-triggers", "provider-trigger pack inventory root")
	check := flag.Bool("check", false, "fail when a checked-in pack envelope is stale")
	flag.Parse()
	if err := stampInventory(strings.TrimSpace(*root), *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func stampInventory(root string, check bool) error {
	entries, err := os.ReadDir(root)
	if err != nil {
		return fmt.Errorf("read provider-trigger inventory %q: %w", root, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		dir := filepath.Join(root, entry.Name())
		envelopePath := filepath.Join(dir, packs.EnvelopeFileName)
		manifestPath := filepath.Join(dir, packs.TriggerManifestFileName)
		envelopeBody, err := os.ReadFile(envelopePath)
		if err != nil {
			return fmt.Errorf("read %s: %w", envelopePath, err)
		}
		manifestBody, err := os.ReadFile(manifestPath)
		if err != nil {
			return fmt.Errorf("read %s: %w", manifestPath, err)
		}
		_, stamped, err := providertriggers.StampPackEnvelope(envelopeBody, manifestBody)
		if err != nil {
			return fmt.Errorf("stamp %s: %w", dir, err)
		}
		if check {
			if !bytes.Equal(envelopeBody, stamped) {
				return fmt.Errorf("provider-trigger pack envelope %s is stale; run go run ./cmd/swarm-provider-trigger-pack-gen --root %s", envelopePath, root)
			}
		} else if err := os.WriteFile(envelopePath, stamped, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", envelopePath, err)
		}
		count++
	}
	if count == 0 {
		return fmt.Errorf("provider-trigger inventory %q contains no packs", root)
	}
	return nil
}
