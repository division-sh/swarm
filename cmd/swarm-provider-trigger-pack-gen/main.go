package main

import (
	"bytes"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"sort"
	"strings"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
	"github.com/division-sh/swarm/internal/providertriggers"
)

func main() {
	root := flag.String("pack-root", "packs", "explicit embedded pack inventory root")
	check := flag.Bool("check", false, "fail when a checked-in pack envelope is stale")
	flag.Parse()
	if err := stampInventory(strings.TrimSpace(*root), *check); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func stampInventory(root string, check bool) error {
	manifest, err := packartifact.LoadInventoryManifest(os.DirFS(root), packartifact.InventoryManifestFileName)
	if err != nil {
		return err
	}
	declared := map[string]packartifact.InventoryManifestPack{}
	for _, pack := range manifest.Packs {
		if pack.Type == packartifact.TypeTrigger {
			declared[pack.Path] = pack
		}
	}
	triggerRoot := filepath.Join(root, "provider-triggers")
	entries, err := os.ReadDir(triggerRoot)
	if err != nil {
		return fmt.Errorf("read provider-trigger inventory %q: %w", triggerRoot, err)
	}
	sort.Slice(entries, func(i, j int) bool { return entries[i].Name() < entries[j].Name() })
	count := 0
	for _, entry := range entries {
		if !entry.IsDir() {
			continue
		}
		relativeDir := filepath.ToSlash(filepath.Join("provider-triggers", entry.Name()))
		declaredPack, ok := declared[relativeDir]
		if !ok {
			return fmt.Errorf("provider-trigger directory %q is absent from %s", relativeDir, packartifact.InventoryManifestFileName)
		}
		dir := filepath.Join(triggerRoot, entry.Name())
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
		stampedEnvelope, stamped, err := providertriggers.StampPackEnvelope(envelopeBody, manifestBody)
		if err != nil {
			return fmt.Errorf("stamp %s: %w", dir, err)
		}
		if check {
			if !bytes.Equal(envelopeBody, stamped) {
				return fmt.Errorf("provider-trigger pack envelope %s is stale; run go run ./cmd/swarm-provider-trigger-pack-gen --pack-root %s", envelopePath, root)
			}
		} else if err := os.WriteFile(envelopePath, stamped, 0o644); err != nil {
			return fmt.Errorf("write %s: %w", envelopePath, err)
		}
		if stampedEnvelope.ID != declaredPack.ID || stampedEnvelope.Type != declaredPack.Type {
			return fmt.Errorf("provider-trigger pack %q contradicts explicit inventory id=%q type=%q", relativeDir, declaredPack.ID, declaredPack.Type)
		}
		delete(declared, relativeDir)
		count++
	}
	if count == 0 {
		return fmt.Errorf("provider-trigger inventory %q contains no packs", root)
	}
	if len(declared) > 0 {
		missing := make([]string, 0, len(declared))
		for path := range declared {
			missing = append(missing, path)
		}
		sort.Strings(missing)
		return fmt.Errorf("explicit inventory trigger paths have no generated directory: %s", strings.Join(missing, ", "))
	}
	return nil
}
