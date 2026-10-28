package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/platform"
	"github.com/division-sh/swarm/internal/providertriggers"
)

func main() {
	root := flag.String("pack-root", "packs", "embedded provider inventory root")
	flag.Bool("check", false, "validate the captured provider trigger bodies without writing files")
	flag.Parse()
	if err := checkInventory(*root); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
}

func checkInventory(root string) error {
	version, err := platform.PlatformVersion()
	if err != nil {
		return err
	}
	base, err := packartifact.LoadPlatformPackInventoryFS(os.DirFS(root), packartifact.InventoryManifestFileName, version, packartifact.SelectionEmbedded)
	if err != nil {
		return err
	}
	inventory, err := packartifact.NewEffectivePackInventory(base)
	if err != nil {
		return err
	}
	_, _, err = providertriggers.NewCatalogSnapshotFromInventory(inventory, version)
	return err
}
