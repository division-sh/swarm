package packartifact

import (
	platformpacks "github.com/division-sh/swarm/packs"
)

func LoadEmbeddedPlatformPackInventory(runningPlatformVersion string) (*PlatformPackInventory, error) {
	return LoadPlatformPackInventoryFS(platformpacks.FS(), InventoryManifestFileName, runningPlatformVersion, SelectionEmbedded)
}
