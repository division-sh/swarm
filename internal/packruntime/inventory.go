package packruntime

import (
	"fmt"

	"github.com/division-sh/swarm/internal/packartifact"
	"github.com/division-sh/swarm/internal/packs"
)

func LoadChannelPacks(inventory *packartifact.EffectivePackInventory, runningPlatformVersion string) ([]packs.LoadedChannelPack, error) {
	if inventory == nil {
		return nil, fmt.Errorf("effective pack inventory is required")
	}
	entries := inventory.EntriesByType(packartifact.TypeChannel)
	loaded := make([]packs.LoadedChannelPack, 0, len(entries))
	for _, entry := range entries {
		pack, err := packs.LoadChannelPackFS(entry.FileSystem(), ".", runningPlatformVersion)
		if err != nil {
			return nil, fmt.Errorf("load effective channel pack %q: %w", entry.ID(), err)
		}
		pack.Directory = entry.Directory()
		pack.Source = packs.MustPackSource(entry.Source(), entry.ID())
		loaded = append(loaded, pack)
	}
	return loaded, nil
}
