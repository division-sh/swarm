package platformpacks

import (
	"embed"
	"io/fs"
)

//go:embed inventory.yaml provider-triggers/*/*.yaml provider-connectors/*/*.yaml channels/*/*.yaml
var embedded embed.FS

// FS returns the binary-owned platform pack assets. Membership remains owned
// by inventory.yaml and is validated by internal/packartifact.
func FS() fs.FS {
	return embedded
}
