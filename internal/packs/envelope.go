package packs

import (
	"io/fs"

	"github.com/division-sh/swarm/internal/packmodel"
)

const (
	EnvelopeFileName          = packmodel.EnvelopeFileName
	TriggerManifestFileName   = packmodel.TriggerManifestFileName
	ConnectorManifestFileName = packmodel.ConnectorManifestFileName
	ChannelManifestFileName   = packmodel.ChannelManifestFileName

	TypeTrigger   = packmodel.TypeTrigger
	TypeConnector = packmodel.TypeConnector
	TypeChannel   = packmodel.TypeChannel

	ProvenancePlatform = packmodel.ProvenancePlatform
	ProvenanceExternal = packmodel.ProvenanceExternal
	ProvenanceProject  = packmodel.ProvenanceProject
)

type (
	Envelope        = packmodel.Envelope
	Provenance      = packmodel.Provenance
	Capabilities    = packmodel.Capabilities
	CanCapabilities = packmodel.CanCapabilities
	Requires        = packmodel.Requires
	Loaded          = packmodel.Loaded
)

func Load(fsys fs.FS, dir, runningPlatformVersion string) (Loaded, error) {
	return packmodel.Load(fsys, dir, runningPlatformVersion)
}

var (
	ManifestFileNameForType = packmodel.ManifestFileNameForType
	ParseEnvelope           = packmodel.ParseEnvelope
	StampEnvelope           = packmodel.StampEnvelope
	ManifestHash            = packmodel.ManifestHash
	CapabilitiesEqual       = packmodel.CapabilitiesEqual
	RequiresEqual           = packmodel.RequiresEqual
)
