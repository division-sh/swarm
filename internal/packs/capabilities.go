package packs

import "github.com/division-sh/swarm/internal/packmodel"

const (
	TriggerManifestFileName   = packmodel.TriggerManifestFileName
	ConnectorManifestFileName = packmodel.ConnectorManifestFileName
	ChannelManifestFileName   = packmodel.ChannelManifestFileName
	TypeTrigger               = packmodel.TypeTrigger
	TypeConnector             = packmodel.TypeConnector
	TypeChannel               = packmodel.TypeChannel
	ProvenancePlatform        = packmodel.ProvenancePlatform
	ProvenanceExternal        = packmodel.ProvenanceExternal
	ProvenanceProject         = packmodel.ProvenanceProject
)

type (
	Capabilities    = packmodel.Capabilities
	CanCapabilities = packmodel.CanCapabilities
	Requires        = packmodel.Requires
)

var ManifestHash = packmodel.ManifestHash
