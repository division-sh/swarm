package packmodel

const (
	TriggerManifestFileName   = "trigger.yaml"
	ConnectorManifestFileName = "connector.yaml"
	ChannelManifestFileName   = "channel.yaml"
)

type BodyRole uint8

const (
	BodyTrigger BodyRole = iota + 1
	BodyConnector
	BodyChannel
)

func (r BodyRole) String() string {
	switch r {
	case BodyTrigger:
		return "trigger"
	case BodyConnector:
		return "connector"
	case BodyChannel:
		return "channel"
	default:
		return "invalid"
	}
}

func (r BodyRole) FileName() string {
	switch r {
	case BodyTrigger:
		return TriggerManifestFileName
	case BodyConnector:
		return ConnectorManifestFileName
	case BodyChannel:
		return ChannelManifestFileName
	default:
		return ""
	}
}

func BodyRoleForFile(name string) (BodyRole, bool) {
	for _, role := range []BodyRole{BodyTrigger, BodyConnector, BodyChannel} {
		if role.FileName() == name {
			return role, true
		}
	}
	return 0, false
}
