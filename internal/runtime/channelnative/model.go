package channelnative

import (
	"context"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/google/uuid"
)

const (
	Command     = "inbox"
	Description = "Open inbox"
)

type Admission struct {
	Provider                     string
	ResourceSlotID               string
	ConversationReference        string
	PrincipalID                  string
	InterfaceKey                 string
	BindingRevision              int64
	ActivationID                 string
	ActivationRevision           int64
	ContextPublicationGeneration int64
	PackID                       string
	PackVersion                  string
	PackManifestHash             string
	EntryContractHash            string
}

func (a Admission) Validate() error {
	if strings.TrimSpace(a.Provider) == "" ||
		!strings.HasPrefix(a.ResourceSlotID, a.Provider+":") ||
		strings.TrimSpace(a.ConversationReference) == "" ||
		uuid.Validate(a.PrincipalID) != nil || strings.TrimSpace(a.InterfaceKey) == "" ||
		a.BindingRevision < 1 || uuid.Validate(a.ActivationID) != nil ||
		a.ActivationRevision < 1 || a.ContextPublicationGeneration < 1 ||
		strings.TrimSpace(a.PackID) == "" || strings.TrimSpace(a.PackVersion) == "" ||
		strings.TrimSpace(a.PackManifestHash) == "" || strings.TrimSpace(a.EntryContractHash) == "" {
		return fmt.Errorf("native inbox setting admission is incomplete")
	}
	return nil
}

func DesiredCommands() ([]byte, error) {
	return canonicaljson.Bytes([]map[string]string{{"command": Command, "description": Description}})
}

type Setting struct {
	SettingID            string
	Provider             string
	ResourceSlotID       string
	ConversationRef      string
	EntryContractHash    string
	PrincipalID          string
	Generation           int64
	State                string
	InstallOperationID   string
	CurrentConsumerCount int64
}

type Store interface {
	AttachNativeInboxSetting(context.Context, Admission) (Setting, error)
	RetireStaleNativeInboxConsumers(context.Context) error
}
