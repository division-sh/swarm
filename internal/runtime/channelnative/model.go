package channelnative

import (
	"context"
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"strings"

	"github.com/division-sh/swarm/internal/runtime/canonicaljson"
	"github.com/division-sh/swarm/internal/runtime/plangeneration"
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
	PlanGeneration               plangeneration.Generation
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
		strings.TrimSpace(a.PackManifestHash) == "" || !a.PlanGeneration.Valid() ||
		strings.TrimSpace(a.EntryContractHash) == "" {
		return fmt.Errorf("native inbox setting admission is incomplete")
	}
	expected, err := EntryContractHash(a.PlanGeneration)
	if err != nil || a.EntryContractHash != expected {
		return fmt.Errorf("native inbox setting contract does not match compiled plan")
	}
	return nil
}

func DesiredCommands() ([]byte, error) {
	return canonicaljson.Bytes([]map[string]string{{"command": Command, "description": Description}})
}

func EntryContractHash(plan plangeneration.Generation) (string, error) {
	if !plan.Valid() {
		return "", fmt.Errorf("native inbox setting requires a compiled plan")
	}
	desired, err := DesiredCommands()
	if err != nil {
		return "", err
	}
	value, err := canonicaljson.Bytes(map[string]any{
		"kind": "native_inbox_chat_commands_v1", "plan_generation": plan.Diagnostic(),
		"scope_kind": "chat", "language_code": "", "commands": json.RawMessage(desired),
	})
	if err != nil {
		return "", err
	}
	return "sha256:" + fmt.Sprintf("%x", sha256.Sum256(value)), nil
}

func InstallOperationID(settingID string, generation int64) (string, error) {
	parsed, err := uuid.Parse(strings.TrimSpace(settingID))
	if err != nil || generation < 1 {
		return "", fmt.Errorf("native inbox install requires exact setting generation")
	}
	return uuid.NewSHA1(parsed, []byte(fmt.Sprintf("install:%d", generation))).String(), nil
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
	MarkNativeInboxSettingUnavailable(context.Context, string, int64) error
}
