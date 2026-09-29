package packs

import (
	"encoding/json"
	"fmt"
	"sort"

	runtimecontracts "github.com/division-sh/swarm/internal/runtime/contracts"
)

// NativeInboxProfile is authored by the provider pack and frozen by its compiler.
// Scoped commands always prefer the declared language over that exact scope's
// fallback. Private launchers independently prefer the chat over the default.
type NativeInboxProfile struct {
	Kind                string   `yaml:"kind"`
	ClientLanguages     []string `yaml:"client_languages"`
	DirectLauncherRead  string   `yaml:"direct_launcher_read"`
	DefaultLauncherRead string   `yaml:"default_launcher_read"`
	CommandsLauncher    string   `yaml:"commands_launcher"`
	InheritedLauncher   string   `yaml:"inherited_launcher"`
}

type CompiledNativeInboxProfile struct {
	profile NativeInboxProfile
}

func compileNativeInboxProfile(raw *NativeInboxProfile, plan SatisfactionPlan) (*CompiledNativeInboxProfile, error) {
	if raw == nil || raw.Kind != "scoped_commands_v1" || raw.CommandsLauncher != "commands" ||
		raw.InheritedLauncher != "default" || raw.DirectLauncherRead == raw.DefaultLauncherRead {
		return nil, fmt.Errorf("native_inbox requires a scoped_commands_v1 profile with commands/default precedence and distinct launcher reads")
	}
	p := *raw
	p.ClientLanguages = append([]string(nil), raw.ClientLanguages...)
	sort.Strings(p.ClientLanguages)
	seen := map[string]bool{}
	for _, language := range p.ClientLanguages {
		if len(language) != 2 || language[0] < 'a' || language[0] > 'z' || language[1] < 'a' || language[1] > 'z' || seen[language] {
			return nil, fmt.Errorf("native_inbox.client_languages contains an invalid or repeated language %q", language)
		}
		seen[language] = true
	}
	if !seen["en"] || !seen["fr"] {
		return nil, fmt.Errorf("native_inbox must qualify English and French clients with English labels")
	}
	for _, name := range []string{p.DirectLauncherRead, p.DefaultLauncherRead} {
		operation, found := plan.operations[name]
		if !found || operation.effect != runtimecontracts.ActivityEffectClassReadOnly {
			return nil, fmt.Errorf("native_inbox launcher %q must be a compiled read-only operation", name)
		}
		launcher, found := operation.outputSchema.Property("launcher")
		if !found || launcher.Validate(p.CommandsLauncher) != nil || launcher.Validate(p.InheritedLauncher) != nil {
			return nil, fmt.Errorf("native_inbox launcher %q does not carry the finite semantic launcher contract", name)
		}
	}
	for _, name := range []string{"read_inbox_entry", "read_shared_inbox_entry"} {
		operation, found := plan.operations[name]
		if !found || operation.effect != runtimecontracts.ActivityEffectClassReadOnly {
			return nil, fmt.Errorf("native_inbox commands %q require a compiled read-only operation", name)
		}
		language, found := operation.inputSchema.Property("language_code")
		if !found || language.Validate("") != nil {
			return nil, fmt.Errorf("native_inbox commands %q omit explicit fallback language", name)
		}
		for _, selected := range p.ClientLanguages {
			if err := language.Validate(selected); err != nil {
				return nil, fmt.Errorf("native_inbox commands %q reject declared client language %q: %w", name, selected, err)
			}
		}
	}
	return &CompiledNativeInboxProfile{profile: p}, nil
}

func (p SatisfactionPlan) NativeInboxProfile() (CompiledNativeInboxProfile, error) {
	if p.nativeInbox == nil {
		return CompiledNativeInboxProfile{}, fmt.Errorf("compiled native inbox profile is missing")
	}
	return *p.nativeInbox, nil
}

func (p OutboundBindingPlan) NativeInboxProfile() (CompiledNativeInboxProfile, error) {
	return p.structural.NativeInboxProfile()
}

func (p CompiledNativeInboxProfile) ValidateLanguage(language string) error {
	for _, allowed := range p.profile.ClientLanguages {
		if language == allowed {
			return nil
		}
	}
	return fmt.Errorf("native inbox requires an explicitly selected supported client language, got %q", language)
}

func (p CompiledNativeInboxProfile) DirectLauncherRead() string { return p.profile.DirectLauncherRead }
func (p CompiledNativeInboxProfile) DefaultLauncherRead() string {
	return p.profile.DefaultLauncherRead
}
func (p CompiledNativeInboxProfile) InheritsLauncher(value string) bool {
	return value == p.profile.InheritedLauncher
}

func (p CompiledNativeInboxProfile) canonicalValue() map[string]any {
	return map[string]any{
		"kind": p.profile.Kind, "client_languages": append([]string(nil), p.profile.ClientLanguages...),
		"direct_launcher_read": p.profile.DirectLauncherRead, "default_launcher_read": p.profile.DefaultLauncherRead,
		"commands_launcher": p.profile.CommandsLauncher, "inherited_launcher": p.profile.InheritedLauncher,
	}
}

type NativeInboxReadback struct {
	SelectedCommands json.RawMessage
	FallbackCommands json.RawMessage
	ChatLauncher     string
	DefaultLauncher  string
	Shared           bool
}

// Qualify checks only effective settings. Irrelevant lower-precedence settings
// neither grant authority nor invalidate a usable exact-scope command.
func (p CompiledNativeInboxProfile) Qualify(language string, readback NativeInboxReadback, desired []byte) error {
	if err := p.ValidateLanguage(language); err != nil {
		return err
	}
	type command struct {
		Command     string `json:"command"`
		Description string `json:"description"`
	}
	var selected, expected []command
	if err := json.Unmarshal(desired, &expected); err != nil || len(expected) != 1 || expected[0].Command == "" || expected[0].Description == "" {
		return fmt.Errorf("native inbox desired command is not exact")
	}
	if len(readback.SelectedCommands) == 0 || string(readback.SelectedCommands) == "null" {
		return fmt.Errorf("native inbox selected-language readback is not an array")
	}
	if err := json.Unmarshal(readback.SelectedCommands, &selected); err != nil {
		return fmt.Errorf("native inbox selected-language readback is invalid: %w", err)
	}
	if len(selected) == 0 {
		if len(readback.FallbackCommands) == 0 || string(readback.FallbackCommands) == "null" {
			return fmt.Errorf("native inbox fallback readback is not an array")
		}
		if err := json.Unmarshal(readback.FallbackCommands, &selected); err != nil {
			return fmt.Errorf("native inbox fallback readback is invalid: %w", err)
		}
	}
	matches := 0
	for _, item := range selected {
		if item.Command == expected[0].Command {
			if item.Description != expected[0].Description {
				return fmt.Errorf("native inbox effective command has a conflicting label")
			}
			matches++
		}
	}
	if matches != 1 {
		return fmt.Errorf("native inbox effective command list does not contain the exact generation entry")
	}
	if !readback.Shared {
		launcher := readback.ChatLauncher
		if p.InheritsLauncher(launcher) {
			launcher = readback.DefaultLauncher
			if p.InheritsLauncher(launcher) {
				launcher = p.profile.CommandsLauncher
			}
		}
		if launcher != p.profile.CommandsLauncher {
			return fmt.Errorf("native inbox effective private launcher does not open commands")
		}
	}
	return nil
}
