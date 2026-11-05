package identity

import (
	"fmt"
	"regexp"
	"strings"

	semver "github.com/Masterminds/semver/v3"
)

var packIDPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

type PackID struct {
	packValue string
}

func ParsePackID(raw string) (PackID, error) {
	value := raw
	if value == "" {
		return PackID{}, fmt.Errorf("pack id is required")
	}
	if value != strings.TrimSpace(value) {
		return PackID{}, fmt.Errorf("pack id %q is not canonical", raw)
	}
	if !packIDPattern.MatchString(value) {
		return PackID{}, fmt.Errorf("pack id %q is invalid", raw)
	}
	return PackID{packValue: value}, nil
}

func MustPackID(raw string) PackID {
	value, err := ParsePackID(raw)
	if err != nil {
		panic(err)
	}
	return value
}

func (i PackID) String() string { return i.packValue }
func (i PackID) Valid() bool    { return packIDPattern.MatchString(i.packValue) }
func (i PackID) Equal(other PackID) bool {
	return i.Valid() && other.Valid() && i.packValue == other.packValue
}

type PackVersion struct {
	packValue string
}

func ParsePackVersion(raw string) (PackVersion, error) {
	value := raw
	if value == "" {
		return PackVersion{}, fmt.Errorf("pack version is required")
	}
	if value != strings.TrimSpace(value) {
		return PackVersion{}, fmt.Errorf("pack version %q is not canonical", raw)
	}
	parsed, err := semver.StrictNewVersion(value)
	if err != nil {
		return PackVersion{}, fmt.Errorf("pack version %q is invalid semver: %w", raw, err)
	}
	if parsed.String() != value {
		return PackVersion{}, fmt.Errorf("pack version %q is not canonical", raw)
	}
	return PackVersion{packValue: value}, nil
}

func MustPackVersion(raw string) PackVersion {
	value, err := ParsePackVersion(raw)
	if err != nil {
		panic(err)
	}
	return value
}

func (v PackVersion) String() string { return v.packValue }
func (v PackVersion) Valid() bool {
	if v.packValue == "" {
		return false
	}
	parsed, err := semver.StrictNewVersion(v.packValue)
	return err == nil && parsed.String() == v.packValue
}
func (v PackVersion) Equal(other PackVersion) bool {
	return v.Valid() && other.Valid() && v.packValue == other.packValue
}
