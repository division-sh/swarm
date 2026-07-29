package packidentity

import (
	"fmt"
	"regexp"
	"strings"

	semver "github.com/Masterminds/semver/v3"
)

var idPattern = regexp.MustCompile(`^[a-z0-9][a-z0-9._-]*$`)

type ID struct {
	value string
}

func ParseID(raw string) (ID, error) {
	value := raw
	if value == "" {
		return ID{}, fmt.Errorf("pack id is required")
	}
	if value != strings.TrimSpace(value) {
		return ID{}, fmt.Errorf("pack id %q is not canonical", raw)
	}
	if !idPattern.MatchString(value) {
		return ID{}, fmt.Errorf("pack id %q is invalid", raw)
	}
	return ID{value: value}, nil
}

func MustID(raw string) ID {
	value, err := ParseID(raw)
	if err != nil {
		panic(err)
	}
	return value
}

func (i ID) String() string { return i.value }
func (i ID) Valid() bool    { return idPattern.MatchString(i.value) }
func (i ID) Equal(other ID) bool {
	return i.Valid() && other.Valid() && i.value == other.value
}

type Version struct {
	value string
}

func ParseVersion(raw string) (Version, error) {
	value := raw
	if value == "" {
		return Version{}, fmt.Errorf("pack version is required")
	}
	if value != strings.TrimSpace(value) {
		return Version{}, fmt.Errorf("pack version %q is not canonical", raw)
	}
	parsed, err := semver.StrictNewVersion(value)
	if err != nil {
		return Version{}, fmt.Errorf("pack version %q is invalid semver: %w", raw, err)
	}
	if parsed.String() != value {
		return Version{}, fmt.Errorf("pack version %q is not canonical", raw)
	}
	return Version{value: value}, nil
}

func MustVersion(raw string) Version {
	value, err := ParseVersion(raw)
	if err != nil {
		panic(err)
	}
	return value
}

func (v Version) String() string { return v.value }
func (v Version) Valid() bool {
	if v.value == "" {
		return false
	}
	parsed, err := semver.StrictNewVersion(v.value)
	return err == nil && parsed.String() == v.value
}
func (v Version) Equal(other Version) bool {
	return v.Valid() && other.Valid() && v.value == other.value
}
