package channeldecision

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/channeleventcount"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-channel-decision-preimages.json"
const Digest = "4367ddcc177b787556595e62c066c7dd769fb69801305a2be3ab5069b7c40962"
const Predecessor = "d2dcbacad05db895e6cda5c1214ef5530f2b253e"
const Calls = 3
const Bodies = 5

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("channel decision complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("channel decision declaration inventory changed")
	}
	return pins, nil
}

func Previous(pins []Pin, file, function, current string) string {
	for _, pin := range pins {
		if pin.File == file && pin.Function == function && pin.After == current {
			return pin.Before
		}
	}
	return current
}

func ReadPrevious(file, function, current string) (string, error) {
	var err error
	current, err = channeleventcount.ReadPrevious(file, function, current)
	if err != nil {
		return "", err
	}
	raw, err := os.ReadFile(Path)
	if err != nil {
		return "", err
	}
	pins, err := Decode(raw)
	if err != nil {
		return "", err
	}
	return Previous(pins, file, function, current), nil
}
