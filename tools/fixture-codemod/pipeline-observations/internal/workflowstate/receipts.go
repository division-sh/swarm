package workflowstate

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"os"
)

const Path = "tools/fixture-codemod/pipeline-observations/family145-workflow-state-preimages.json"
const Digest = "25d031ef995292f317b42fcd901405ca22945818948369ef40a0b4735540cb1f"
const Predecessor = "b44b2ff3711b3dc8e65ce278edaeea49594153be"
const Calls = 28
const Bodies = 12

type Pin struct {
	File, Function, Before, After, SHA256 string
	Calls                                 int
}

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("workflow state complete source manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != Bodies {
		return nil, fmt.Errorf("workflow state declaration inventory changed")
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
