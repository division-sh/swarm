package main

import (
	_ "embed"
	"encoding/json"
	"fmt"
	"go/format"
	"strings"
)

// #1745/6207972014 appends current spellings without changing historical edits.
//
//go:embed state-spelling-1745.json
var stateSpellingEvidence1745 []byte

type stateSpellingEndpoint1745 struct{ File, PriorSHA256, CandidateSHA256 string }

func isStageSpellingEndpoint1745(c change, body []byte) (bool, error) {
	var endpoints []stateSpellingEndpoint1745
	if err := json.Unmarshal(stateSpellingEvidence1745, &endpoints); err != nil {
		return false, err
	}
	for _, endpoint := range endpoints {
		if endpoint.File != c.File || endpoint.PriorSHA256 != c.AfterHash || digest(body) != endpoint.CandidateSHA256 {
			continue
		}
		// Inversion must recover the exact historical output. Other edits,
		// including an independently changed decision, cannot mint a successor.
		prior := strings.ReplaceAll(string(body), "state.yaml", "entities.yaml")
		prior = strings.ReplaceAll(prior, `\n  _key:`, `\n  key:`)
		prior = strings.ReplaceAll(prior, "\n  _key:", "\n  key:")
		preimage := []byte(prior)
		var err error
		if strings.HasSuffix(c.File, ".go") {
			preimage, err = format.Source(preimage)
			if err != nil {
				return false, err
			}
		}
		if digest(preimage) != endpoint.PriorSHA256 {
			return false, fmt.Errorf("%s: spelling successor does not recover its historical output", c.File)
		}
		return true, nil
	}
	return false, nil
}
