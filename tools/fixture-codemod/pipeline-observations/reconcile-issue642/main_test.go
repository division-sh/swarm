package main

import (
	"bytes"
	"encoding/json"
	"reflect"
	"testing"
)

func TestMergeReviewedRecipeSuccessorsPreservesThreeWayAuthority(t *testing.T) {
	for _, variant := range []string{"master only", "lane only", "same successor", "crossed successors", "lane history", "master history"} {
		t.Run(variant, func(t *testing.T) {
			base := []rowMap{{"Family": json.RawMessage(`"original"`), "Successor": json.RawMessage(`"before"`)}}
			var lane, master []rowMap
			decode(encode(base, true), &lane)
			decode(encode(base, true), &master)
			switch variant {
			case "master only":
				master[0]["Successor"] = json.RawMessage(`"master"`)
			case "lane only":
				lane[0]["Successor"] = json.RawMessage(`"lane"`)
			case "same successor":
				lane[0]["Successor"], master[0]["Successor"] = json.RawMessage(`"same"`), json.RawMessage(`"same"`)
			case "crossed successors":
				lane[0]["Successor"], master[0]["Successor"] = json.RawMessage(`"lane"`), json.RawMessage(`"master"`)
			case "lane history":
				lane[0]["Family"] = json.RawMessage(`"changed"`)
			case "master history":
				master[0]["Family"] = json.RawMessage(`"changed"`)
			}
			beforeBase, beforeMaster := encode(base, true), encode(master, true)
			rejected := false
			func() {
				defer func() { rejected = recover() != nil }()
				mergeReviewedRecipeSuccessors(base, lane, master)
			}()
			wantRefusal := variant == "crossed successors" || variant == "lane history" || variant == "master history"
			if rejected != wantRefusal {
				t.Fatalf("three-way refusal=%t want=%t", rejected, wantRefusal)
			}
			if !bytes.Equal(beforeBase, encode(base, true)) || !bytes.Equal(beforeMaster, encode(master, true)) {
				t.Fatal("merge rewrote a reviewed predecessor")
			}
			if !rejected {
				want := master[0]["Successor"]
				if variant == "lane only" {
					want = json.RawMessage(`"lane"`)
				}
				if !reflect.DeepEqual(lane[0]["Successor"], want) || !reflect.DeepEqual(lane[0]["Family"], base[0]["Family"]) {
					t.Fatal("merge lost its exact successor or immutable historical recipe")
				}
			}
		})
	}
}
