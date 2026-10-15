package servedsettlement

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"
	"go/parser"
	"go/token"
	"strings"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const LookupPredecessor = "268be4d63cd63dfe3c0bf6cf96c3a6defed9477c"
const LookupPath = "tools/fixture-codemod/pipeline-observations/family145-served-publication-lookup-preimages.json"
const LookupDigest = "d826a092d7b7da7843cd43ff1a462563cc5752857a8c7ba7f3884eacc07f6e69"

func DecodeLookup(raw []byte) ([]runphysical.Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != LookupDigest {
		return nil, fmt.Errorf("served publication lookup whole-family manifest changed")
	}
	var pins []runphysical.Pin
	if err := json.Unmarshal(raw, &pins); err != nil || len(pins) != 11 {
		return nil, fmt.Errorf("served publication lookup declaration inventory changed: %v", err)
	}
	return pins, nil
}

var LookupChanges = map[string]Change{
	"waitServedEventPublishEventID":               {1, true},
	"servedEventPayloadObject":                    {1, false},
	"servedEventPublishEventEntityID":             {1, false},
	"assertServedDynamicAutoEmitPayload":          {1, false},
	"runServedDynamicAutoEmitProof":               {2, true},
	"servedEventPublishReceiptCountBySubscribers": {1, false},
}

func LookupOwnerBody(name, source string) (string, error) {
	if name == "runServedDynamicAutoEmitPostgresProof" {
		cut := "_, db, _ := installServeRuntimeEmptyPostgresTestStores"
		if strings.Count(source, cut) != 1 {
			return "", fmt.Errorf("dynamic native setup lost original constructor")
		}
		return strings.Replace(source, cut, "_, _, _ = installServeRuntimeEmptyPostgresTestStores", 1), nil
	}
	start, end, err := lookupSQLRange(name, source)
	if err != nil || start == end {
		return source, err
	}
	after := source[:start] + source[end:]
	switch name {
	case "waitServedEventPublishEventID":
		after = source[:start] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};` + source[end:]
		after = strings.Replace(after, "var eventID string\n\t\terr := db.QueryRowContext(context.Background(), query, args...).Scan(&eventID)", "eventID,err:=readServedRunNamedPublicationIdentity(context.Background(),reader,runID,eventName)", 1)
		after = strings.Replace(after, `"%s event id query failed: %v\n%s", backend, err, query`, `"%s event id query failed: %v", backend, err`, 1)
	case "servedEventPayloadObject":
		after = strings.Replace(after, "var raw string\n\tif err := db.QueryRowContext(context.Background(), query, args...).Scan(&raw); err != nil", "raw,err:=readServedPublicationPayloadText(context.Background(),reader,eventID);if err!=nil", 1)
	case "servedEventPublishEventEntityID":
		after = source[:start] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};` + source[end:]
		after = strings.Replace(after, "var entityID string\n\tif err := db.QueryRowContext(context.Background(), query, args...).Scan(&entityID); err != nil", "entityID,err:=readServedPublicationEntityIdentity(context.Background(),reader,eventID);if err!=nil", 1)
	case "assertServedDynamicAutoEmitPayload":
		after = source[:start] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};raw,err:=readServedPublicationPayloadText(context.Background(),reader,eventID);if err!=nil{t.Fatalf("%s load event payload %s: %v",backend,eventID,err)};` + source[end:]
	case "servedEventPublishReceiptCountBySubscribers":
		after = source[:start] + `if backend!="postgres"&&backend!="sqlite"{t.Fatalf("unknown proof backend %q",backend)};count,err:=readServedSubscriberReceiptCount(context.Background(),reader,eventID,subscriberIDs);if err!=nil{t.Fatalf("%s receipt subscriber count query failed: %v",backend,err)};` + source[end:]
	}
	if _, err := parser.ParseFile(token.NewFileSet(), "source.go", "package proof\n"+after, parser.AllErrors); err != nil {
		return "", err
	}
	return after, nil
}

func lookupSQLRange(name, source string) (int, int, error) {
	first, next := "", ""
	switch name {
	case "waitServedEventPublishEventID":
		first, next = "var query string", "deadline :="
	case "servedEventPayloadObject":
		first, next = "query :=", "var raw string"
	case "servedEventPublishEventEntityID":
		first, next = "var query string", "var entityID string"
	case "assertServedDynamicAutoEmitPayload":
		first, next = "var raw string", "var payload map"
	case "servedEventPublishReceiptCountBySubscribers":
		first, next = "var query string", "return count"
	default:
		return 0, 0, nil
	}
	start, end := strings.Index(source, first), strings.Index(source, next)
	if start < 0 || end <= start || strings.Count(source, next) != 1 {
		return 0, 0, fmt.Errorf("served publication lookup lost its finite query/read cut: %s", name)
	}
	return start, end, nil
}
