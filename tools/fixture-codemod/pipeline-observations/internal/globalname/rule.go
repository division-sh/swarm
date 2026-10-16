package globalname

import (
	"crypto/sha256"
	"encoding/json"
	"fmt"

	"github.com/division-sh/swarm/tools/fixture-codemod/pipeline-observations/internal/runphysical"
)

const Predecessor = "55bc89e2adea0df46cec8c56aa179931396073c0"
const Path = "tools/fixture-codemod/pipeline-observations/family145-global-event-name-preimages.json"
const Digest = "9faffbd6ffd5e33f4f1dd3d92cec698def49a3923b81f5144bfdca0b9859d565"

type Pin = runphysical.Pin

func Decode(raw []byte) ([]Pin, error) {
	if fmt.Sprintf("%x", sha256.Sum256(raw)) != Digest {
		return nil, fmt.Errorf("global event name complete manifest changed")
	}
	var pins []Pin
	if err := json.Unmarshal(raw, &pins); err != nil {
		return nil, err
	}
	if len(pins) != 7 {
		return nil, fmt.Errorf("global event name complete declaration inventory changed")
	}
	return pins, nil
}

var Rules = []runphysical.Rule{
	{File: "internal/serveapp/scenario_root_setup_test.go", Function: "TestServedRootScenarioCLISetupUsesRuntimeCoordinates", Edits: []runphysical.Edit{
		expression("published", "rt.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE event_name = 'widget.scored'`)", "readServedGlobalEventNameCount(context.Background(), rt.ReceiverStateReader, \"widget.scored\")"),
	}, Calls: 1},
	{File: "internal/serveapp/reset_candidate_fence_test.go", Function: "TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores", Edits: []runphysical.Edit{
		expression("bootEvents", "selectedStoreDatabaseForTest(t, stores).QueryRow(\"SELECT COUNT(*) FROM events WHERE event_name = 'platform.boot'\")", "readServedGlobalEventNameCount(context.Background(), requireReceiverProofStateReader(t, stores.RuntimeDeps().EventStore), \"platform.boot\")"),
	}, Calls: 1},
	{File: "internal/serveapp/inbound_admission_runtime_test.go", Function: "runInboundAdmissionSupportedSurfacePolicyMatrix", Edits: []runphysical.Edit{
		expression("count", "storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = ?`, eventName)", "storetest.CountEventNameStorage(context.Background(), sqliteStore, eventName)"),
		expression("count", "storetest.DatabaseForTest(postgresStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, eventName)", "storetest.CountEventNameStorage(context.Background(), postgresStore, eventName)"),
	}, Calls: 2},
	{File: "internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", Function: "TestDormantIngressProvisionRestartSignedInputBothStores", Edits: []runphysical.Edit{
		{Before: "var db *sql.DB", After: "var db *sql.DB\nvar observationLocation string"},
		{Before: "path := filepath.Join(t.TempDir(), \"dormant-ingress.sqlite\")", After: "path := filepath.Join(t.TempDir(), \"dormant-ingress.sqlite\")\nobservationLocation = path"},
		{Before: "db = opened", After: "db = opened\nobservationLocation = dsn"},
		{Before: "process, endpoint := start()", After: "process, endpoint := start()\nnameReader := openChannelAnchorStorageAtLocation(t, backend, observationLocation)"},
		expression("published", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram'`)", "nameReader.CountEventNameStorage(context.Background(), \"inbound.telegram\")"),
	}, Calls: 1},
	{File: "internal/serveapp/channel_object_input_supported_surface_test.go", Function: "proveObjectChannelChooserRestart", Edits: []runphysical.Edit{
		expression("count", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, Calls: 1},
	{File: "internal/serveapp/channel_object_input_supported_surface_test.go", Function: "proveObjectChannelLostPrompt", Edits: []runphysical.Edit{
		expression("decisions", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, Calls: 1},
	{File: "internal/serveapp/channel_input_process_supported_surface_test.go", Function: "proveChannelInputAuthorityRestart", Edits: []runphysical.Edit{
		expression("decisions", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, Calls: 1},
}

func expression(destination, query, read string) runphysical.Edit {
	return runphysical.Edit{Before: query + ".Scan(&" + destination + ")", After: "func() error { var err error; " + destination + ",err=" + read + "; return err }()"}
}
