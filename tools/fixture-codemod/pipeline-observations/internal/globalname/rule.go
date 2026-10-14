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
	{"internal/serveapp/scenario_root_setup_test.go", "TestServedRootScenarioCLISetupUsesRuntimeCoordinates", []runphysical.Edit{
		expression("published", "rt.DB.QueryRowContext(context.Background(), `SELECT COUNT(*) FROM events WHERE event_name = 'widget.scored'`)", "readServedGlobalEventNameCount(context.Background(), rt.ReceiverStateReader, \"widget.scored\")"),
	}, 1},
	{"internal/serveapp/reset_candidate_fence_test.go", "TestResetCandidateSetFencesConsumersWhileSecondPreparesBothStores", []runphysical.Edit{
		expression("bootEvents", "selectedStoreDatabaseForTest(t, stores).QueryRow(\"SELECT COUNT(*) FROM events WHERE event_name = 'platform.boot'\")", "readServedGlobalEventNameCount(context.Background(), requireReceiverProofStateReader(t, stores.RuntimeDeps().EventStore), \"platform.boot\")"),
	}, 1},
	{"internal/serveapp/inbound_admission_runtime_test.go", "runInboundAdmissionSupportedSurfacePolicyMatrix", []runphysical.Edit{
		expression("count", "storetest.DatabaseForTest(sqliteStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = ?`, eventName)", "storetest.CountEventNameStorage(context.Background(), sqliteStore, eventName)"),
		expression("count", "storetest.DatabaseForTest(postgresStore).QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = $1`, eventName)", "storetest.CountEventNameStorage(context.Background(), postgresStore, eventName)"),
	}, 2},
	{"internal/serveapp/standing_ingress_dormancy_supported_surface_test.go", "TestDormantIngressProvisionRestartSignedInputBothStores", []runphysical.Edit{
		{"var db *sql.DB", "var db *sql.DB\nvar observationLocation string"},
		{"path := filepath.Join(t.TempDir(), \"dormant-ingress.sqlite\")", "path := filepath.Join(t.TempDir(), \"dormant-ingress.sqlite\")\nobservationLocation = path"},
		{"db = opened", "db = opened\nobservationLocation = dsn"},
		{"process, endpoint := start()", "process, endpoint := start()\nnameReader := openChannelAnchorStorageAtLocation(t, backend, observationLocation)"},
		expression("published", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name = 'inbound.telegram'`)", "nameReader.CountEventNameStorage(context.Background(), \"inbound.telegram\")"),
	}, 1},
	{"internal/serveapp/channel_object_input_supported_surface_test.go", "proveObjectChannelChooserRestart", []runphysical.Edit{
		expression("count", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, 1},
	{"internal/serveapp/channel_object_input_supported_surface_test.go", "proveObjectChannelLostPrompt", []runphysical.Edit{
		expression("decisions", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, 1},
	{"internal/serveapp/channel_input_process_supported_surface_test.go", "proveChannelInputAuthorityRestart", []runphysical.Edit{
		expression("decisions", "db.QueryRow(`SELECT COUNT(*) FROM events WHERE event_name='mailbox.card_decided'`)", "channelAnchorStorageObservation(t, h).CountEventNameStorage(context.Background(), \"mailbox.card_decided\")"),
	}, 1},
}

func expression(destination, query, read string) runphysical.Edit {
	return runphysical.Edit{Before: query + ".Scan(&" + destination + ")", After: "func() error { var err error; " + destination + ",err=" + read + "; return err }()"}
}
