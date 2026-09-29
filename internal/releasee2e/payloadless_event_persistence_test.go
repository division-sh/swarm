package releasee2e

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"
)

func TestPayloadlessEventPublicPersistenceJourneyBothStores(t *testing.T) {
	dsn := strings.TrimSpace(os.Getenv(goldenPostgresEnv))
	if dsn == "" {
		t.Fatalf("%s is required for the both-store persistence journey", goldenPostgresEnv)
	}
	releaseRoot := goldenReleaseRoot(t)
	binary := buildReleaseBinary(t, releaseRoot)
	for _, backend := range []string{"sqlite", "postgres"} {
		t.Run(backend, func(t *testing.T) {
			root := filepath.Join(releaseRoot, backend)
			contracts := filepath.Join(root, "contracts")
			copyReleaseTree(t, filepath.Join(releaseE2ERepoRoot(t), "tests/tier8-boot-verification/test-boot-success"), contracts)
			// Comments and trailing spaces are part of the admitted source identity.
			const eventSource = "# A payload-less public ingress\ntask.requested:  # no fields\n"
			writeReleaseFile(t, filepath.Join(contracts, "events.yaml"), eventSource)
			store := goldenSQLiteStore(root)
			if backend == "postgres" {
				store = goldenPostgresStore(t, dsn)
			}
			config := filepath.Join(root, "swarm.yaml")
			writeReleaseFile(t, config, goldenRuntimeConfig(store))
			token := filepath.Join(root, "api-token")
			writeReleaseFile(t, token, goldenAPIToken+"\n")
			env := goldenProcessEnv(t, root, store.passwordEnv, 0)
			verify := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "verify", contracts, "--config", config, "--json")
			var verified struct {
				OK bool `json:"ok"`
			}
			if err := json.Unmarshal([]byte(verify.output), &verified); verify.err != nil || err != nil || !verified.OK {
				t.Fatalf("public verify: command=%v decode=%v\n%s", verify.err, err, verify.output)
			}
			describe := runReleaseCommand(t, goldenStartupTimeout, root, env, "", binary, "describe", contracts, "--config", config, "--json")
			var described struct {
				SourceHash string `json:"source_hash"`
			}
			if err := json.Unmarshal([]byte(describe.output), &described); describe.err != nil || err != nil || described.SourceHash == "" {
				t.Fatalf("public describe: command=%v decode=%v\n%s", describe.err, err, describe.output)
			}
			bundleHash := described.SourceHash
			start := func(source, hash string) *releaseServeProcess {
				t.Helper()
				process := startReleaseServe(t, releaseProcessSpec{
					BinaryPath: binary, WorkingDir: root, Source: source, BundleHash: hash, ConfigPath: config,
					Store: backend, TokenFile: token, Token: goldenAPIToken, Env: env,
				})
				ctx, cancel := context.WithTimeout(context.Background(), goldenStartupTimeout)
				defer cancel()
				if err := process.waitReady(ctx); err != nil {
					t.Fatalf("public serve readiness: %v\n%s", err, process.output.String())
				}
				if got := goldenServedBundleHash(t, process.rpc, "live"); got != bundleHash {
					t.Fatalf("served hash=%s, described source hash=%s", got, bundleHash)
				}
				return process
			}
			process := start(contracts, "")
			initialBlob := assertPayloadlessPersistedSource(t, root, store, bundleHash, []byte(eventSource))
			publish := func(key string) string {
				t.Helper()
				var published struct {
					RunID string `json:"run_id"`
				}
				ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
				defer cancel()
				if err := process.rpc.call(ctx, "event.publish", map[string]any{
					"bundle_hash": bundleHash, "event_name": "task.requested", "payload": map[string]any{},
					"emitter": "releasee2e", "idempotency_key": key,
				}, &published); err != nil || published.RunID == "" {
					t.Fatalf("payload-less ingress: result=%+v err=%v", published, err)
				}
				waitForGoldenTerminalRun(t, process, store, published.RunID, 30*time.Second)
				events, err := listGoldenEvents(ctx, process.rpc, published.RunID)
				if err != nil {
					t.Fatal(err)
				}
				assertGoldenExactPayload(t, goldenSingleNamedEvent(t, events, "task.requested"), map[string]any{})
				return published.RunID
			}
			initialRun := publish("payloadless-initial")
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			if err := os.RemoveAll(contracts); err != nil {
				t.Fatal(err)
			}
			process = start("", bundleHash)
			if restartedBlob := assertPayloadlessPersistedSource(t, root, store, bundleHash, []byte(eventSource)); !bytes.Equal(restartedBlob, initialBlob) {
				t.Fatal("hash-only restart changed the persisted logical source blob")
			}
			waitForGoldenTerminalRun(t, process, store, initialRun, 30*time.Second)
			if restartedRun := publish("payloadless-restarted"); restartedRun == initialRun {
				t.Fatal("post-restart ingress reused the initial run")
			}
			if err := process.stopAndWait(10 * time.Second); err != nil {
				t.Fatal(err)
			}
			t.Log("proof_surface=public verify/local serve/hash restart; exact source bytes/hash and terminal RPC readback")
		})
	}
}

func assertPayloadlessPersistedSource(t *testing.T, root string, store goldenStoreSelection, bundleHash string, eventSource []byte) []byte {
	t.Helper()
	db := store.diagnosticDB
	placeholder := "$1"
	if store.name == "sqlite" {
		var err error
		db, err = sql.Open("sqlite", filepath.Join(root, "runtime.db"))
		if err != nil {
			t.Fatal(err)
		}
		defer db.Close()
		placeholder = "?"
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	var hash string
	var blob []byte
	if err := db.QueryRowContext(ctx, "SELECT bundle_hash, source_blob FROM source_artifacts WHERE bundle_hash = "+placeholder,
		bundleHash).Scan(&hash, &blob); err != nil {
		t.Fatal(err)
	}
	digest := sha256.Sum256(blob)
	if hash != bundleHash || "bundle-v2:sha256:"+hex.EncodeToString(digest[:]) != hash {
		t.Fatal("selected-store blob does not match the public admitted source hash")
	}
	// Inspect only the spec's binary framing, not an in-process bundle loader.
	buffer := bytes.NewBuffer(blob)
	const prelude = "swarm-bundle-v2\x00"
	if string(buffer.Next(len(prelude))) != prelude {
		t.Fatal("invalid persisted source prelude")
	}
	var count uint64
	if err := binary.Read(buffer, binary.BigEndian, &count); err != nil || count == 0 || count > uint64(buffer.Len()) {
		t.Fatalf("invalid persisted member count: %d, %v", count, err)
	}
	readField := func() []byte {
		t.Helper()
		var size uint64
		if err := binary.Read(buffer, binary.BigEndian, &size); err != nil || size > uint64(buffer.Len()) {
			t.Fatalf("invalid persisted field size: %d, %v", size, err)
		}
		return buffer.Next(int(size))
	}
	found := 0
	for index := uint64(0); index < count; index++ {
		code, err := buffer.ReadByte()
		if err != nil {
			t.Fatal(err)
		}
		label, body := readField(), readField()
		if string(label) == "events.yaml" {
			found++
			if code != 1 || !bytes.Equal(body, eventSource) {
				t.Fatalf("persisted declaration bytes changed: %q", body)
			}
		}
	}
	if found != 1 || buffer.Len() != 0 {
		t.Fatalf("events.yaml occurrences=%d, trailing bytes=%d", found, buffer.Len())
	}
	return blob
}
