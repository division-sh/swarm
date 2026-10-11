package sessionprovider

import (
	"bufio"
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"os"
	"os/exec"
	"path/filepath"
	"reflect"
	"testing"
	"time"
)

type prekeyProcessFixture struct {
	Path, Account, Phase string
}

func TestSDKPreKeyPreparationProcessDeathDurability(t *testing.T) {
	if encoded := os.Getenv("SWARM_PREKEY_PROCESS"); encoded != "" {
		var config prekeyProcessFixture
		if err := json.Unmarshal([]byte(encoded), &config); err != nil {
			t.Fatal(err)
		}
		_, owner := openSDKStoreFixture(t, config.Path)
		device, err := owner.CurrentDevice(context.Background(), config.Account)
		if err != nil || device == nil || device.ID.String() != config.Account {
			t.Fatal("process lost the exact SDK account", err)
		}
		prepare := func(ctx context.Context) error {
			keys, err := device.PreKeys.GetOrGenPreKeys(ctx, 812)
			if err != nil || len(keys) != 812 {
				return fmt.Errorf("genuine key batch: count=%d error=%w", len(keys), err)
			}
			if config.Phase == "before_commit" {
				fmt.Println("PREKEY_PROCESS_READY")
				select {}
			}
			return nil
		}
		if config.Phase == "before_commit" {
			err = device.EventBuffer.DoDecryptionTxn(context.Background(), prepare)
		} else if config.Phase == "after_commit" {
			err = prepare(context.Background())
		} else {
			t.Fatal("unknown prekey process boundary", config.Phase)
		}
		if err != nil {
			t.Fatal(err)
		}
		fmt.Println("PREKEY_PROCESS_READY")
		select {}
	}
	for _, phase := range []string{"before_commit", "after_commit"} {
		t.Run(phase, func(t *testing.T) {
			path := filepath.Join(t.TempDir(), "provider.db")
			fixture, owner := openSDKStoreFixture(t, path)
			device := newSDKDeviceFixture(t, owner)
			before, err := fixture.SDKPreKeyInventory(context.Background(), device.ID.String())
			if err != nil {
				t.Fatal(err)
			}
			if err := fixture.Close(); err != nil {
				t.Fatal(err)
			}
			killPrekeyProcessAtBoundary(t, prekeyProcessFixture{Path: path, Account: device.ID.String(), Phase: phase})
			observed, reopened := openSDKStoreFixture(t, path)
			after, err := observed.SDKPreKeyInventory(context.Background(), device.ID.String())
			if err != nil {
				t.Fatal(err)
			}
			if phase == "before_commit" {
				if !reflect.DeepEqual(before, after) {
					t.Fatal("process death committed unfinished SDK keys", len(before), len(after))
				}
			} else if len(after) != len(before)+812 || !reflect.DeepEqual(before, after[:len(before)]) {
				t.Fatal("process death lost or replaced committed SDK keys", len(before), len(after))
			}
			retained, err := reopened.CurrentDevice(context.Background(), device.ID.String())
			if err != nil || retained == nil {
				t.Fatal("reopen lost the original SDK account", err)
			}
			keys, err := retained.PreKeys.GetOrGenPreKeys(context.Background(), 812)
			if err != nil || len(keys) != 812 {
				t.Fatal("reopen could not complete genuine SDK preparation", len(keys), err)
			}
		})
	}
}

func killPrekeyProcessAtBoundary(t *testing.T, fixture prekeyProcessFixture) {
	t.Helper()
	encoded, err := json.Marshal(fixture)
	if err != nil {
		t.Fatal(err)
	}
	binary, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	child := exec.CommandContext(ctx, binary, "-test.run=^TestSDKPreKeyPreparationProcessDeathDurability$", "-test.timeout=30s")
	child.Env = append(os.Environ(), "SWARM_PREKEY_PROCESS="+string(encoded))
	var diagnostics bytes.Buffer
	child.Stderr = &diagnostics
	output, err := child.StdoutPipe()
	if err != nil {
		t.Fatal(err)
	}
	if err := child.Start(); err != nil {
		t.Fatal(err)
	}
	joined := false
	defer func() {
		if !joined {
			_ = child.Process.Kill()
			_ = child.Wait()
		}
	}()
	rows := bufio.NewScanner(output)
	for rows.Scan() {
		if rows.Text() == "PREKEY_PROCESS_READY" {
			if err := child.Process.Kill(); err != nil {
				t.Fatal(err)
			}
			err := child.Wait()
			joined = true
			if err == nil {
				t.Fatal("process-death proof returned success")
			}
			return
		}
	}
	err = child.Wait()
	joined = true
	t.Fatalf("SDK preparation did not reach %s: scan=%v exit=%v stderr=%s", fixture.Phase, rows.Err(), err, diagnostics.String())
}
