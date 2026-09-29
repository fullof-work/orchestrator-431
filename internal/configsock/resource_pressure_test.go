package configsock

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"path/filepath"
	"sync/atomic"
	"testing"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

type pressureAdminStub struct{ calls atomic.Int32 }

func (s *pressureAdminStub) ResourcePressureStatus(context.Context) (nodectl.PressureStatus, error) {
	s.calls.Add(1)
	return nodectl.PressureStatus{Enabled: true, PressureSnapshot: nodectl.PressureSnapshot{PressureRecord: nodectl.PressureRecord{Zone: nodectl.ZoneCritical, Reason: "recovery_obligation", Version: 4}, RawZone: nodectl.ZoneGreen, Pending: 1, Paused: 1}}, nil
}

func TestPressureAdminAuthenticatesAndReturnsEffectiveZone(t *testing.T) {
	pidfile := filepath.Join(t.TempDir(), "admin.pids")
	if err := os.WriteFile(pidfile, []byte("99999999\n"), 0600); err != nil {
		t.Fatal(err)
	}
	admin := &pressureAdminStub{}
	_, client := startTestServer(t, Deps{ResourcePressureAdmin: admin, AdminPidfile: pidfile})
	resp, err := client.Get("http://unix" + PathAdminResourcePressure)
	if err != nil {
		t.Fatal(err)
	}
	resp.Body.Close()
	if resp.StatusCode != http.StatusForbidden || admin.calls.Load() != 0 {
		t.Fatalf("admin gate: status=%d calls=%d", resp.StatusCode, admin.calls.Load())
	}
	if err := os.WriteFile(pidfile, []byte(fmt.Sprintf("%d\n", os.Getpid())), 0600); err != nil {
		t.Fatal(err)
	}
	resp, err = client.Get("http://unix" + PathAdminResourcePressure)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var status nodectl.PressureStatus
	if err := json.NewDecoder(resp.Body).Decode(&status); err != nil {
		t.Fatal(err)
	}
	if resp.StatusCode != http.StatusOK || status.Zone != nodectl.ZoneCritical || status.RawZone != nodectl.ZoneGreen || status.Pending != 1 {
		t.Fatalf("authoritative pressure=%+v HTTP=%d", status, resp.StatusCode)
	}
}
