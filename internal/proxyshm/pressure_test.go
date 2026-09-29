package proxyshm

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/kuasar-sandbox/orchestrator/internal/proxy"
	"github.com/kuasar-sandbox/orchestrator/internal/routesync"
)

func TestPressureAdoptionPublishesWithoutFalseActivationRollback(t *testing.T) {
	for _, service := range []string{"forward", "exec"} {
		t.Run(service, func(t *testing.T) {
			table, err := Create(filepath.Join(t.TempDir(), "routes.shm"), 16)
			if err != nil {
				t.Fatal(err)
			}
			defer table.Close()
			updates := &Updates{ch: make(chan struct{})}
			wakes := make(chan string, 4)
			worker := NewWorkerView(table, updates, func(sid string) { wakes <- sid }, 4*time.Second)
			entry := routesync.RouteEntry{SandboxID: "pressure", StableID: "stable-pressure", Profile: "e2b", State: routesync.StatePaused, EnvdUDS: "/old/envd.sock", EnvdAccessToken: "envd", ForwardAccessToken: "forward", ServiceSecret: "secret", PauseReason: "resource-pressure", ResourceObligation: true, PressureVersion: 7}
			table.BeginSync()
			if err := table.Upsert(entry); err != nil {
				t.Fatal(err)
			}
			table.Bookmark()
			_, _, initialRev := table.LookupRevision(entry.SandboxID)
			globalRev := table.Rev()
			ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
			defer cancel()
			done := make(chan error, 1)
			go func() {
				if service == "exec" {
					identity, ok := workerExecIdentity(entry, true)
					if !ok {
						done <- context.Canceled
						return
					}
					_, found, err := worker.ActivateExec(ctx, entry.SandboxID, identity)
					if err == nil && !found {
						err = context.Canceled
					}
					done <- err
				} else {
					binding, found, err := worker.LookupRoute(ctx, entry.SandboxID, proxy.LegacyTarget(49983))
					if err != nil || !found {
						done <- context.Canceled
						return
					}
					route, found, err := worker.ActivateRoute(ctx, binding)
					if err == nil && (!found || route.UDS != "/fresh/envd.sock") {
						err = context.Canceled
					}
					done <- err
				}
			}()
			select {
			case <-wakes:
			case <-time.After(time.Second):
				t.Fatal("authorized request did not Wake")
			}
			adopted := entry
			adopted.PauseReason = "explicit"
			adopted.ResourceObligation = false
			adopted.PressureVersion++
			if err := table.Upsert(adopted); err != nil {
				t.Fatal(err)
			}
			updates.bump()
			got, _, rev := table.LookupRevision(entry.SandboxID)
			if got.PauseReason != "explicit" || got.ResourceObligation || rev != initialRev || table.Rev() <= globalRev {
				t.Fatalf("intent publication/revision=%+v %d/%d", got, rev, initialRev)
			}
			select {
			case err := <-done:
				t.Fatalf("adoption falsely completed activation: %v", err)
			case <-time.After(100 * time.Millisecond):
			}
			// This is the same still-undelivered, authorized request. A later Wake
			// rechecks ordinary eligibility; there has been no backend request to replay.
			select {
			case <-wakes:
			case <-time.After(2 * time.Second):
				t.Fatal("paused request never rechecked eligibility")
			}
			adopted.State = routesync.StateStarting
			adopted.PressureVersion++
			if err := table.Upsert(adopted); err != nil {
				t.Fatal(err)
			}
			updates.bump()
			adopted.State = routesync.StateRunning
			adopted.EnvdUDS = "/fresh/envd.sock"
			adopted.PauseReason = ""
			adopted.PressureVersion++
			if err := table.Upsert(adopted); err != nil {
				t.Fatal(err)
			}
			updates.bump()
			select {
			case err := <-done:
				if err != nil {
					t.Fatal(err)
				}
			case <-time.After(time.Second):
				t.Fatal("new running endpoint did not unpark")
			}
			table.BeginSync()
			if err := table.Upsert(adopted); err != nil {
				t.Fatal(err)
			}
			table.Bookmark()
			got, found := table.Lookup(entry.SandboxID)
			if !found || got.State != routesync.StateRunning || got.ResourceObligation || got.PressureVersion != adopted.PressureVersion {
				t.Fatalf("full sync lost successor: %+v", got)
			}
		})
	}
}
