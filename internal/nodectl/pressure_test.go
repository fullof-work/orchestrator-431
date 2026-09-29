package nodectl

import (
	"testing"
	"time"
)

func pressureTestState(t *testing.T) (*State, *time.Time) {
	t.Helper()
	s := NewState(1000, 1000, 0, 0, Watermarks{LowFactor: .7, HighFactor: .85, EmergencyFactor: .05, StartupFactor: .5})
	now := time.Now()
	s.mu.Lock()
	s.initPressureLocked()
	s.pressure.clock = func() time.Time { return now }
	s.mu.Unlock()
	return s, &now
}

func TestPressureOperationMatrix(t *testing.T) {
	for _, zone := range []Zone{ZoneGreen, ZoneYellow, ZoneRed, ZoneCritical} {
		for _, op := range []Operation{OperationCreate, OperationResume, OperationRecovery} {
			s, _ := pressureTestState(t)
			if err := s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: zone, Version: 1}); err != nil {
				t.Fatal(err)
			}
			got := s.AnalyzeLaunch("launch", 100, LaunchAdmission{Operation: op})
			want := op == OperationRecovery || (op == OperationResume && zone != ZoneCritical) || (op == OperationCreate && (zone == ZoneGreen || zone == ZoneYellow))
			if (got.Status == OutcomeAdmitted) != want {
				t.Fatalf("zone=%s op=%d outcome=%+v", zone, op, got)
			}
		}
	}
	s, _ := pressureTestState(t)
	_ = s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 1})
	if oc := s.AnalyzeLaunch("adopted", 975, LaunchAdmission{Operation: OperationResume, SavedSource: true}); oc.Status != OutcomeLongTermReject {
		t.Fatalf("saved source acquired critical exemption: %+v", oc)
	}
}

func TestPressureObligationsAndStepwiseRelief(t *testing.T) {
	s, now := pressureTestState(t)
	s.ObserveObligation("a", 1, "capturing", false)
	s.ObserveObligation("a", 2, "paused", true)
	s.ObserveObligation("a", 2, "starting", false)
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.RawZone != ZoneGreen || p.Pending != 1 || p.Starting != 1 {
		t.Fatalf("starting cleared Q: %+v", p)
	}
	s.ObserveObligation("a", 3, "", true)
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.Pending != 0 || p.Cleanup != 1 {
		t.Fatalf("cleanup did not hold critical: %+v", p)
	}
	s.ObserveObligation("a", 3, "", false)
	*now = now.Add(5 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("critical exit=%+v", p)
	}
	*now = now.Add(time.Hour)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("downgrade skipped new hold: %+v", p)
	}
	*now = now.Add(30 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneYellow {
		t.Fatalf("red exit=%+v", p)
	}
	_ = s.PressureSnapshot()
	*now = now.Add(30 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("yellow exit=%+v", p)
	}
	s.ObserveObligation("a", 2, "paused", false)
	if p := s.PressureSnapshot(); p.Pending != 0 {
		t.Fatalf("stale obligation resurrected: %+v", p)
	}
}

func TestPressureDistinctFailureRounds(t *testing.T) {
	s, now := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "existing", Token: "existing", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 600, Stage: StageSettled})
	a := LaunchAdmission{Operation: OperationResume, Identity: "run-1", Accepted: true}
	for i := 0; i < 20; i++ {
		s.RecordAdmissionWait("resume", a, 500)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("duplicate requests counted: %+v", p)
	}
	for i := 0; i < 2; i++ {
		*now = now.Add(500 * time.Millisecond)
		s.RecordAdmissionWait("resume", a, 500)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical || p.PauseEligible {
		t.Fatalf("critical must precede Pause: %+v", p)
	}
	for i := 0; i < 3; i++ {
		*now = now.Add(500 * time.Millisecond)
		s.RecordAdmissionWait("resume", a, 500)
	}
	if !s.BeginPressurePause("existing", 1) {
		t.Fatal("qualified Pause rejected")
	}
	if s.BeginPressurePause("second", 1) {
		t.Fatal("old rounds paused another object")
	}
	if p := s.PressureSnapshot(); p.Pending != 1 || p.Capturing != 1 {
		t.Fatalf("capture absent from Q: %+v", p)
	}
}

func TestSavedSourceCompleteBudgetAndSerialStartup(t *testing.T) {
	for _, budget := range []uint64{400, 975} {
		s, _ := pressureTestState(t)
		restore := LaunchAdmission{Operation: OperationResume, SavedSource: true}
		if budget == 975 {
			if oc := s.AnalyzeLaunch("template", budget, LaunchAdmission{}); oc.RejectCode != "exceeds_startup_pool" {
				t.Fatalf("template got recovery budget: %+v", oc)
			}
		}
		r, _, err := s.Admit(AdmitSpec{SandboxID: "resume", Token: "t", Capacity: Resources{MemoryBytes: 1000}, InitialBudget: budget, Admission: &restore})
		if err != nil || r.InitialBudget != budget || s.AdmissionSnapshot().StartupInFlight != budget {
			t.Fatalf("complete budget not charged: %+v %v", r, err)
		}
		if oc := s.AnalyzeLaunch("other", 1, LaunchAdmission{Accepted: true}); oc.Status == OutcomeAdmitted {
			t.Fatal("concurrent startup entered saved-source lane")
		}
		if _, _, err := s.SetSettled("t", 0, time.Now()); err != nil {
			t.Fatal(err)
		}
		if s.AdmissionSnapshot().StartupInFlight != 0 || s.ResourceSnapshot().Reserved.MemoryBytes != budget {
			t.Fatal("Settled changed live charge or retained startup budget")
		}
	}
}

func TestStickyCriticalRuntimeHeadroomAndFinalGate(t *testing.T) {
	s, _ := pressureTestState(t)
	installReservationForTest(t, s, Reservation{SandboxID: "running", Token: "t", Capacity: Resources{MemoryBytes: 1000}, ReservationMemory: 100, Stage: StageSettled})
	s.ObserveObligation("paused", 1, "paused", false)
	a := NewAllocator(AllocatorPolicy{MemoryGrantPerSecBytes: 1000, MinGrantStep: 1, MaxGrantStep: 1000})
	got, found, err := s.ReconcileAndGrant("t", 100, 100, UrgencyNormal, a)
	if err != nil || !found || got.Decision.GrantedDelta != 100 {
		t.Fatalf("sticky critical blocked grow: %+v %v", got, err)
	}
	ordinary := LaunchAdmission{}
	if _, _, err := s.Admit(AdmitSpec{SandboxID: "new", Token: "new", Capacity: Resources{MemoryBytes: 1000}, InitialBudget: 10, Admission: &ordinary}); err == nil {
		t.Fatal("final gate admitted new create in critical")
	}
	if s.ResourceSnapshot().Reserved.MemoryBytes != 200 {
		t.Fatal("failed admission changed accounting")
	}
}

func TestPressureRestartAndImpossibleDemand(t *testing.T) {
	s, now := pressureTestState(t)
	for i := 0; i < 10; i++ {
		s.RecordAdmissionWait("impossible", LaunchAdmission{Operation: OperationResume}, 1001)
		*now = now.Add(time.Second)
	}
	if p := s.PressureSnapshot(); p.Zone != ZoneGreen {
		t.Fatalf("impossible demand escalated: %+v", p)
	}
	_ = s.ConfigurePressure(DefaultPressurePolicy(), PressureRecord{Zone: ZoneCritical, Version: 10, SinceUnix: 1})
	if p := s.PressureSnapshot(); p.Zone != ZoneCritical {
		t.Fatalf("wall time skipped restart hold: %+v", p)
	}
	*now = now.Add(5 * time.Second)
	if p := s.PressureSnapshot(); p.Zone != ZoneRed {
		t.Fatalf("restart exit did not pass red: %+v", p)
	}
}
