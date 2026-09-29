package nodectl

import (
	"fmt"
	"os"
)

// ReadHostAvailable runs outside State.mu. This safety observation never
// replaces reservation math or estimates a snapshot's saved Budget.
func ReadHostAvailable() (uint64, error) {
	body, err := os.ReadFile("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	for _, line := range splitLines(body) {
		var label, unit string
		var value uint64
		if _, err := fmt.Sscanf(line, "%s %d %s", &label, &value, &unit); err == nil && label == "MemAvailable:" && unit == "kB" && value <= ^uint64(0)/1024 {
			return value * 1024, nil
		}
	}
	return 0, fmt.Errorf("MemAvailable is unavailable")
}

// ObserveHostAvailable uses the configured operational margin as the safety
// floor. It cannot grant any of that margin or manufacture a reservation.
func (s *State) ObserveHostAvailable(available uint64) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.initPressureLocked()
	margin := s.OperationalMargin.MemoryBytes
	if margin == 0 || available >= margin {
		s.clearDemandLocked("host-safety")
		return
	}
	s.failDemandLocked("host-safety", "", "host-operational-margin", 0, 0, false, s.pressure.clock())
}
