package nodectl

// PressureStatus is the local admin projection. It carries no credentials,
// snapshot contents, or placement instructions. Durations are nanoseconds.
type PressureStatus struct {
	Enabled       bool   `json:"enabled"`
	WorkerBlocked string `json:"worker_blocked,omitempty"`
	PressureSnapshot
	Sandboxes []SandboxPressureStatus   `json:"sandboxes"`
	Recent    []PressureOperationResult `json:"recent_operations,omitempty"`
}

type SandboxPressureStatus struct {
	ID                   string `json:"sandbox_id"`
	State                string `json:"state"`
	PauseReason          string `json:"pause_reason,omitempty"`
	RecoveryObligation   bool   `json:"recovery_obligation"`
	Version              uint64 `json:"pressure_version"`
	RunningSinceUnixNano int64  `json:"running_since_unix_nano,omitempty"`
	WaitingSinceUnixNano int64  `json:"waiting_since_unix_nano,omitempty"`
}

type PressureOperationResult struct {
	SandboxID  string `json:"sandbox_id"`
	Operation  string `json:"operation"`
	Result     string `json:"result"`
	AtUnixNano int64  `json:"at_unix_nano"`
}
