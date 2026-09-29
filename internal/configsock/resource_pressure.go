package configsock

import (
	"context"
	"net/http"

	"github.com/kuasar-sandbox/orchestrator/internal/nodectl"
)

const PathAdminResourcePressure = "/internal/admin/resource-pressure"

type ResourcePressureAdmin interface {
	ResourcePressureStatus(context.Context) (nodectl.PressureStatus, error)
}

func (s *Server) handleAdminResourcePressure(w http.ResponseWriter, r *http.Request) {
	peer, ok := peerFrom(r.Context())
	if !ok || !s.adminAuthed(peer) {
		writeJSON(w, http.StatusForbidden, map[string]string{"error": "not authorized (admin)"})
		return
	}
	status, err := s.deps.ResourcePressureAdmin.ResourcePressureStatus(r.Context())
	if err != nil {
		s.log.Warn("configsock resource pressure status", "err", err)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "internal error"})
		return
	}
	writeJSON(w, http.StatusOK, status)
}
