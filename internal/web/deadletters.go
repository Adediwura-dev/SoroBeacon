package web

import (
	"net/http"
	"strconv"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/sorotrail/sorobeacon/internal/notify"
	"github.com/sorotrail/sorobeacon/internal/store"
)

type DeadLetterViewModel struct {
	ID           int64
	AlertID      int64
	ChannelID    int64
	ChannelName  string
	LastError    string
	AttemptCount int
	LastStatus   int
	CreatedAt    string
}

func (s *Server) listDeadLetters(w http.ResponseWriter, r *http.Request) {
	var filter store.DeadLetterFilter
	if q := r.URL.Query().Get("channel_id"); q != "" {
		if id, err := strconv.ParseInt(q, 10, 64); err == nil {
			filter.ChannelID = id
		}
	}
	if q := r.URL.Query().Get("alert_id"); q != "" {
		if id, err := strconv.ParseInt(q, 10, 64); err == nil {
			filter.AlertID = id
		}
	}
	filter.Limit = 100

	letters, err := s.store.ListDeadLetters(r.Context(), filter)
	if err != nil {
		s.renderError(w, r, http.StatusInternalServerError, err.Error())
		return
	}

	vms := make([]DeadLetterViewModel, 0, len(letters))
	for _, dl := range letters {
		chName := strconv.FormatInt(dl.ChannelID, 10)
		if ch, err := s.store.GetChannel(r.Context(), dl.ChannelID); err == nil && ch != nil {
			chName = ch.Name
		}
		vms = append(vms, DeadLetterViewModel{
			ID:           dl.ID,
			AlertID:      dl.AlertID,
			ChannelID:    dl.ChannelID,
			ChannelName:  chName,
			LastError:    dl.LastError,
			AttemptCount: dl.AttemptCount,
			LastStatus:   dl.LastStatus,
			CreatedAt:    dl.CreatedAt.UTC().Format("2006-01-02 15:04:05 UTC"),
		})
	}

	data := map[string]any{
		"DeadLetters": vms,
	}
	s.render(w, r, "deadletters.html", data)
}

func (s *Server) redriveDeadLetter(w http.ResponseWriter, r *http.Request) {
	id, err := strconv.ParseInt(chi.URLParam(r, "id"), 10, 64)
	if err != nil {
		s.renderError(w, r, http.StatusBadRequest, "malformed dead letter id")
		return
	}

	dl, err := s.store.GetDeadLetter(r.Context(), id)
	if err != nil {
		s.renderError(w, r, http.StatusNotFound, "dead letter not found")
		return
	}

	alert, err := s.store.GetAlert(r.Context(), dl.AlertID)
	if err != nil {
		s.renderError(w, r, http.StatusNotFound, "alert not found")
		return
	}

	ch, err := s.store.GetChannel(r.Context(), dl.ChannelID)
	if err != nil {
		s.renderError(w, r, http.StatusNotFound, "channel not found")
		return
	}

	dispatcher := notify.NewDispatcher(s.store, s.factory, s.log)
	notifyAlert := notifyAlertFromStore(r.Context(), s.store, *alert)

	attempts, _ := s.store.ListDeliveryAttempts(r.Context(), alert.ID, "")
	var lastTry time.Time
	for _, att := range attempts {
		if att.ChannelID == ch.ID && att.AttemptedAt.After(lastTry) {
			lastTry = att.AttemptedAt
		}
	}

	ctx := r.Context()
	dispatcher.Dispatch(ctx, notifyAlert)

	_ = s.store.DeleteDeadLetter(ctx, dl.ID)

	http.Redirect(w, r, "/dead-letters", http.StatusSeeOther)
}
