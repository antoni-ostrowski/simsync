package htmx

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/antoni-ostrowski/simsync"
)

type Profile interface {
	isProfile()
}

// used to send htmx events, so client can listen on them, and refetch if triggered
type EventProfile struct {
	EventName string
}

func (EventProfile) isProfile() {}

// used to declare htmx markup with OOB swap true, to modify anything on the client
type MarkupProfile struct {
	RenderFn func(ctx context.Context, value string) (string, error)
}

func (MarkupProfile) isProfile() {}

type Registry = map[string]Profile

func CreateHtmxSSEHandler(engine *simsync.Engine, registry Registry) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher := w.(http.Flusher)
		userID := "aaa"
		channel, close := engine.Backend.Subscribe(r.Context(), engine.GetUserChannel(userID))
		defer func() {
			close()
			engine.UntrackAll(context.Background(), userID)
		}()
		fmt.Fprintf(w, ": ok\n\n")
		flusher.Flush()
		slog.Info("got to see handler")

		for coreMsg := range channel {
			slog.Info("got new msg!", "value", coreMsg.Value)
			profile, ok := registry[coreMsg.Resource]
			if !ok {
				slog.Warn("no profile found for resource", "resource", coreMsg.Resource)
				continue
			}

			switch p := profile.(type) {
			case EventProfile:
				slog.Info("handling the event type")
				fmt.Fprintf(w, "event: %s\n", p.EventName)
				fmt.Fprintf(w, "data: {}\n\n")

			case MarkupProfile:
				slog.Info("handling the markup")
				if p.RenderFn == nil {
					slog.Error("RenderFn is nil for markup mode", "resource", coreMsg.Resource)
					continue
				}

				htmlContent, err := p.RenderFn(r.Context(), coreMsg.Value)
				if err != nil {
					slog.Error("RenderFn error", "error", err.Error(), "resource", coreMsg.Resource)
					continue
				}

				lines := strings.Split(htmlContent, "\n")
				for _, line := range lines {
					fmt.Fprintf(w, "data: %s\n", strings.TrimRight(line, "\r"))
				}
				fmt.Fprintf(w, "\n")

			default:
				slog.Error("unknown profile variant encountered", "resource", coreMsg.Resource)
				panic("incorrect profile variant!? htmx handler switch failed")
			}

			flusher.Flush()
		}
	}
}
