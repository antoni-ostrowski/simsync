package htmx

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strings"

	"github.com/antoni-ostrowski/simsync/simsync"
	"github.com/redis/go-redis/v9"
)

const (
	MessageTypeEvent = iota
	MessageTypeMarkup
)

// payload stores for
//
// event type: event name that, client's htmx should listen on, so it can trigger refetch
// markup type: string of htmx markup that uses OOB to trigger swap on client, separated by \n
type HtmxMsg struct {
	Type    int8   `json:"type"`
	Payload string `json:"payload"`
}

func (m HtmxMsg) MarshalBinary() (data []byte, err error) {
	return json.Marshal(m)
}

func NewEventHtmxMsg(payload string) HtmxMsg {
	return HtmxMsg{
		Type:    MessageTypeEvent,
		Payload: payload,
	}
}

func NewMarkupHtmxMsg(payload string) HtmxMsg {
	return HtmxMsg{
		Type:    MessageTypeMarkup,
		Payload: payload,
	}
}

func CreateHtmxSSEHandler(engine *simsync.Engine, rdb *redis.Client) http.HandlerFunc {
	return func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher := w.(http.Flusher)
		userID := "aaa"
		pubsub := rdb.Subscribe(r.Context(), engine.GetUserChannel(userID))
		defer func() {
			pubsub.Close()
			engine.UntrackAll(context.Background(), userID)
		}()

		for redisMsg := range pubsub.Channel() {
			slog.Info("got new msg! ", "channel", redisMsg.Channel, "payload", redisMsg.Payload)

			var msg HtmxMsg

			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				slog.Error("failed to unmarshal somehow", "error", err.Error())
				continue
			}

			switch msg.Type {
			case MessageTypeEvent:
				slog.Info("telling client to refetch, via event", "event", msg.Payload)
				fmt.Fprintf(w, "event: %s\n", msg.Payload)
				fmt.Fprintf(w, "data: {}\n\n")
			case MessageTypeMarkup:
				slog.Info("seding markup to client to swap!", "markup", msg.Payload)
				lines := strings.Split(msg.Payload, "\n")
				for _, line := range lines {
					cleaned := strings.TrimRight(line, "\r")
					fmt.Fprintf(w, "data: %s\n", cleaned)
				}
				fmt.Fprintf(w, "\n")
			}
			flusher.Flush()

		}
	}
}
