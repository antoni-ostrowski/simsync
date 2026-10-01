package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"strings"

	"github.com/antoni-ostrowski/simsync/simsync"
	"github.com/redis/go-redis/v9"
)

// this serves as app storage
// in real app it would access real db
type Store struct {
	counter int
}

func (s *Store) getCounter(ctx context.Context, e *simsync.Engine) int {
	e.Track(ctx, "aaa", "counter")
	return s.counter
}

func (s *Store) setCounter(ctx context.Context, newCount int, e *simsync.Engine) {
	s.counter = newCount
	var buf bytes.Buffer
	CounterOOB(newCount).Render(ctx, &buf)
	e.Invalidate(ctx, "counter", simsync.NewMarkupMessage(buf.String()))
}

func main() {
	rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
	engine := simsync.New(simsync.Config{
		RedisClient: rdb,
		Namespace:   "simsync",
	})
	_ = engine

	store := Store{counter: 0}

	http.HandleFunc("GET /", func(w http.ResponseWriter, r *http.Request) {
		val := store.getCounter(r.Context(), engine)
		if err := Page(val).Render(r.Context(), w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	http.HandleFunc("GET /counter", func(w http.ResponseWriter, r *http.Request) {
		val := store.getCounter(r.Context(), engine)
		if err := Counter(val).Render(r.Context(), w); err != nil {
			http.Error(w, err.Error(), http.StatusInternalServerError)
		}
	})

	http.HandleFunc("GET /connect", func(w http.ResponseWriter, r *http.Request) {
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

			var msg simsync.Message
			if err := json.Unmarshal([]byte(redisMsg.Payload), &msg); err != nil {
				slog.Error("failed to unmarshal somehow", "error", err.Error())
				continue
			}

			switch msg.Type {
			case simsync.MessageTypeEvent:
				slog.Info("telling client to refetch, via event", "event", msg.Payload)
				fmt.Fprintf(w, "event: %s\n", msg.Payload)
				fmt.Fprintf(w, "data: {}\n\n")
			case simsync.MessageTypeMarkup:
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
	})

	http.HandleFunc("POST /{count}", func(w http.ResponseWriter, r *http.Request) {
		str := r.PathValue("count")
		n, _ := strconv.ParseInt(str, 10, 64)
		slog.Info("setting new counter", "new", str)
		store.setCounter(r.Context(), int(n), engine)

		w.Write([]byte("set new counter"))
	})

	slog.Info("listening on :3000")
	if err := http.ListenAndServe(":3000", nil); err != nil {
		slog.Error("http server failed", "error", err.Error())
	}
}
