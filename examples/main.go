package main

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/antoni-ostrowski/simsync/simsync"
	"github.com/redis/go-redis/v9"
)

// this serves as app storage
// in real app it would access real db
type Store struct {
	counter int
}

func (s *Store) getCounter(ctx context.Context, e *simsync.Engine) int {
	e.Track(ctx, "stub", "counter")
	return s.counter
}

func (s *Store) setCounter(ctx context.Context, newCount int, e *simsync.Engine) {
	e.Invalidate(ctx, "counter", "")
	s.counter = newCount
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

	http.HandleFunc("GET /connect", func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/event-stream")
		w.Header().Set("Cache-Control", "no-cache")
		w.Header().Set("Connection", "keep-alive")

		flusher := w.(http.Flusher)
		for i := range 10 {
			fmt.Fprintf(w, "data: <h1>%v</h1>\n\n", i)
			flusher.Flush()
			time.Sleep(time.Second * 1)
		}

		// userID := getUserFromSession(r)
		// userID := ""
		//
		// // 1. Subscribe to the channel managed by your package
		// pubsub := rdb.Subscribe(r.Context(), engine.GetUserChannel(userID))
		//
		// // 2. The magic cleanup! If they close the tab, clear their active tracking immediately
		// defer func() {
		// 	pubsub.Close()
		// 	// Instant zero-leak cleanup using the reverse index we built
		// 	_ = engine.UntrackAll(context.Background(), userID)
		// }()
		//
		// flusher := w.(http.Flusher)
		// for msg := range pubsub.Channel() {
		// 	fmt.Fprintf(w, "event: %s\ndata: trigger\n\n", msg.Payload)
		// 	flusher.Flush()
		// }
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
