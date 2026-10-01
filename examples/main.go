package main

import (
	"context"
	"log/slog"
	"net/http"
	"strconv"

	"github.com/antoni-ostrowski/simsync/simsync"
	simsync_htmx "github.com/antoni-ostrowski/simsync/simsync/handlers/htmx"
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
	e.Invalidate(ctx, "counter", simsync_htmx.NewEventHtmxMsg("counter-event"))
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

	http.HandleFunc("GET /connect", simsync_htmx.CreateHtmxSSEHandler(engine, rdb))

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
