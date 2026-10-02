package htmx

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strings"

	"github.com/antoni-ostrowski/simsync"
)

// Profile is implemented by each profile type with its own SSE sending logic.
type Profile[T any] interface {
	Decode(raw json.RawMessage) (T, error)
	RenderSSE(ctx context.Context, w http.ResponseWriter, val T) error
}

// erasedProfile type-erases Profile[T] so different T can coexist in one Registry.
type erasedProfile struct {
	decode func(json.RawMessage) (any, error)
	render func(context.Context, http.ResponseWriter, any) error
}

func EraseProfile[T any](p Profile[T]) erasedProfile {
	return erasedProfile{
		decode: func(raw json.RawMessage) (any, error) { return p.Decode(raw) },
		render: func(ctx context.Context, w http.ResponseWriter, val any) error {
			return p.RenderSSE(ctx, w, val.(T))
		},
	}
}

type Registry = map[string]erasedProfile

// EventProfile sends an empty htmx event, client listens and refetches.
type EventProfile[T any] struct {
	EventName string
	DecodeFn  *func(json.RawMessage) (T, error)
}

func (p EventProfile[T]) Decode(raw json.RawMessage) (T, error) {
	if p.DecodeFn != nil {
		return (*p.DecodeFn)(raw)
	}
	var val T
	return val, json.Unmarshal(raw, &val)
}
func (p EventProfile[T]) RenderSSE(_ context.Context, w http.ResponseWriter, _ T) error {
	fmt.Fprintf(w, "event: %s\n", p.EventName)
	fmt.Fprintf(w, "data: {}\n\n")
	return nil
}

// NewEventProfile creates an EventProfile with default json.Unmarshal decoding.
func NewEventProfile[T any](eventName string) erasedProfile {
	return EraseProfile(EventProfile[T]{EventName: eventName})
}

// MarkupProfile renders HTML server-side and sends it as OOB swap data.
//
// if you want multiple fragments, separate them with \n
type MarkupProfile[T any] struct {
	DecodeFn *func(json.RawMessage) (T, error)
	RenderFn func(context.Context, T) (string, error)
}

func (p MarkupProfile[T]) Decode(raw json.RawMessage) (T, error) {
	if p.DecodeFn != nil {
		return (*p.DecodeFn)(raw)
	}
	var val T
	return val, json.Unmarshal(raw, &val)
}
func (p MarkupProfile[T]) RenderSSE(ctx context.Context, w http.ResponseWriter, val T) error {
	html, err := p.RenderFn(ctx, val)
	if err != nil {
		return err
	}
	for _, line := range strings.Split(html, "\n") {
		fmt.Fprintf(w, "data: %s\n", strings.TrimRight(line, "\r"))
	}
	fmt.Fprintf(w, "\n")
	return nil
}

// NewMarkupProfile creates a MarkupProfile with default json.Unmarshal decoding.
func NewMarkupProfile[T any](render func(context.Context, T) (string, error)) erasedProfile {
	return EraseProfile(MarkupProfile[T]{RenderFn: render})
}

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

			val, err := profile.decode(coreMsg.Value)
			if err != nil {
				slog.Error("decode failed", "error", err.Error(), "resource", coreMsg.Resource)
				continue
			}

			if err := profile.render(r.Context(), w, val); err != nil {
				slog.Error("render failed", "error", err.Error(), "resource", coreMsg.Resource)
				continue
			}

			flusher.Flush()
		}
	}
}

func RenderTemplToStr(ctx context.Context, render func(ctx context.Context, w io.Writer) error) (string, error) {
	var buf bytes.Buffer
	if err := render(ctx, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}
