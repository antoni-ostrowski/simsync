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

// Profile is an opaque SSE profile. Build one with Event or Markup.
type Profile struct {
	decode func(json.RawMessage) (any, error)
	render func(context.Context, http.ResponseWriter, any) error
}

type Registry map[string]Profile

// RegistryEntry pairs a resource name with its Profile.
// Construct with Event or Markup, collect with NewRegistry.
type RegistryEntry struct {
	resource string
	profile  Profile
}

// Event sends an empty htmx event, client listens and refetches.
// No payload decoding needed.
func Event(resource, eventName string) RegistryEntry {
	return RegistryEntry{
		resource: resource,
		profile: Profile{
			decode: func(json.RawMessage) (any, error) { return nil, nil },
			render: func(_ context.Context, w http.ResponseWriter, _ any) error {
				fmt.Fprintf(w, "event: %s\n", eventName)
				fmt.Fprintf(w, "data: {}\n\n")
				return nil
			},
		},
	}
}

// Markup renders HTML server-side and sends it as SSE data.
// Separate multiple fragments with \n.
func Markup[T any](resource string, render func(context.Context, T) (string, error)) RegistryEntry {
	return RegistryEntry{
		resource: resource,
		profile: Profile{
			decode: func(raw json.RawMessage) (any, error) {
				var v T
				return v, json.Unmarshal(raw, &v)
			},
			render: func(ctx context.Context, w http.ResponseWriter, val any) error {
				html, err := render(ctx, val.(T))
				if err != nil {
					return err
				}
				writeDataLines(w, html)
				return nil
			},
		},
	}
}

func NewRegistry(entries ...RegistryEntry) Registry {
	m := make(Registry, len(entries))
	for _, e := range entries {
		m[e.resource] = e.profile
	}
	return m
}

func writeDataLines(w http.ResponseWriter, html string) {
	for _, line := range strings.Split(html, "\n") {
		fmt.Fprintf(w, "data: %s\n", strings.TrimRight(line, "\r"))
	}
	fmt.Fprintf(w, "\n")
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

		for coreMsg := range channel {
			profile, ok := registry[coreMsg.Resource]
			if !ok {
				slog.Debug("no profile found for resource", "resource", coreMsg.Resource)
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

// takes render func and executes it to generate markup string
//
// render func matches templ .Render func signature
func RenderTemplToStr(ctx context.Context, render func(ctx context.Context, w io.Writer) error) (string, error) {
	var buf bytes.Buffer
	if err := render(ctx, &buf); err != nil {
		return "", err
	}
	return buf.String(), nil
}
