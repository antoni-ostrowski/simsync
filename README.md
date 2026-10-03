# simsync

simsync tracks which clients are viewing which resources, and fans out update messages to just those clients when a resource changes. It doesn't store your data.

- everything is explicit, you control if you want resource to be tracked, or when to invalidate it.
- simsync doesn't care about how you store data, it only tracks *resources* and *who* is viewing them. 
- high extensibility, at it's core is engine struct, backend part and how you propagate events can be customized (checkout [adapters](#adapters)).

> see demo from `/example` [demo](#demo)
# installation

```bash
go get github.com/antoni-ostrowski/simsync
```

# usage
Checkout full example with simple htmx web app. (`/example`)

### overview

Init sync engine, pass backend implementation

```go

import (
    "github.com/antoni-ostrowski/simsync"
	"github.com/redis/go-redis/v9"
	simsync_redis "github.com/antoni-ostrowski/simsync/backends/redis"
) 

rdb := redis.NewClient(&redis.Options{Addr: "localhost:6379"})
// create engine, with TTL and prefix
e := simsync.New(simsync_redis.NewRedisBackend(rdb), time.Hour, "simsync")
```

Tracking resource. You pass resource that client has requested ("is viewing") and clientID.  

```go
clientID := "123"
e.Track(ctx, clientID, "counter")
```


Invalidating resource. Call after you updated the resource in your source of truth (database or other storage) and want clients to receive the update.

```go
// will send event to every client that is 'looking' at the resource
m := simsync.NewEventMessage("counter")
e.Invalidate(ctx, m)

```

Untracking. Call when the client's update stream disconnects (SSE close) so it stops receiving updates. The bundled htmx handler already does this for you; only needed in custom handlers. Use a fresh context, the request one is already cancelled at that point.

```go
e.UntrackAll(context.Background(), clientID)
```


### custom backends and handlers

Backend is the interface the engine uses to store viewer presence and deliver updates. See `simsync.go` (`Backend = Storer + PubSub`) for the exact definition — it's intentionally Redis-shaped (hash + set ops), so a custom backend emulates those semantics on whatever store/pubsub you use.

If you want, you can easily implement your own backend with specific storage or pubsub system.

Handlers are also abstracted away, so you can implement the event propagation to clients however you like. There's no concrete interface for handlers, so to not force any structure you get access to the engine and can implement anything you like! The engine lets you subscribe and handle events.

```go
ch, cleanup := engine.SubscribeClient(ctx, clientID)
defer cleanup()
for msg := range ch {
    json.NewEncoder(conn).Encode(msg) // or your own routing
}
```


### demo

Two clients are viewing same resource and get notified when resource gets updated in storage.

https://github.com/user-attachments/assets/f3eeb18f-0c3a-4c1e-86c5-0dda254acf00


### adapters

simsync has built in: 
- redis backend (`simsync/backends/redis`)
- htmx event handler based on SSE (`simsync/handlers/htmx`)

#### redis backend

All viewer state (who looks at what) and pub/sub delivery live in Redis. The Go server holds no viewer state itself, so it stays stateless: any replica can `Track`/`Invalidate`, which is what makes this horizontally scalable.

#### htmx handler

Two update styles per resource:
- **event** — pushes an empty htmx event over SSE, client refetches with `hx-get` + `hx-trigger`. Cheap, client decides what to reload.
- **markup** — renders HTML server-side and pushes it as SSE data. Pairs with htmx [OOB swap](https://htmx.org/attributes/hx-swap-oob/): include `hx-swap-oob="true"` in the markup and the server can update any element on the page without a client refetch.

```go
import simsync_htmx "github.com/antoni-ostrowski/simsync/handlers/htmx"

registry := simsync_htmx.NewRegistry(
	// 1. event: push empty htmx event, client refetches with hx-get + hx-trigger
	simsync_htmx.Event("counter", "counter-event"),

	// 2. markup: decode payload (must match NewStreamMessage type),
	// render HTML server-side and push it as SSE data
	simsync_htmx.Markup("counter", func(ctx context.Context, p CounterPayload) (string, error) {
		// plain string, no templ needed (add hx-swap-oob="true" for OOB swap):
		return fmt.Sprintf(`<p id="counter-display" hx-swap-oob="true">%d</p>`, p.Count), nil

		// or with helper for templ components:
		return simsync_htmx.RenderTemplToStr(ctx, Counter(p.Count).Render)
	}),
)

resolveClient := func(r *http.Request) (string, error) {
    // your auth logic to get clientID from req
	return "aaa", nil
}

http.HandleFunc("GET /connect", simsync_htmx.CreateHtmxSSEHandler(engine, htmxRegistry, resolveClient))

// invalidate to match:
// 1. event needs no payload
// will use the event name set in registry
m := simsync.NewEventMessage("counter")
e.Invalidate(ctx, m)

// 2. markup payload must match the Markup[T] type
m, err := simsync.NewStreamMessage("counter", CounterPayload{Count: newCount})
e.Invalidate(ctx, m)
```

Then in your htmx:

```html

<body hx-sse:connect="/connect">
	<!-- this is how you can listen on the events sent from SSE conn, with "hx-trigger" -->
	<p id="counter-display" hx-get="/counter" hx-trigger="counter-event from:body" hx-swap="outerHTML">resource { val }</p>
	<form hx-post="/counter" hx-swap="none">
		<input name="counter" type="number"/>
		<button type="submit">update counter</button>
	</form>
</body>

```

