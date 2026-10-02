# simsync
simsync (simple sync) is a simple sync library, targeting flow of propagating updates from server to clients. It's focused around idea of tracking resources. Inspired by convex mechanisms, this small lib provides similar benefits with core diffs:

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
e := simsync.New(simsync_redis.NewRedisBackend(rdb), time.Hour, "simsync")
```

Tracking resource. You pass resource that client has requested ("is viewing") and clientID.  

```go
resourceName := "project1"
clientID := "123"
e.Track(ctx, clientID, resourceName)
```


Invalidating resource. Call after you updated the resource in your source of truth (database or other storage) and want clients to receive the update.

```go
resourceName := "project1"
newValue := "some_value" 
// will send event to every client that is 'looking' at the resource
// StreamMessage takes resource name and json.RawMessage, you can send new value state, or other data needed in event handler
m, _ := simsync.NewStreamMessage("counter", CounterPayload{Count: newCount})
e.Invalidate(ctx, m)
```


### custom backends and handlers

Backend is interface used by main engine to store data and send out updates.

```go

type Backend interface {
	Storer
	PubSub
}

// handles realtime events
type PubSub interface {
	Subscribe(ctx context.Context, channel string) (<-chan StreamMessage, func())
	Publish(ctx context.Context, channel string, msg StreamMessage) error
}


// handles storage (KV, HashSets)
type Storer interface {
	HSet(ctx context.Context, key string, values ...any) error
	SAdd(ctx context.Context, key string, members ...any) error
	HKeys(ctx context.Context, key string) ([]string, error)
	SMembers(ctx context.Context, key string) ([]string, error)
	HDel(ctx context.Context, key string, fields ...string) error
	Del(ctx context.Context, keys ...string) error
	Expire(ctx context.Context, key string, expiration time.Duration) error
}

```


If you want, you can easly implement your own backend with specific storage or pubsub system. 

Handlers are also abstracted away, so you can implement the even propagation to clients however you like. Theres no concrete interface for handler yet, so to not force any structure, you have access to engine and can implement anything you like! Engine backend lets you subscribe and handle events.

### adapters

simsync has built in: 
- redis backend (`simsync/backends/redis`)
- htmx even handler based on SSE (`simsync/handlers/htmx`)

### demo

Two clients are viewing same resource and get notified when resource gets updated in storage.

https://github.com/user-attachments/assets/f3eeb18f-0c3a-4c1e-86c5-0dda254acf00


