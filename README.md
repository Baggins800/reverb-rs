# reverb-rs

A drop-in replacement for [Laravel Reverb](https://github.com/laravel/reverb), written in Rust on Tokio.

It speaks the same Pusher protocol on the same routes, reads the same `.env`, and returns
byte-identical frames — so `pusher-js`, Laravel Echo and `pusher-php-server` need no changes.

**Close to a 100% replacement, but read *What is not covered* first.** Everything a WebSocket
client or the Pusher HTTP API can observe is covered. Reverb's five Laravel events are relayed
back onto your event bus by the [companion package](laravel/), which restores Pulse, Telescope
and your own listeners. A few things genuinely cannot follow.

## Using it in a Laravel application

You keep `laravel/reverb` installed. It still provides `config/reverb.php`, the `reverb`
broadcast connection, the event classes and the Pulse cards. The only thing that changes is which
process serves WebSockets.

### 1. Build the server

```bash
git clone https://github.com/you/reverb-rs && cd reverb-rs
cargo build --release
```

The binary is `target/release/reverb-rs`. Copy it wherever you keep deployed binaries.

### 2. Change nothing in your application

`reverb-rs` reads the same environment variables as Reverb, so your existing `.env` already
configures it:

```dotenv
REVERB_APP_ID=123456
REVERB_APP_KEY=...
REVERB_APP_SECRET=...

REVERB_SERVER_HOST=0.0.0.0     # what the server binds to
REVERB_SERVER_PORT=8080
REVERB_HOST=reverb.example.com # what your app and browsers connect to
REVERB_PORT=443
REVERB_SCHEME=https
```

`config/broadcasting.php`, your Echo setup, `broadcast(new OrderShipped($order))`,
`->toOthers()`, `Broadcast::channel()` authorization and `/broadcasting/auth` all stay exactly
as they are. `tests/laravel.rs` drives Laravel's own broadcaster against `reverb-rs` to prove it:
broadcasts reach subscribers, `toOthers()` excludes the right socket, and the signatures
`/broadcasting/auth` hands to the browser are accepted for both private and presence channels.

### 3. Run it instead of `reverb:start`

Run it from the application root so it finds `.env`, or pass `--env-file`:

```bash
cd /var/www/my-app && /usr/local/bin/reverb-rs
```

Supervisor — replace the `command` in your existing Reverb program:

```ini
[program:reverb]
command=/usr/local/bin/reverb-rs
directory=/var/www/my-app        ; so .env is found
autostart=true
autorestart=true
user=www-data
stopsignal=TERM                  ; closes connections cleanly before exiting
stopwaitsecs=15
```

Or systemd:

```ini
[Service]
Type=simple
WorkingDirectory=/var/www/my-app
ExecStart=/usr/local/bin/reverb-rs
Restart=always
User=www-data
KillSignal=SIGTERM
TimeoutStopSec=15
```

Command-line flags mirror `reverb:start`, and win over the environment:

```
reverb-rs --host 0.0.0.0 --port 8080 --path /ws --hostname reverb.example.com --debug
```

### 4. Behind a reverse proxy

Unchanged from Reverb — terminate TLS at nginx and forward the upgrade:

```nginx
location / {
    proxy_pass http://127.0.0.1:8080;
    proxy_http_version 1.1;
    proxy_set_header Upgrade $http_upgrade;
    proxy_set_header Connection "upgrade";
    proxy_set_header Host $host;
    proxy_set_header Origin $http_origin;   # needed if you restrict allowed_origins
    proxy_read_timeout 3600s;
}
```

To terminate TLS in the server instead, set `REVERB_SERVER_TLS_CERT` and
`REVERB_SERVER_TLS_KEY`. For local development with Herd or Valet, setting `REVERB_HOST` to your
`.test` hostname is enough — the certificate is found the same way Reverb finds it.

### 5. Check it worked

```bash
curl http://127.0.0.1:8080/up                    # {"health":"OK"}
php artisan tinker
>>> broadcast(new App\Events\OrderShipped(Order::first()));
```

Your browser should receive the event. Reverb's own `php artisan reverb:restart` no longer
applies — send `SIGTERM` (or `supervisorctl restart reverb`), which closes connections cleanly
first.

### 6. Optional: keep Pulse, Telescope and your listeners

Reverb's Laravel events do not fire in a separate process. See [Observability](#observability)
to relay them back; the Pulse **Connections** card needs nothing at all.

### Rolling back

Nothing in your application changed, so rolling back is stopping `reverb-rs` and starting
`php artisan reverb:start` again. The one exception is a Redis-scaled cluster, which must be all
one implementation or the other — see [What is not covered](#what-is-not-covered).

## Why

Reverb runs on ReactPHP: one process, one thread, one event loop. Every broadcast frame is a
PHP array allocation and a `json_encode`. On a stock PHP install the loop is `StreamSelectLoop`,
which is `select(2)` and therefore capped at `FD_SETSIZE` (1024) descriptors.

`reverb-rs` keeps the same architecture on the outside and replaces the inside: a work-stealing
Tokio runtime across every core, one encoded frame shared by reference across all subscribers of
a channel, and no garbage collector.

The single largest win came from counting syscalls rather than guessing. Writing each frame
individually cost one `sendto` per message; coalescing the frames already queued for a connection
into one flush took 2,101 syscalls down to 428 for the same 2,000 messages, and CPU per message
with it. Nothing waits to be batched — only frames already sitting in the queue are gathered — so
an idle connection's latency is unchanged.

## Measured against the real thing

Full results and method are in **[benchmark.md](benchmark.md)**. The headline, from
500 subscribers on one channel receiving 1000 events of 100 bytes — 500,000 delivered
messages, median of three runs on 14 cores:

| | Laravel Reverb | reverb-rs | |
|---|---|---|---|
| Messages delivered | 138,006 msg/s | 1,646,182 msg/s | **11.9× faster** |
| Wire throughput | 194 Mbit/s | 2318 Mbit/s | **11.9×** |
| CPU per message | 5.98 µs | 1.82 µs | **3.3× less** |
| Latency p50 | 54.6 ms | 3.8 ms | **14.2× lower** |
| Memory idle | 53.0 MB | 6.7 MB | **7.9× smaller** |
| Memory per idle connection | 21.8 KB | 6.9 KB | **3.1× smaller** |

Two of those deserve a word. **CPU per message** is the figure that survives a change of
hardware: it is the work each server does to put one frame on one socket, and reverb-rs needs
3.3× less of it. The 11.9× in wall-clock terms is that efficiency multiplied by being able to
use more than one core, which Reverb by design cannot. **Wire throughput** is loopback, so read
it as a ceiling the server does not impose rather than a rate a real NIC would carry.

Reproduce it:

```bash
cd /path/to/reverb && composer install    # once
cargo build --release
cargo run --release --example benchmark -- --reverb-php /path/to/reverb
```

That starts and stops both servers itself, restarting them before every measurement so idle
memory is genuinely idle, and writes `benchmark.md`. To load-test a single server instead, use
`cargo run --release --example bench -- --addr 127.0.0.1:8080`.

PHP could not complete a 2000-connection run at all: stock PHP has no `ext-event`, so ReactPHP
falls back to `StreamSelectLoop` and `select(2)` caps it at `FD_SETSIZE` (1024) descriptors.
reverb-rs was tested to 5000 connections at 55 MB. That ceiling is a property of the PHP install
rather than of Reverb's design — installing `ext-event`, `ext-ev` or `ext-uv` lifts it, though
the server stays single-threaded either way.

## Running in a container

```bash
docker build -t reverb-rs .

docker run -d -p 8080:8080 \
  -e REVERB_APP_ID=... -e REVERB_APP_KEY=... -e REVERB_APP_SECRET=... \
  reverb-rs
```

A multi-stage build on `rust:bookworm` producing a `distroless/cc` image — **31.8 MB**, no shell,
running as `nonroot`. The binary is PID 1 and handles `SIGTERM` itself, so `docker stop` closes
client connections cleanly before the process exits.

Since the image has no shell or HTTP client, the binary doubles as its own health probe:

```bash
reverb-rs --healthcheck      # exit 0 if the configured port answers /up
```

which is what the image's `HEALTHCHECK` and the compose service use. `docker-compose.yml` brings
up the server with Redis, and carries the switches for horizontal scaling and the Laravel event
relay:

```bash
REVERB_APP_ID=... REVERB_APP_KEY=... REVERB_APP_SECRET=... docker compose up -d
```

Set `REVERB_SCALING_ENABLED=true` before scaling the service past one replica, or each replica
will only serve its own connections.

## Compatibility

Verified by `examples/conformance.rs`, which drives a fixed script against a running server and
prints every frame and response body with socket IDs masked. Run it against Laravel Reverb and
against `reverb-rs` and diff the transcripts:

```bash
cargo run --release --example conformance -- --addr 127.0.0.1:8080 > php.txt
cargo run --release --example conformance -- --addr 127.0.0.1:8081 > rust.txt
diff php.txt rust.txt
```

Of 77 transcript lines, two differ — both cosmetic, both listed under *Deliberate differences*.
Everything else is byte-identical, down to Symfony's `JSON_HEX_TAG|HEX_AMP|HEX_APOS|HEX_QUOT`
escaping of API bodies and the plain-text `Not found.` / `Method not allowed.` / `Payload too
large.` failure bodies.

Implemented in full:

- **Routes** — `GET /app/{appKey}`, `POST /apps/{appId}/events`, `POST /apps/{appId}/batch_events`,
  `GET /apps/{appId}/connections`, `GET /apps/{appId}/channels`, `GET /apps/{appId}/channels/{channel}`,
  `GET /apps/{appId}/channels/{channel}/users`,
  `POST /apps/{appId}/users/{userId}/terminate_connections`, `GET /up`, all under the configured path prefix.
- **Channels** — public, private, presence, cache, private-cache and presence-cache, including
  Reverb's prefix matching quirks (`cache` and `private` are matched without a trailing dash).
- **Auth** — HMAC-SHA256 subscription signatures and the full Pusher request-signing scheme,
  with body MD5 and the 600-second timestamp tolerance.
- **Client events** — `client-*` whispers with `all` / `members` / disabled policies, payload
  rebuilding and authenticated `user_id` injection.
- **Presence** — `member_added` / `member_removed`, de-duplication by user across connections,
  and the roster in `subscription_succeeded`.
- **Cache channels** — last-payload replay, `pusher:cache_miss`, and the rule that internal
  events never overwrite the cache.
- **Error codes** — 4001, 4004, 4009, 4200, 4201, 4301 with Reverb's exact messages.
- **Connection management** — origin allow-lists with wildcards, connection quotas, per-connection
  message rate limiting, max message size, ping/pong over both `pusher:ping` and WebSocket control
  frames, and the 60-second prune/ping sweep.
- **Horizontal scaling** — Redis pub/sub fan-out, cross-node `terminate_connections`, and
  distributed metrics gathering for the channel endpoints.
- **Laravel events** — all five relayed back to your application, with per-event opt-in and
  sampling for the two that fire per frame.
- **Request limits** — `max_request_size` enforced with Reverb's `Payload too large.` 413, and
  its `Not found.` / `Method not allowed.` bodies for unrouted paths and wrong methods.
- **TLS**, including Herd and Valet certificate discovery from `REVERB_HOST`; graceful shutdown
  on `SIGINT`/`SIGTERM`; multi-application tenancy.

## Configuration

Every `REVERB_*` and `REDIS_*` variable from `config/reverb.php` is read with the same name and
the same default, so an existing Laravel `.env` works as-is. Command-line flags mirror
`reverb:start`:

```
reverb-rs --host 0.0.0.0 --port 8080 --path /ws --hostname reverb.example.com --debug
```

For the multi-application `config` provider, export the `reverb.apps.apps` array to JSON and
point `REVERB_APPS_FILE` at it.

A few knobs have no Reverb equivalent. The defaults are what the benchmark settled on and are
worth leaving alone unless you are measuring:

| | |
|---|---|
| `REVERB_WS_READ_BUFFER` | Inbound framing buffer, preallocated per connection. Default 1024. |
| `REVERB_WS_WRITE_BUFFER` | Outbound bytes to accumulate before writing. Larger coalesces more frames per syscall but leaves more resident. Default 2048. |
| `REVERB_SEND_QUEUE_DEPTH` | Frames a slow client may fall behind before being disconnected. Default 1024. |
| `REVERB_LISTEN_BACKLOG` | `listen(2)` queue depth. Default 4096; too small costs reconnecting clients a one-second SYN retransmit. |
| `REVERB_MAINTENANCE_INTERVAL` | Seconds between ping/prune sweeps. Default 60, matching Reverb. |
| `REVERB_SERVER_TLS_CERT` / `_KEY` | Terminate TLS in the server. |
| `REVERB_APP_ALLOWED_ORIGINS` | Comma-separated origin allow-list. |

See `.env.example` for the rest.

## Observability

Reverb dispatches five events from inside the server process, and Pulse, Telescope and any
listeners you wrote hang off them. `reverb-rs` publishes the same five to Redis; the
[`reverb-rs/laravel`](laravel/) companion package re-dispatches them in your application as the
real `Laravel\Reverb\Events\*` objects, so all of that keeps working:

```dotenv
REVERB_EVENTS_ENABLED=true
REVERB_EVENTS_TYPES=all
```

```bash
php artisan reverb-rs:relay     # alongside your app, like a queue worker
```

The Pulse **Connections** card needs nothing at all: `ReverbConnections` runs inside
`pulse:check`, not inside the server, and reads `GET /apps/{id}/connections` over HTTP — which
`reverb-rs` serves identically. Only the **Messages** card needs the relay.

`message_sent` and `message_received` fire once per delivered frame, so they are counted always
and relayed only when asked for. Measured at 1000 subscribers:

| Setting | Fan-out | Events dropped |
|---|---|---|
| Relay off | 877k frames/s | — |
| Lifecycle events only (default) | 846k frames/s | none |
| `all`, sample rate 1 | 774k frames/s | 45%, Redis could not keep up |
| `all`, sample rate 0.05 | 828k frames/s | none |

The relay sheds load rather than slowing the server: on a saturated queue it drops events, warns
once, and reports the total as `events_dropped` on `GET /apps/{id}/counters`. Watch that counter
after turning message events on, and lower `REVERB_EVENTS_SAMPLE_RATE` if it climbs. At a few
thousand frames a second none of this applies — everything is relayed exactly.

`GET /apps/{appId}/counters` is a `reverb-rs` addition, signed like every other endpoint,
reporting cumulative `messages_sent`, `messages_received` and `events_dropped` for your own
dashboards.

## What is not covered

- **`php artisan reverb:restart`.** Reverb polls a Laravel cache key every five seconds. Send
  `SIGTERM` or `SIGINT` instead — connections are closed cleanly before the process exits.
- **`php artisan reverb:install`.** Scaffolding for a Laravel app; not applicable.
- **Listeners that write to a connection.** A relayed event carries a connection you can read —
  its ID, origin and application are faithful — but `send()`, `control()` and `terminate()` throw,
  because the socket lives in the server process. Broadcast to the channel, or use the HTTP API's
  `terminate_connections` endpoint.
- **Custom `ApplicationProvider` drivers.** Reverb resolves applications through an
  `ApplicationManager`, so a package can register a database-backed provider for dynamic
  tenancy. `reverb-rs` reads applications from the environment or `REVERB_APPS_FILE` only, both
  of which are static for the life of the process.
- **`config/reverb.php` is not read.** Configuration comes from the environment. That is
  equivalent for a stock config file, which is entirely `env()` calls — but if you hardcoded
  values there, or declared several applications inline, export them to `REVERB_APPS_FILE`.
- **The `options.tls` config array.** Reverb passes a PHP stream context (`local_cert`,
  `local_pk`, `verify_peer`, `passphrase`, …). Here TLS is `REVERB_SERVER_TLS_CERT` and
  `REVERB_SERVER_TLS_KEY`. Herd and Valet certificate discovery from `REVERB_HOST` does work,
  so local HTTPS development behaves the same.
- **Mixed-language Redis clusters.** Reverb PHP-serializes the `Application` object into its
  pub/sub envelope; `reverb-rs` sends the application ID as JSON. The envelope is otherwise the
  same shape, so a scaled cluster must be all-Rust or all-PHP — which matters only during a
  rolling migration. Fixing it is a contained change to `src/pubsub.rs`.

## Deliberate differences

Where behaviour diverges on purpose rather than by omission:

1. **`GET /apps/{id}/channels` orders its keys by name.** Reverb emits them in channel-creation
   order, which a sharded concurrent map cannot reproduce without giving up what makes broadcast
   fast. Sorting is at least deterministic. Same channels, same values; JSON object member order
   carries no meaning and no Pusher client depends on it.

2. **`Allow: GET,HEAD` where Reverb sends `Allow: GET`.** `HEAD` is implied by `GET` in HTTP and
   axum serves it; Reverb answers `HEAD` with a 405. Strictly more permissive, so nothing that
   worked before breaks.

3. **`max_connections` counts every socket, not just subscribed ones.** Reverb derives its count
   from channel membership, so a client that connects and never subscribes is invisible to the
   quota. A limit that does not limit is a bug; `reverb-rs` enforces the real number. The
   `/connections` endpoint still reports Reverb's channel-derived count, so the API is unchanged.

4. **Ping and prune sweep every socket.** Reverb only visits connections that joined a channel,
   so an idle unsubscribed client is never pinged and never reclaimed. `reverb-rs` sweeps all of
   them.

5. **A client that cannot keep up is disconnected.** Each connection has a bounded outbound queue
   (`REVERB_SEND_QUEUE_DEPTH`, 1024 frames). Overflowing it closes the connection rather than
   buffering without limit, which is what lets one stalled subscriber take down a PHP node.

## Tests

```bash
cargo test

# The scaling tests need Redis and are skipped without it.
REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 cargo test --test scaling

# The Laravel and relay tests additionally drive a real PHP process.
./laravel/tests/setup-test-app.sh /tmp/reverb-rs-test-app
REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 \
REVERB_TEST_PHP_APP=/tmp/reverb-rs-test-app \
  cargo test
```

123 tests, all in Rust. The PHP — both Laravel's broadcaster and the companion package — is
driven from here rather than carrying a second test framework. Unit tests cover protocol formatting, signing, channel classification and metrics
merging. `tests/protocol.rs` drives a live server through the Pusher handshake, all six channel
types, presence membership, cache replay, client events, rate limiting, origin checks and quotas.
`tests/api.rs` covers every HTTP endpoint and its failure modes. `tests/scaling.rs` stands up a
two-node cluster on one Redis channel and checks cross-node broadcast, socket exclusion,
cluster-wide metrics and remote termination. `tests/events.rs` asserts that all five Laravel
events are emitted at the moments Reverb emits them, and freezes the JSON envelope the PHP
package decodes. `tests/relay.rs` spawns the real companion package — Laravel boots, subscribes
to Redis and dispatches Reverb's own event classes — then asserts on what Laravel actually saw:
the five events, the rebuilt channel subclasses, that a relayed connection refuses to be written
to, that a throwing listener cannot stop the relay, and that unknown applications and malformed
payloads are discarded.

`tests/laravel.rs` is the one that matters for a migration: it runs Laravel's own
`Broadcast::connection('reverb')` against `reverb-rs` and asserts that broadcasts reach
subscribers, that `toOthers()` excludes the right socket, that the signatures
`/broadcasting/auth` returns are accepted for private and presence channels, and that the Pusher
SDK's info endpoints answer correctly.

The PHP-facing assertions were checked by mutation — breaking the companion package in three
places fails three different tests — so they are known to bite rather than merely pass. The expected frames are taken verbatim from Reverb's
own test suite.

## Layout

| File | |
|---|---|
| `src/server.rs` | Protocol core: lifecycle, subscriptions, fan-out, signing |
| `src/channel.rs` | The six channel flavours and their subscribers |
| `src/registry.rs` | Sharded per-application channel and socket registry |
| `src/conn.rs` | One connection: outbound queue, liveness, rate limit |
| `src/protocol.rs` | Pusher frame formatting and error codes |
| `src/http.rs` | HTTP API and Pusher request signing |
| `src/ws.rs` | WebSocket upgrade and the per-connection loop |
| `src/metrics.rs` | Channel statistics, local and merged across nodes |
| `src/pubsub.rs` | Redis scaling |
| `src/events.rs` | Counters and the event relay |
| `laravel/` | Companion package that re-dispatches the events |
| `examples/benchmark.rs` | Starts both servers and writes `benchmark.md` |
| `examples/bench.rs` | Load generator for a single server |
| `Dockerfile`, `docker-compose.yml` | Container build and a deployment with Redis |
| `src/config.rs` | `config/reverb.php`-compatible configuration |

## License

MIT, matching Laravel Reverb.
