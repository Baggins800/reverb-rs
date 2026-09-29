# baggins800/reverb-rs

Relays [`reverb-rs`](../) server events onto your Laravel event bus.

Laravel Reverb dispatches five events from inside the server process. `reverb-rs` is a separate
process, so it publishes those events to Redis and this package re-dispatches them in your
application — as the real `Laravel\Reverb\Events\*` classes, so Pulse recorders, Telescope and
your own listeners work unchanged.

You still need `laravel/reverb` installed. You keep its config, its event classes, its Pulse
cards and its broadcaster; you just stop running `php artisan reverb:start`.

## Install

```bash
composer require baggins800/reverb-rs
php artisan reverb-rs:binary --build
```

`--build` compiles the Rust sources shipped with this package and needs a Rust toolchain.
Omitting it downloads a prebuilt release, but none have been published yet, so that path
currently fails. A `reverb-rs` already on `PATH` is used as-is.

## Commands

| | |
|---|---|
| `reverb-rs:start` | Start the server with this application's configuration. A drop-in for `reverb:start`. |
| `reverb-rs:binary` | Install the server binary — built from source with `--build`, downloaded otherwise. |
| `reverb-rs:config` | Print what `config/reverb.php` resolves to, as the server reads it. |
| `reverb-rs:relay` | Re-dispatch the server's events onto this application's event bus. |

## Relaying events

Turn the relay on:

```dotenv
REVERB_EVENTS_ENABLED=true
REVERB_EVENTS_TYPES=all          # or a subset, see below
```

Run the consumer alongside your app, the way you run a queue worker:

```bash
php artisan reverb-rs:relay
```

Supervise it like any other long-running command. One process is enough; running several would
dispatch every event once per process.

## Which events to relay

`REVERB_EVENTS_TYPES` takes a comma-separated list of `message_sent`, `message_received`,
`channel_created`, `channel_removed`, `connection_pruned` — or `all`, or `none`.

The default is the three lifecycle events, which fire rarely and cost nothing measurable. The two
message events fire **once per delivered frame**, so relaying them has a real price:

| Setting | Fan-out (500 subscribers) | Events dropped |
|---|---|---|
| Relay off | 1,552k msg/s | — |
| Lifecycle only (default) | 1,597k msg/s | none |
| `all`, `SAMPLE_RATE=1` | 1,263k msg/s | 43% — Redis could not keep up |
| `all`, `SAMPLE_RATE=0.05` | 1,459k msg/s | none |

The relay sheds load rather than slowing the server down: when its queue saturates it drops
events, logs a warning once, and reports the running total as `events_dropped` on
`GET /apps/{id}/counters`. **Check that counter after enabling message events.** If it is
climbing, lower `REVERB_EVENTS_SAMPLE_RATE`.

Those numbers are from a synthetic peak. A server doing a few thousand frames a second relays
everything exactly, with no drops and no sampling needed.

## Pulse

The **Connections** card already works with no changes at all — `ReverbConnections` listens to
Pulse's `IsolatedBeat` inside `pulse:check` and reads `GET /apps/{id}/connections` over HTTP,
which `reverb-rs` serves identically.

The **Messages** card needs `message_sent` and `message_received` relayed. With
`REVERB_EVENTS_SAMPLE_RATE` left at 1, the stock `ReverbMessages` recorder works unchanged.

If you sample in the server, swap in the recorder from this package so the two sample rates do
not compound — see the docblock on `ReverbRs\Pulse\Recorders\Messages`.

## What a relayed connection can and cannot do

Events carry a `RelayedConnection`. Reading it is faithful: `id()`, `origin()` and `app()` return
what the server had. Writing to it — `send()`, `control()`, `terminate()` — throws, because the
socket belongs to another process. Broadcast to the channel, or use the Pusher HTTP API's
`terminate_connections` endpoint, instead.

A listener that throws is reported and skipped; it will not take the relay down.

## Tests

This package has no test suite of its own. It is covered from the Rust side, which spawns a real
PHP process running this code and asserts on what Laravel dispatched:

```bash
./tests/setup-test-app.sh /tmp/reverb-rs-test-app

REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 \
REVERB_TEST_PHP_APP=/tmp/reverb-rs-test-app \
  cargo test --test relay      # from the repository root
```

`tests/relay-harness.php` is the driver: it boots Laravel with this package registered, prints
one JSON line per dispatched event, and runs the relay.

## Redis key prefixes

Laravel applies its Redis key prefix to subscriptions, which would listen on a channel the server
never publishes to. The relay clears that prefix automatically on PhpRedis. On Predis it warns
instead — set `REVERB_EVENTS_CHANNEL` on the server to include your prefix.
