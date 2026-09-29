<?php

/*
 * A machine-readable driver for the relay, exercised from Rust.
 *
 * Boots a Laravel application with this package registered, listens for every
 * event Reverb dispatches, and prints one JSON line per event so an
 * integration test can assert on it. Then it runs the relay, which blocks.
 *
 * Environment:
 *   REVERB_TEST_PHP_APP   directory containing vendor/autoload.php  (required)
 *   REVERB_EVENTS_CHANNEL the Redis channel to subscribe to
 *   REDIS_HOST/REDIS_PORT where to find Redis
 *   RELAY_HARNESS_THROW   set to 1 to install a listener that always throws,
 *                         proving one bad listener cannot stop the relay
 *
 * Every line of interest is prefixed with `@@` so it can be picked out of
 * whatever else Laravel decides to print.
 */

$base = getenv('REVERB_TEST_PHP_APP');

if (! $base || ! is_file($base.'/vendor/autoload.php')) {
    fwrite(STDERR, "REVERB_TEST_PHP_APP must point at a directory containing vendor/autoload.php\n");
    exit(1);
}

require $base.'/vendor/autoload.php';

// This package is not installed via composer here, so map its namespace by hand.
spl_autoload_register(function (string $class): void {
    if (! str_starts_with($class, 'ReverbRs\\')) {
        return;
    }

    $path = __DIR__.'/../src/'.str_replace('\\', '/', substr($class, strlen('ReverbRs\\'))).'.php';

    if (is_file($path)) {
        require $path;
    }
});

use Illuminate\Contracts\Console\Kernel;
use Illuminate\Support\Facades\Event;
use Laravel\Reverb\ApplicationManagerServiceProvider;
use Laravel\Reverb\Events\ChannelCreated;
use Laravel\Reverb\Events\ChannelRemoved;
use Laravel\Reverb\Events\ConnectionPruned;
use Laravel\Reverb\Events\MessageReceived;
use Laravel\Reverb\Events\MessageSent;
use Laravel\Reverb\ReverbServiceProvider;
use Orchestra\Testbench\Foundation\Application as Testbench;
use ReverbRs\ReverbRsServiceProvider;

/**
 * Emit one observation for the Rust side to assert on.
 */
function emit(array $line): void
{
    fwrite(STDOUT, '@@'.json_encode($line)."\n");
    flush();
}

$app = Testbench::create(
    basePath: $base.'/vendor/orchestra/testbench-core/laravel',
    options: ['extra' => ['dont-discover' => ['*']]],
);

$app->register(ApplicationManagerServiceProvider::class);
$app->register(ReverbServiceProvider::class);
$app->register(ReverbRsServiceProvider::class);

$app['config']->set('reverb.apps.apps', [[
    'app_id' => getenv('REVERB_APP_ID') ?: '123456',
    'key' => getenv('REVERB_APP_KEY') ?: 'reverb-key',
    'secret' => getenv('REVERB_APP_SECRET') ?: 'reverb-secret',
    'allowed_origins' => ['*'],
    'ping_interval' => 60,
    'activity_timeout' => 30,
    'max_message_size' => 10_000,
    'accept_client_events_from' => 'members',
    'options' => [],
]]);

$app['config']->set('database.redis.default', [
    'host' => getenv('REDIS_HOST') ?: '127.0.0.1',
    'port' => (int) (getenv('REDIS_PORT') ?: 6379),
    'database' => 0,
]);

/**
 * Report a connection carried by an event, including whether writing to it
 * fails the way it should.
 */
function describeConnection($connection): array
{
    $write = ['threw' => false, 'message' => null];

    try {
        $connection->send('probe');
    } catch (Throwable $e) {
        $write = ['threw' => true, 'message' => $e->getMessage()];
    }

    return [
        'app' => $connection->app()->id(),
        'socket' => $connection->id(),
        'origin' => $connection->origin(),
        'write' => $write,
    ];
}

Event::listen(MessageSent::class, fn (MessageSent $e) => emit([
    'event' => 'MessageSent',
    'connection' => describeConnection($e->connection),
    'message' => $e->message,
]));

Event::listen(MessageReceived::class, fn (MessageReceived $e) => emit([
    'event' => 'MessageReceived',
    'connection' => describeConnection($e->connection),
    'message' => $e->message,
]));

Event::listen(ChannelCreated::class, fn (ChannelCreated $e) => emit([
    'event' => 'ChannelCreated',
    'channel' => $e->channel->name(),
    'class' => get_class($e->channel),
]));

Event::listen(ChannelRemoved::class, fn (ChannelRemoved $e) => emit([
    'event' => 'ChannelRemoved',
    'channel' => $e->channel->name(),
    'class' => get_class($e->channel),
]));

Event::listen(ConnectionPruned::class, fn (ConnectionPruned $e) => emit([
    'event' => 'ConnectionPruned',
    'socket' => $e->connection->id(),
    'data' => $e->connection->data(),
    'user_id' => $e->connection->data('user_id'),
]));

if (getenv('RELAY_HARNESS_THROW')) {
    // Runs after the reporting listener above, so the event is still observed
    // and the throw happens on the way back out of the dispatcher — where the
    // relay has to isolate it or die.
    Event::listen(ChannelCreated::class, function (): void {
        throw new RuntimeException('deliberate listener failure');
    });
}

emit(['event' => 'Ready']);

$app->make(Kernel::class)->call('reverb-rs:relay');
