<?php

// Boot a minimal Laravel app via Testbench and start the real Reverb server,
// matching what `php artisan reverb:start` does.

// Resolve the Reverb checkout from the environment, or the working directory.
$checkout = getenv('REVERB_CHECKOUT') ?: getcwd();

if (! is_file($checkout.'/vendor/autoload.php')) {
    fwrite(STDERR, "No vendor/autoload.php in [{$checkout}]; run composer install there.\n");
    exit(1);
}

require $checkout.'/vendor/autoload.php';

use Laravel\Reverb\ApplicationManagerServiceProvider;
use Laravel\Reverb\Contracts\Logger;
use Laravel\Reverb\Jobs\PingInactiveConnections;
use Laravel\Reverb\Jobs\PruneStaleConnections;
use Laravel\Reverb\Loggers\NullLogger;
use Laravel\Reverb\Protocols\Pusher\Contracts\ChannelConnectionManager;
use Laravel\Reverb\Protocols\Pusher\Contracts\ChannelManager;
use Laravel\Reverb\Protocols\Pusher\Managers\ArrayChannelConnectionManager;
use Laravel\Reverb\Protocols\Pusher\Managers\ArrayChannelManager;
use Laravel\Reverb\ReverbServiceProvider;
use Laravel\Reverb\Servers\Reverb\Factory;
use Orchestra\Testbench\Foundation\Application as Testbench;
use React\EventLoop\Loop;

$app = Testbench::create(
    basePath: $checkout.'/vendor/orchestra/testbench-core/laravel',
    options: ['extra' => ['dont-discover' => ['*']]],
);

$app->register(ApplicationManagerServiceProvider::class);
$app->register(ReverbServiceProvider::class);

$app->instance(Logger::class, new NullLogger);
$app->singleton(ChannelManager::class, fn () => new ArrayChannelManager);
$app->bind(ChannelConnectionManager::class, fn () => new ArrayChannelConnectionManager);

$app['config']->set('reverb.apps.apps', [[
    'app_id' => '123456',
    'key' => 'reverb-key',
    'secret' => 'reverb-secret',
    'allowed_origins' => ['*'],
    'ping_interval' => 60,
    'activity_timeout' => 30,
    'max_message_size' => 10_000,
    'max_connections' => null,
    'accept_client_events_from' => 'members',
    'options' => [],
]]);

$host = getenv('BENCH_HOST') ?: '127.0.0.1';
$port = getenv('BENCH_PORT') ?: '8080';

$loop = Loop::get();
$server = Factory::make($host, $port, '', maxRequestSize: 10_000, loop: $loop);

$loop->addPeriodicTimer(60, function () {
    PruneStaleConnections::dispatch();
    PingInactiveConnections::dispatch();
});

fwrite(STDERR, "Laravel Reverb listening on {$host}:{$port}\n");

$server->start();
