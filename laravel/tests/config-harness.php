<?php

/*
 * Boots a Laravel application and runs `reverb-rs:config`, so the export can
 * be exercised from Rust.
 *
 * The configuration deliberately uses things a plain .env cannot express: two
 * applications defined inline, a hardcoded TLS block, and a file cache store
 * for reverb:restart.
 *
 *   REVERB_TEST_PHP_APP=<dir> php config-harness.php [cache-path]
 */

$base = getenv('REVERB_TEST_PHP_APP');

if (! $base || ! is_file($base.'/vendor/autoload.php')) {
    fwrite(STDERR, "REVERB_TEST_PHP_APP must point at a directory containing vendor/autoload.php\n");
    exit(1);
}

require $base.'/vendor/autoload.php';

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
use Laravel\Reverb\ApplicationManagerServiceProvider;
use Laravel\Reverb\ReverbServiceProvider;
use Orchestra\Testbench\Foundation\Application as Testbench;
use ReverbRs\ReverbRsServiceProvider;

$app = Testbench::create(
    basePath: $base.'/vendor/orchestra/testbench-core/laravel',
    options: ['extra' => ['dont-discover' => ['*']]],
);

$app->register(ApplicationManagerServiceProvider::class);
$app->register(ReverbServiceProvider::class);
$app->register(ReverbRsServiceProvider::class);

$app['config']->set('reverb.servers.reverb.host', '127.0.0.1');
$app['config']->set('reverb.servers.reverb.port', (int) (getenv('BENCH_PORT') ?: 8092));
$app['config']->set('reverb.servers.reverb.max_request_size', 20000);

// Two applications, which a single set of REVERB_APP_* variables cannot hold.
$app['config']->set('reverb.apps.apps', [
    [
        'app_id' => 'primary-app',
        'key' => 'primary-key',
        'secret' => 'primary-secret',
        'allowed_origins' => ['*'],
        'ping_interval' => 60,
        'activity_timeout' => 30,
        'max_message_size' => 10_000,
        'accept_client_events_from' => 'members',
        'options' => [],
    ],
    [
        'app_id' => 'secondary-app',
        'key' => 'secondary-key',
        'secret' => 'secondary-secret',
        'allowed_origins' => ['*'],
        'ping_interval' => 60,
        'activity_timeout' => 45,
        'max_message_size' => 5_000,
        'accept_client_events_from' => 'all',
        'options' => [],
    ],
]);

// A file cache store, so reverb:restart has somewhere readable to signal.
if ($path = ($_SERVER['argv'][1] ?? null)) {
    $app['config']->set('cache.default', 'file');
    $app['config']->set('cache.stores.file', ['driver' => 'file', 'path' => $path]);
}

$kernel = $app->make(Kernel::class);

// With a command named, run that instead and let its output through. Used to
// exercise reverb-rs:start, which replaces this process with the server.
if ($command = getenv('HARNESS_COMMAND')) {
    $options = [];

    foreach (explode(',', getenv('HARNESS_OPTIONS') ?: '') as $pair) {
        if (str_contains($pair, '=')) {
            [$k, $v] = explode('=', $pair, 2);
            $options["--{$k}"] = $v;
        }
    }

    exit($kernel->call($command, $options));
}

$kernel->call('reverb-rs:config');

echo $kernel->output();
