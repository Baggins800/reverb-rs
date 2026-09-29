<?php

/*
 * Drives Laravel's own broadcaster against a running server.
 *
 * This is the path a real application takes: the `reverb` broadcast connection
 * resolves to Laravel's Pusher driver, which talks to the server's HTTP API.
 * Nothing here is reverb-rs aware — if it works, an application's broadcasts
 * work.
 *
 * Usage:
 *   php broadcast-harness.php <command> [args...]
 *
 *   broadcast <channel> <event> <json-payload> [socket-to-exclude]
 *   auth-private <channel> <socket-id>
 *   auth-presence <channel> <socket-id> <user-id>
 *   info <channel>
 *
 * Environment:
 *   REVERB_TEST_PHP_APP   directory containing vendor/autoload.php  (required)
 *   REVERB_HOST/REVERB_PORT/REVERB_SCHEME, REVERB_APP_*
 *
 * Results are printed as a single JSON object.
 */

$base = getenv('REVERB_TEST_PHP_APP');

if (! $base || ! is_file($base.'/vendor/autoload.php')) {
    fwrite(STDERR, "REVERB_TEST_PHP_APP must point at a directory containing vendor/autoload.php\n");
    exit(1);
}

require $base.'/vendor/autoload.php';

use Illuminate\Support\Facades\Broadcast;
use Orchestra\Testbench\Foundation\Application as Testbench;

$app = Testbench::create(
    basePath: $base.'/vendor/orchestra/testbench-core/laravel',
    options: ['extra' => ['dont-discover' => ['*']]],
);

// Exactly the connection Laravel ships in config/broadcasting.php.
$app['config']->set('broadcasting.default', 'reverb');
$app['config']->set('broadcasting.connections.reverb', [
    'driver' => 'reverb',
    'key' => getenv('REVERB_APP_KEY') ?: 'reverb-key',
    'secret' => getenv('REVERB_APP_SECRET') ?: 'reverb-secret',
    'app_id' => getenv('REVERB_APP_ID') ?: '123456',
    'options' => [
        'host' => getenv('REVERB_HOST') ?: '127.0.0.1',
        'port' => (int) (getenv('REVERB_PORT') ?: 8081),
        'scheme' => getenv('REVERB_SCHEME') ?: 'http',
        'useTLS' => (getenv('REVERB_SCHEME') ?: 'http') === 'https',
    ],
    'client_options' => [],
]);

function finish(array $result): never
{
    echo json_encode($result), "\n";
    exit(0);
}

$argv = $_SERVER['argv'];
$command = $argv[1] ?? '';

try {
    $broadcaster = Broadcast::connection('reverb');
    $pusher = $broadcaster->getPusher();

    switch ($command) {
        case 'broadcast':
            [$channel, $event, $payload] = [$argv[2], $argv[3], json_decode($argv[4], true)];

            if (isset($argv[5])) {
                $payload['socket'] = $argv[5];
            }

            $broadcaster->broadcast([$channel], $event, $payload);

            finish(['ok' => true]);

            // no break

        case 'auth-private':
            // What /broadcasting/auth returns for a private channel.
            finish([
                'ok' => true,
                'auth' => json_decode($pusher->authorizeChannel($argv[2], $argv[3]), true)['auth'],
            ]);

            // no break

        case 'auth-presence':
            $response = json_decode(
                $pusher->authorizePresenceChannel($argv[2], $argv[3], $argv[4], ['name' => 'Test User']),
                true,
            );

            finish([
                'ok' => true,
                'auth' => $response['auth'],
                'channel_data' => $response['channel_data'],
            ]);

            // no break

        case 'info':
            finish([
                'ok' => true,
                'channel' => (array) $pusher->getChannelInfo($argv[2], ['info' => 'subscription_count']),
                'channels' => json_decode(json_encode($pusher->getChannels()), true),
            ]);

            // no break

        default:
            finish(['ok' => false, 'error' => "unknown command [{$command}]"]);
    }
} catch (Throwable $e) {
    finish(['ok' => false, 'error' => get_class($e).': '.$e->getMessage()]);
}
