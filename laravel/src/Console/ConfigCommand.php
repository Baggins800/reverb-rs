<?php

namespace ReverbRs\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Config\Repository;
use Laravel\Reverb\Application;
use Laravel\Reverb\Contracts\ApplicationProvider;
use Symfony\Component\Console\Attribute\AsCommand;

/**
 * Export everything config/reverb.php resolves to, for reverb-rs to read.
 *
 * reverb-rs reads the environment, which covers a stock config file because
 * that file is entirely env() calls. This covers the rest: values hardcoded in
 * the config, several applications defined inline, a custom application
 * provider resolving them from a database, the options.tls block, and where
 * `php artisan reverb:restart` leaves its signal.
 *
 *     php artisan reverb-rs:config > reverb-rs.json
 *     REVERB_CONFIG_FILE=reverb-rs.json reverb-rs
 */
#[AsCommand(name: 'reverb-rs:config')]
class ConfigCommand extends Command
{
    protected $signature = 'reverb-rs:config
                {--server=reverb : Which entry under reverb.servers to export}
                {--pretty : Indent the output}';

    protected $description = 'Export this application\'s Reverb configuration for reverb-rs';

    public function handle(Repository $config, ApplicationProvider $applications): int
    {
        $server = $config->get('reverb.servers.'.$this->option('server'), []);

        $payload = [
            'server' => [
                'host' => $server['host'] ?? '0.0.0.0',
                'port' => (int) ($server['port'] ?? 8080),
                'path' => $server['path'] ?? '',
                'hostname' => $server['hostname'] ?? null,
                'max_request_size' => (int) ($server['max_request_size'] ?? 10000),
                'tls' => array_filter([
                    'local_cert' => $server['options']['tls']['local_cert'] ?? null,
                    'local_pk' => $server['options']['tls']['local_pk'] ?? null,
                ]),
            ],
            'scaling' => [
                'enabled' => (bool) ($server['scaling']['enabled'] ?? false),
                'channel' => $server['scaling']['channel'] ?? 'reverb',
                'redis_url' => $this->redisUrl($server['scaling']['server'] ?? []),
            ],
            'restart' => $this->restart($config),
            // Resolved through the configured provider, so a custom one that
            // reads applications from a database is exported like any other.
            'apps' => $applications->all()
                ->map(fn (Application $app) => $this->application($app))
                ->values()
                ->all(),
        ];

        $this->output->writeln(json_encode(
            $payload,
            JSON_UNESCAPED_SLASHES | ($this->option('pretty') ? JSON_PRETTY_PRINT : 0)
        ));

        return self::SUCCESS;
    }

    /**
     * @return array<string, mixed>
     */
    protected function application(Application $app): array
    {
        return [
            'app_id' => $app->id(),
            'key' => $app->key(),
            'secret' => $app->secret(),
            'ping_interval' => $app->pingInterval(),
            'activity_timeout' => $app->activityTimeout(),
            'allowed_origins' => $app->allowedOrigins(),
            'max_message_size' => $app->maxMessageSize(),
            'max_connections' => $app->maxConnections(),
            'accept_client_events_from' => $app->acceptClientEventsFrom(),
            'rate_limiting' => $app->rateLimiting(),
        ];
    }

    /**
     * Where reverb:restart writes, so reverb-rs can watch the same place.
     *
     * @return array<string, mixed>
     */
    protected function restart(Repository $config): array
    {
        $store = $config->get('cache.default');
        $driver = $config->get("cache.stores.{$store}.driver");

        return match ($driver) {
            'file' => [
                'driver' => 'file',
                'path' => $config->get("cache.stores.{$store}.path"),
            ],
            'redis' => [
                'driver' => 'redis',
                // Laravel's Redis cache store appends a colon to the prefix.
                'prefix' => ($p = $config->get('cache.prefix')) ? $p.':' : '',
                'redis_url' => $this->redisUrl($config->get(
                    'database.redis.'.($config->get("cache.stores.{$store}.connection") ?: 'cache'),
                    $config->get('database.redis.default', [])
                )),
            ],
            // Database and memcached stores are not readable from here; the
            // server falls back to stopping on a signal.
            default => ['driver' => (string) $driver],
        };
    }

    /**
     * @param  array<string, mixed>  $connection
     */
    protected function redisUrl(array $connection): string
    {
        if ($url = $connection['url'] ?? null) {
            return $url;
        }

        $auth = ($password = $connection['password'] ?? null)
            ? rawurlencode((string) ($connection['username'] ?? '')).':'.rawurlencode((string) $password).'@'
            : '';

        $scheme = ($connection['scheme'] ?? 'tcp') === 'tls' ? 'rediss' : 'redis';

        return sprintf(
            '%s://%s%s:%s/%s',
            $scheme,
            $auth,
            $connection['host'] ?? '127.0.0.1',
            $connection['port'] ?? 6379,
            $connection['database'] ?? 0,
        );
    }
}
