<?php

namespace ReverbRs\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Events\Dispatcher;
use Illuminate\Support\Facades\Redis;
use Redis as PhpRedis;
use ReverbRs\RelayedEventFactory;
use Symfony\Component\Console\Attribute\AsCommand;
use Throwable;

/**
 * Consumes the events reverb-rs publishes and re-dispatches them locally.
 *
 * Run one of these alongside your application — the same way you run a queue
 * worker — and Reverb's events fire in your app again, so Pulse recorders,
 * Telescope and your own listeners behave as they did with the PHP server.
 */
#[AsCommand(name: 'reverb-rs:relay')]
class RelayCommand extends Command
{
    protected $signature = 'reverb-rs:relay
                {--channel= : The Redis channel reverb-rs publishes events to}
                {--connection= : The Redis connection to subscribe on}';

    protected $description = 'Relay reverb-rs server events onto this application\'s event bus';

    public function handle(RelayedEventFactory $factory, Dispatcher $events): int
    {
        $channel = $this->option('channel')
            ?: config('reverb-rs.channel', env('REVERB_EVENTS_CHANNEL', 'reverb-rs:events'));

        $connection = Redis::connection(
            $this->option('connection') ?: config('reverb-rs.connection', 'default')
        );

        $this->ignoreKeyPrefix($connection);

        $this->components->info("Relaying reverb-rs events from [{$channel}].");

        $connection->subscribe([$channel], function (string $payload) use ($factory, $events) {
            $this->dispatchBatch($payload, $factory, $events);
        });

        return self::SUCCESS;
    }

    /**
     * Dispatch every event in one relayed batch.
     */
    protected function dispatchBatch(
        string $payload,
        RelayedEventFactory $factory,
        Dispatcher $events,
    ): void {
        $batch = json_decode($payload, associative: true);

        if (! is_array($batch)) {
            $this->components->warn('Discarded a malformed relay payload.');

            return;
        }

        foreach ($batch as $entry) {
            if (! is_array($entry)) {
                continue;
            }

            try {
                if ($event = $factory->make($entry)) {
                    $events->dispatch($event);
                }
            } catch (Throwable $e) {
                // One failing listener must not take the relay down with it.
                $this->components->warn(
                    'Failed to dispatch a relayed event: '.$e->getMessage()
                );

                report($e);
            }
        }
    }

    /**
     * Subscribe to the raw channel name.
     *
     * Laravel applies its Redis key prefix to subscriptions, which would look
     * for a channel reverb-rs never publishes to. PhpRedis lets us turn that
     * off; on Predis, set REVERB_EVENTS_CHANNEL to include the prefix instead.
     */
    protected function ignoreKeyPrefix($connection): void
    {
        $client = $connection->client();

        if ($client instanceof PhpRedis) {
            $client->setOption(PhpRedis::OPT_PREFIX, '');

            return;
        }

        $prefix = config('database.redis.options.prefix');

        if ($prefix) {
            $this->components->warn(
                "This Redis client applies the key prefix [{$prefix}] to subscriptions. ".
                'Set REVERB_EVENTS_CHANNEL on the server to include it, or the relay will '.
                'receive nothing.'
            );
        }
    }
}
