<?php

namespace ReverbRs;

use Laravel\Reverb\Contracts\ApplicationProvider;
use Laravel\Reverb\Events\ChannelCreated;
use Laravel\Reverb\Events\ChannelRemoved;
use Laravel\Reverb\Events\ConnectionPruned;
use Laravel\Reverb\Events\MessageReceived;
use Laravel\Reverb\Events\MessageSent;
use Laravel\Reverb\Exceptions\InvalidApplication;
use Laravel\Reverb\Protocols\Pusher\Channels\ChannelBroker;
use Laravel\Reverb\Protocols\Pusher\Channels\ChannelConnection;
use ReverbRs\Events\MemberAdded;
use ReverbRs\Events\MemberRemoved;
use ReverbRs\Support\RelayedConnection;

/**
 * Rebuilds Laravel Reverb's own event objects from the JSON reverb-rs relays.
 *
 * The events are the real classes, so existing listeners, Pulse recorders and
 * Telescope see exactly what they would have seen from the PHP server. The two
 * presence events have no Reverb counterpart and are this package's own.
 */
class RelayedEventFactory
{
    public function __construct(protected ApplicationProvider $applications)
    {
        //
    }

    /**
     * Build the event described by one relayed entry, or null if it is not
     * something this version understands.
     *
     * @param  array<string, mixed>  $entry
     */
    public function make(array $entry): ?object
    {
        $name = $entry['event'] ?? null;
        $payload = $entry['payload'] ?? [];

        if (! is_string($name) || ! is_array($payload)) {
            return null;
        }

        try {
            $application = $this->applications->findById((string) ($entry['application'] ?? ''));
        } catch (InvalidApplication) {
            return null;
        }

        return match ($name) {
            'message_sent' => new MessageSent(
                $this->connection($payload, $application),
                (string) ($payload['message'] ?? ''),
            ),
            'message_received' => new MessageReceived(
                $this->connection($payload, $application),
                (string) ($payload['message'] ?? ''),
            ),
            'channel_created' => new ChannelCreated(
                ChannelBroker::create((string) ($payload['channel'] ?? '')),
            ),
            'channel_removed' => new ChannelRemoved(
                ChannelBroker::create((string) ($payload['channel'] ?? '')),
            ),
            'connection_pruned' => new ConnectionPruned(
                new ChannelConnection(
                    $this->connection($payload, $application),
                    (array) ($payload['data'] ?? []),
                ),
            ),
            'member_added' => new MemberAdded(
                (string) ($payload['channel'] ?? ''),
                (string) ($payload['socket_id'] ?? ''),
                $this->userId($payload),
                is_array($payload['user_info'] ?? null) ? $payload['user_info'] : null,
            ),
            'member_removed' => new MemberRemoved(
                (string) ($payload['channel'] ?? ''),
                (string) ($payload['socket_id'] ?? ''),
                $this->userId($payload),
            ),
            default => null,
        };
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    protected function userId(array $payload): string|int|null
    {
        $id = $payload['user_id'] ?? null;

        return is_string($id) || is_int($id) ? $id : null;
    }

    /**
     * @param  array<string, mixed>  $payload
     */
    protected function connection(array $payload, $application): RelayedConnection
    {
        return new RelayedConnection(
            (string) ($payload['socket_id'] ?? ''),
            $application,
            $payload['origin'] ?? null,
        );
    }
}
