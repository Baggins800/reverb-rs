<?php

namespace ReverbRs\Support;

use Laravel\Reverb\Contracts\WebSocketConnection;
use RuntimeException;

/**
 * Stands in for a socket that lives in the reverb-rs process.
 *
 * Relayed events carry a connection so listeners can read its ID, origin and
 * application. The socket itself is in another process, so writing to it fails
 * loudly rather than silently doing nothing.
 */
class DetachedSocket implements WebSocketConnection
{
    public function __construct(protected string $socketId)
    {
        //
    }

    public function id(): int|string
    {
        return $this->socketId;
    }

    public function send(mixed $message): void
    {
        throw new RuntimeException(static::unreachable($this->socketId));
    }

    public function close(mixed $message = null): void
    {
        throw new RuntimeException(static::unreachable($this->socketId));
    }

    /**
     * The message explaining why this connection cannot be written to.
     */
    public static function unreachable(string $socketId): string
    {
        return "Connection [{$socketId}] belongs to the reverb-rs server process and cannot be "
            .'written to from here. Broadcast to its channel, or use the Pusher HTTP API '
            .'(for example the terminate_connections endpoint), instead.';
    }
}
