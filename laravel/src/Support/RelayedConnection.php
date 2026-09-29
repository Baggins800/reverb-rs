<?php

namespace ReverbRs\Support;

use Laravel\Reverb\Application;
use Laravel\Reverb\Contracts\Connection;
use Ratchet\RFC6455\Messaging\Frame;
use RuntimeException;

/**
 * A Reverb connection reconstructed from a relayed event.
 *
 * Everything a listener reads — the socket ID, the origin, the application —
 * is faithful. Everything that would write to the socket throws, because the
 * socket is held by the reverb-rs process.
 */
class RelayedConnection extends Connection
{
    public function __construct(
        protected string $socketId,
        Application $application,
        ?string $origin = null,
    ) {
        parent::__construct(new DetachedSocket($socketId), $application, $origin);
    }

    public function identifier(): string
    {
        return $this->socketId;
    }

    public function id(): string
    {
        return $this->socketId;
    }

    public function send(string $message): void
    {
        throw new RuntimeException(DetachedSocket::unreachable($this->socketId));
    }

    public function control(string $type = Frame::OP_PING): void
    {
        throw new RuntimeException(DetachedSocket::unreachable($this->socketId));
    }

    public function terminate(): void
    {
        throw new RuntimeException(DetachedSocket::unreachable($this->socketId));
    }
}
