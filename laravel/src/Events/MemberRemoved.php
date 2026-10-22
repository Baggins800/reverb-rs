<?php

namespace ReverbRs\Events;

use Illuminate\Foundation\Events\Dispatchable;

/**
 * A user's last connection left a presence channel.
 *
 * Relayed once per departure, including when the channel is left empty and
 * when the server shuts down, so a listener sees every member leave.
 */
class MemberRemoved
{
    use Dispatchable;

    public function __construct(
        public string $channel,
        public string $socketId,
        public string|int|null $userId,
    ) {
        //
    }
}
