<?php

namespace ReverbRs\Events;

use Illuminate\Foundation\Events\Dispatchable;

/**
 * A user's first connection joined a presence channel.
 *
 * Reverb has no such event: it tells the channel's other subscribers and no
 * one else. reverb-rs relays it once per join, whoever else is subscribed, so
 * a listener can track presence without being a client itself.
 */
class MemberAdded
{
    use Dispatchable;

    /**
     * @param  array<string, mixed>|null  $userInfo
     */
    public function __construct(
        public string $channel,
        public string $socketId,
        public string|int|null $userId,
        public ?array $userInfo = null,
    ) {
        //
    }
}
