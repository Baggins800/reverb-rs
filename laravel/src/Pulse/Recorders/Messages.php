<?php

namespace ReverbRs\Pulse\Recorders;

use Laravel\Reverb\Pulse\Recorders\ReverbMessages;

/**
 * Reverb's messages recorder, without a second round of sampling.
 *
 * Requires laravel/pulse, which neither this package nor laravel/reverb
 * depends on. Referencing this class without Pulse installed is a fatal
 * error, so only name it in config/pulse.php, which Pulse itself reads.
 *
 * Only needed when REVERB_EVENTS_SAMPLE_RATE is below 1: the server has
 * already sampled, so sampling again here would compound the two rates and
 * the Pulse card would under-report by their product.
 *
 * Register this in place of ReverbMessages, and leave the stock recorder's
 * `sample_rate` set to the server's rate so the card still scales the graph:
 *
 *     // config/pulse.php
 *     'recorders' => [
 *         \ReverbRs\Pulse\Recorders\Messages::class => [],
 *         \Laravel\Reverb\Pulse\Recorders\ReverbMessages::class => [
 *             // Match REVERB_EVENTS_SAMPLE_RATE. Read by the card, which
 *             // multiplies the graph by 1 / sample_rate.
 *             'sample_rate' => 0.05,
 *         ],
 *     ],
 *
 * With REVERB_EVENTS_SAMPLE_RATE left at 1, use the stock recorder instead —
 * this class has nothing to add.
 */
class Messages extends ReverbMessages
{
    /**
     * Sampling already happened in the server.
     */
    protected function shouldSample(): bool
    {
        return true;
    }
}
