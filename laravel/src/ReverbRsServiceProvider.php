<?php

namespace ReverbRs;

use Illuminate\Support\ServiceProvider;
use Laravel\Reverb\Protocols\Pusher\Contracts\ChannelConnectionManager;
use Laravel\Reverb\Protocols\Pusher\Managers\ArrayChannelConnectionManager;
use ReverbRs\Console\RelayCommand;

class ReverbRsServiceProvider extends ServiceProvider
{
    public function register(): void
    {
        // Reverb binds this inside its server factory, which never runs here.
        // Rebuilding a Channel for ChannelCreated/ChannelRemoved needs it.
        $this->app->bindIf(
            ChannelConnectionManager::class,
            fn () => new ArrayChannelConnectionManager
        );

        $this->app->singleton(RelayedEventFactory::class);
    }

    public function boot(): void
    {
        if ($this->app->runningInConsole()) {
            $this->commands([RelayCommand::class]);
        }
    }
}
