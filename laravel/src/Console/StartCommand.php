<?php

namespace ReverbRs\Console;

use Illuminate\Console\Command;
use Illuminate\Contracts\Config\Repository;
use Illuminate\Contracts\Console\Kernel;
use ReverbRs\Binary;
use Symfony\Component\Console\Attribute\AsCommand;

/**
 * Start the reverb-rs server using this application's Reverb configuration.
 *
 * A drop-in for `php artisan reverb:start`: it finds the server binary,
 * hands it everything config/reverb.php resolved to — several applications, a
 * custom application provider, the TLS block, where reverb:restart signals —
 * and replaces this process with it, so signals and supervisors behave as they
 * did before.
 */
#[AsCommand(name: 'reverb-rs:start')]
class StartCommand extends Command
{
    protected $signature = 'reverb-rs:start
                {--host= : The IP address the server should bind to}
                {--port= : The port the server should listen on}
                {--path= : The path the server should prefix to all routes}
                {--hostname= : The hostname the server is accessible from}
                {--debug : Display debug messages in the terminal}
                {--binary= : Path to the reverb-rs binary}
                {--server=reverb : Which entry under reverb.servers to use}';

    protected $description = 'Start the reverb-rs server with this application\'s configuration';

    public function handle(Binary $binary, Repository $config, Kernel $kernel): int
    {
        $executable = $binary->resolve($this->option('binary'));

        if (! $executable) {
            $this->components->error('No reverb-rs binary found.');
            $this->components->bulletList([
                'Build one from source: php artisan reverb-rs:binary --build',
                'Or install a release:  php artisan reverb-rs:binary',
                'Or point REVERB_RS_BINARY at an existing install.',
            ]);

            return self::FAILURE;
        }

        $exported = $this->exportConfig($kernel, $config);

        $this->components->info("Starting reverb-rs ({$executable}).");

        $arguments = [$executable];

        foreach (['host', 'port', 'path', 'hostname'] as $option) {
            if ($value = $this->option($option)) {
                $arguments[] = "--{$option}";
                $arguments[] = $value;
            }
        }

        if ($this->option('debug')) {
            $arguments[] = '--debug';
        }

        // Never read a .env: the exported config already holds everything, and
        // a stale .env alongside it would be a confusing second source.
        $arguments[] = '--env-file';
        $arguments[] = '/dev/null';

        return $this->replaceProcess($arguments, ['REVERB_CONFIG_FILE' => $exported]);
    }

    /**
     * Write this application's resolved Reverb configuration to disk.
     */
    protected function exportConfig(Kernel $kernel, Repository $config): string
    {
        $kernel->call('reverb-rs:config', ['--server' => $this->option('server')]);

        $path = $config->get('reverb-rs.config_path')
            ?: storage_path('framework/reverb-rs.json');

        if (! is_dir($directory = dirname($path))) {
            mkdir($directory, 0o755, true);
        }

        file_put_contents($path, trim($kernel->output()));

        return $path;
    }

    /**
     * Hand the process over to the server.
     *
     * `pcntl_exec` replaces this process, so the server becomes the one the
     * supervisor is watching and receives its signals directly. Without pcntl
     * the command falls back to running it as a child.
     *
     * @param  array<int, string>  $arguments
     * @param  array<string, string>  $environment
     */
    protected function replaceProcess(array $arguments, array $environment): int
    {
        $executable = array_shift($arguments);

        if (function_exists('pcntl_exec')) {
            pcntl_exec($executable, $arguments, $environment + getenv());

            // Only reached if exec failed.
            $this->components->error("Could not execute {$executable}.");

            return self::FAILURE;
        }

        foreach ($environment as $key => $value) {
            putenv("{$key}={$value}");
        }

        $command = implode(' ', array_map(escapeshellarg(...), [$executable, ...$arguments]));

        passthru($command, $status);

        return $status;
    }
}
