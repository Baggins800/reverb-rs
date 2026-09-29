<?php

namespace ReverbRs;

use RuntimeException;

/**
 * Finds the reverb-rs server binary, and builds one if there is none.
 *
 * Looked for in order: an explicit path, the REVERB_RS_BINARY environment
 * variable, the project's vendor/bin, then PATH. A system-wide install is
 * therefore used as-is, and a build only happens when asked for.
 */
class Binary
{
    public function __construct(protected string $basePath)
    {
        //
    }

    /**
     * The binary to run, or null if none has been installed.
     */
    public function resolve(?string $explicit = null): ?string
    {
        foreach ($this->candidates($explicit) as $candidate) {
            if ($candidate && is_file($candidate) && is_executable($candidate)) {
                return $candidate;
            }
        }

        return $this->onPath();
    }

    /**
     * @return array<int, string|null>
     */
    protected function candidates(?string $explicit): array
    {
        return [
            $explicit,
            getenv('REVERB_RS_BINARY') ?: null,
            $this->installedPath(),
        ];
    }

    /**
     * Where a locally built or downloaded binary is kept.
     */
    public function installedPath(): string
    {
        return $this->basePath.'/vendor/bin/reverb-rs';
    }

    /**
     * A reverb-rs already installed system-wide.
     */
    public function onPath(): ?string
    {
        $found = @shell_exec('command -v reverb-rs 2>/dev/null');
        $found = is_string($found) ? trim($found) : '';

        return $found !== '' && is_executable($found) ? $found : null;
    }

    /**
     * The package directory holding the Rust sources, if they were shipped.
     */
    public function sourcePath(): ?string
    {
        foreach ([
            $this->basePath.'/vendor/baggins800/reverb-rs',
            // A development checkout, where this package is the repository.
            dirname(__DIR__, 2),
        ] as $path) {
            if (is_file($path.'/Cargo.toml')) {
                return $path;
            }
        }

        return null;
    }

    public function cargo(): ?string
    {
        $found = @shell_exec('command -v cargo 2>/dev/null');
        $found = is_string($found) ? trim($found) : '';

        return $found !== '' ? $found : null;
    }

    /**
     * Compile the server from the sources shipped with this package.
     *
     * @param  callable(string): void  $output
     */
    public function build(callable $output): string
    {
        if (! $source = $this->sourcePath()) {
            throw new RuntimeException(
                'The Rust sources are not available here, so there is nothing to build. '.
                'Install a released binary instead, or point REVERB_RS_BINARY at one.'
            );
        }

        if (! $this->cargo()) {
            throw new RuntimeException(
                'cargo was not found on PATH. Install Rust from https://rustup.rs, '.
                'or use a released binary instead.'
            );
        }

        $output("Building reverb-rs from {$source} — this takes a few minutes the first time.");

        $command = sprintf(
            'cargo build --release --locked --bin reverb-rs --manifest-path %s 2>&1',
            escapeshellarg($source.'/Cargo.toml')
        );

        $handle = popen($command, 'r');

        if ($handle === false) {
            throw new RuntimeException('Could not start cargo.');
        }

        while (($line = fgets($handle)) !== false) {
            $output(rtrim($line, "\n"));
        }

        if (pclose($handle) !== 0) {
            throw new RuntimeException('cargo build failed; see the output above.');
        }

        $built = $source.'/target/release/reverb-rs';

        if (! is_file($built)) {
            throw new RuntimeException("cargo reported success but {$built} is missing.");
        }

        return $this->install($built);
    }

    /**
     * Put a binary where this package will find it again.
     */
    public function install(string $from): string
    {
        $target = $this->installedPath();

        if (! is_dir($directory = dirname($target))) {
            mkdir($directory, 0o755, true);
        }

        if (! copy($from, $target)) {
            throw new RuntimeException("Could not copy {$from} to {$target}.");
        }

        chmod($target, 0o755);

        return $target;
    }

    /**
     * The release asset name for the machine this is running on.
     */
    public function platformTarget(): ?string
    {
        $machine = php_uname('m');

        $architecture = match ($machine) {
            'x86_64', 'amd64' => 'x86_64',
            'arm64', 'aarch64' => 'aarch64',
            default => null,
        };

        if ($architecture === null) {
            return null;
        }

        return match (PHP_OS_FAMILY) {
            'Linux' => "{$architecture}-unknown-linux-gnu",
            'Darwin' => "{$architecture}-apple-darwin",
            default => null,
        };
    }
}
