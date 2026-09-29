<?php

namespace ReverbRs\Console;

use Illuminate\Console\Command;
use ReverbRs\Binary;
use Symfony\Component\Console\Attribute\AsCommand;
use Throwable;

/**
 * Install the reverb-rs server binary.
 *
 * With `--build` it compiles the Rust sources shipped in this package, which
 * needs a Rust toolchain. Otherwise it downloads the release matching this
 * platform. Either way the result lands in vendor/bin, where reverb-rs:start
 * looks for it.
 */
#[AsCommand(name: 'reverb-rs:binary')]
class BinaryCommand extends Command
{
    protected $signature = 'reverb-rs:binary
                {--build : Compile from the Rust sources instead of downloading}
                {--version=latest : Which release to download}
                {--force : Replace an existing binary}';

    protected $description = 'Install the reverb-rs server binary';

    public function handle(Binary $binary): int
    {
        if (($existing = $binary->resolve()) && ! $this->option('force')) {
            $this->components->info("reverb-rs is already available at {$existing}.");
            $this->components->warn('Pass --force to replace it.');

            return self::SUCCESS;
        }

        try {
            $path = $this->option('build')
                ? $binary->build(fn (string $line) => $this->output->writeln("  <fg=gray>{$line}</>"))
                : $this->download($binary);
        } catch (Throwable $e) {
            $this->components->error($e->getMessage());

            return self::FAILURE;
        }

        $this->components->info("Installed {$path}.");
        $this->components->bulletList(['Start it with: php artisan reverb-rs:start']);

        return self::SUCCESS;
    }

    /**
     * Fetch the release build for this platform.
     */
    protected function download(Binary $binary): string
    {
        $target = $binary->platformTarget();

        if (! $target) {
            throw new \RuntimeException(sprintf(
                'No released build for %s on %s. Compile one instead: '.
                'php artisan reverb-rs:binary --build',
                php_uname('m'),
                PHP_OS_FAMILY,
            ));
        }

        $version = $this->option('version');
        $release = $version === 'latest' ? 'latest/download' : "download/{$version}";
        $url = "https://github.com/Baggins800/reverb-rs/releases/{$release}/reverb-rs-{$target}.tar.gz";

        $this->components->info("Downloading {$url}");

        $archive = tempnam(sys_get_temp_dir(), 'reverb-rs-').'.tar.gz';
        $source = @fopen($url, 'r');

        if ($source === false) {
            throw new \RuntimeException(
                "Could not download {$url}. If no release has been published yet, ".
                'compile instead: php artisan reverb-rs:binary --build'
            );
        }

        file_put_contents($archive, $source);
        fclose($source);

        $extracted = $archive.'-extracted';
        mkdir($extracted, 0o755, true);

        $phar = new \PharData($archive);
        $phar->decompress();
        (new \PharData(str_replace('.tar.gz', '.tar', $archive)))->extractTo($extracted, null, true);

        $found = $extracted.'/reverb-rs';

        if (! is_file($found)) {
            throw new \RuntimeException("The archive did not contain a reverb-rs binary.");
        }

        $path = $binary->install($found);

        @unlink($archive);

        return $path;
    }
}
