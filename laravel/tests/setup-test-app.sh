#!/usr/bin/env bash
# Build the Laravel application the Rust relay tests drive.
#
#   ./laravel/tests/setup-test-app.sh /tmp/reverb-rs-test-app
#
# Then run the tests against it:
#
#   REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 \
#   REVERB_TEST_PHP_APP=/tmp/reverb-rs-test-app \
#   cargo test --test relay

set -euo pipefail

target="${1:?usage: setup-test-app.sh <directory>}"

mkdir -p "$target"
cd "$target"

if [ ! -f composer.json ]; then
    cat > composer.json <<'JSON'
{
    "name": "reverb-rs/relay-test-app",
    "description": "Host application for the reverb-rs relay tests.",
    "require": {
        "laravel/reverb": "*",
        "orchestra/testbench": "*"
    },
    "minimum-stability": "dev",
    "prefer-stable": true
}
JSON
fi

composer install --no-interaction

echo
echo "Ready. Run the relay tests with:"
echo
echo "  REVERB_TEST_REDIS_URL=redis://127.0.0.1:6379 \\"
echo "  REVERB_TEST_PHP_APP=$target \\"
echo "  cargo test --test relay"
