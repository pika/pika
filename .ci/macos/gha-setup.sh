#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

script_dir="$(CDPATH='' cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
echo "[INFO] script_dir: '$script_dir'"
pika_dir="$(CDPATH='' cd "$script_dir/../.." && pwd)"
readonly pika_dir
echo "[INFO] pika_dir: '$pika_dir'"

if [[ -n "${RABBITMQ_VERSION:-}" ]]
then
    rabbitmq_ver="$RABBITMQ_VERSION"
    echo "[INFO] rabbitmq version (from RABBITMQ_VERSION): '$rabbitmq_ver'"
else
    echo '[INFO] Resolving latest RabbitMQ release...'
    rabbitmq_ver="$(
        gh release view --repo rabbitmq/rabbitmq-server --json tagName \
            --jq '.tagName | ltrimstr("v")'
    )"
    echo "[INFO] rabbitmq version: '$rabbitmq_ver'"
fi
readonly rabbitmq_ver

readonly install_dir="$script_dir/rabbitmq"

readonly archive_name="rabbitmq-server-generic-unix-$rabbitmq_ver.tar.xz"
readonly archive_path="${RUNNER_TEMP:-/tmp}/$archive_name"
readonly download_url="https://github.com/rabbitmq/rabbitmq-server/releases/download/v$rabbitmq_ver/$archive_name"
readonly rabbitmq_home="$install_dir/rabbitmq_server-$rabbitmq_ver"

echo '[INFO] Installing Erlang 27...'
brew install erlang@27
brew link --overwrite --force erlang@27

if [[ ! -d "$rabbitmq_home" ]]
then
    echo "[INFO] Downloading RabbitMQ $rabbitmq_ver..."
    mkdir -p "$install_dir"
    curl --fail --location --retry 3 --output "$archive_path" "$download_url"
    tar -xJf "$archive_path" -C "$install_dir"
else
    echo "[INFO] Found RabbitMQ $rabbitmq_ver in $rabbitmq_home"
fi

readonly rabbitmq_sbin="$rabbitmq_home/sbin"
readonly rabbitmq_server_cmd="$rabbitmq_sbin/rabbitmq-server"
readonly rabbitmqctl_cmd="$rabbitmq_sbin/rabbitmqctl"

export PATH="$rabbitmq_sbin:$PATH"
export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost'

if [[ ! -x "$rabbitmq_server_cmd" ]]
then
    echo "[ERROR] rabbitmq-server executable not found in $rabbitmq_sbin" >&2
    exit 1
fi

# Enable the TLS listener (5671) alongside plaintext (5672), pointing at the
# repo's test certs via the shared template. RABBITMQ_CONFIG_FILE is given
# without the .conf extension, per RabbitMQ convention.
readonly rabbitmq_conf_dir="$rabbitmq_home/etc/rabbitmq"
mkdir -p "$rabbitmq_conf_dir"
sed "s|PIKA_DIR|$pika_dir|g" "$script_dir/../rabbitmq.conf.in" \
    > "$rabbitmq_conf_dir/rabbitmq.conf"
export RABBITMQ_CONFIG_FILE="$rabbitmq_conf_dir/rabbitmq"
echo '[INFO] RabbitMQ configuration:'
cat "$rabbitmq_conf_dir/rabbitmq.conf"

echo '[INFO] Starting RabbitMQ...'
"$rabbitmq_server_cmd" -detached

declare -i count=12
while (( count > 0 )) && ! epmd -names | grep -F 'name rabbit'
do
    echo '[WARNING] epmd is not reporting rabbit name just yet...'
    sleep 5
    count=$((count - 1))
done

echo '[INFO] Waiting for RabbitMQ to start...'
"$rabbitmqctl_cmd" await_startup
