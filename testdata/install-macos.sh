#!/usr/bin/env bash

set -o errexit
set -o pipefail
set -o xtrace

script_dir="$(CDPATH='' cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
echo "[INFO] script_dir: '$script_dir'"

readonly versions_path="$script_dir/versions.json"

rabbitmq_ver="$(
    python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["rabbitmq"])' "$versions_path"
)"
readonly rabbitmq_ver

readonly install_dir="$script_dir/rabbitmq"

readonly archive_name="rabbitmq-server-generic-unix-$rabbitmq_ver.tar.xz"
readonly archive_path="${RUNNER_TEMP:-/tmp}/$archive_name"
readonly download_url="https://github.com/rabbitmq/rabbitmq-server/releases/download/v$rabbitmq_ver/$archive_name"
readonly rabbitmq_home="$install_dir/rabbitmq_server-$rabbitmq_ver"

echo '[INFO] Installing Erlang...'
brew install erlang

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
    echo "[ERROR] rabbitmq-server executable not found in $rabbitmq_sbin" 1>&2
    exit 1
fi

echo '[INFO] Starting RabbitMQ...'
"$rabbitmq_server_cmd" -detached

echo '[INFO] Waiting for RabbitMQ to start...'
"$rabbitmqctl_cmd" await_startup
