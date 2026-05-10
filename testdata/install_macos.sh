#!/usr/bin/env bash

set -euo pipefail

script_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
versions_path="${script_dir}/versions.json"
rabbitmq_ver="$(
    python3 -c 'import json, sys; print(json.load(open(sys.argv[1]))["rabbitmq"])' \
        "${versions_path}"
)"

install_dir="${HOME}/rabbitmq"
archive_name="rabbitmq-server-generic-unix-${rabbitmq_ver}.tar.xz"
archive_path="${RUNNER_TEMP:-/tmp}/${archive_name}"
download_url="https://github.com/rabbitmq/rabbitmq-server/releases/download/v${rabbitmq_ver}/${archive_name}"
rabbitmq_home="${install_dir}/rabbitmq_server-${rabbitmq_ver}"

echo '[INFO] Installing Erlang...'
brew install erlang

if [ ! -d "${rabbitmq_home}" ]; then
    echo "[INFO] Downloading RabbitMQ ${rabbitmq_ver}..."
    mkdir -p "${install_dir}"
    curl --fail --location --retry 3 --output "${archive_path}" \
        "${download_url}"
    tar -xJf "${archive_path}" -C "${install_dir}"
else
    echo "[INFO] Found RabbitMQ ${rabbitmq_ver} in ${rabbitmq_home}"
fi

rabbitmq_sbin="${rabbitmq_home}/sbin"
export PATH="${rabbitmq_sbin}:${PATH}"
export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost'

if [ ! -x "${rabbitmq_sbin}/rabbitmq-server" ]; then
    echo "[ERROR] RabbitMQ executable not found in ${rabbitmq_sbin}"
    exit 1
fi

echo '[INFO] Starting RabbitMQ...'
rabbitmq-server -detached

echo '[INFO] Waiting for RabbitMQ to start...'
for _ in {1..30}; do
    if rabbitmq-diagnostics -q check_running &&
        rabbitmq-diagnostics -q check_port_listener 5672; then
        echo '[INFO] RabbitMQ is running!'
        rabbitmqctl status
        exit 0
    fi
    sleep 2
done

echo '[ERROR] RabbitMQ did not start in time.'
rabbitmq-diagnostics -q check_running
