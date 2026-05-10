#!/usr/bin/env bash

set -euo pipefail

echo '[INFO] Installing RabbitMQ...'
brew install rabbitmq

rabbitmq_sbin="$(brew --prefix rabbitmq)/sbin"
export PATH="${rabbitmq_sbin}:${PATH}"
export RABBITMQ_SERVER_ADDITIONAL_ERL_ARGS='-rabbitmq_stream advertised_host localhost'

echo '[INFO] Starting RabbitMQ...'
rabbitmq-server -detached

echo '[INFO] Waiting for RabbitMQ to start...'
for _ in {1..30}; do
    if rabbitmq-diagnostics -q ping; then
        echo '[INFO] RabbitMQ is running!'
        rabbitmqctl status
        exit 0
    fi
    sleep 2
done

echo '[ERROR] RabbitMQ did not start in time.'
rabbitmq-diagnostics -q ping
