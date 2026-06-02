#!/usr/bin/env bash

set -o errexit
set -o nounset
set -o pipefail
set -o xtrace

script_dir="$(CDPATH='' cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
readonly script_dir
pika_dir="$(CDPATH='' cd "$script_dir/../.." && pwd)"
readonly pika_dir
echo "[INFO] pika_dir: '$pika_dir'"

if [[ -n "${RABBITMQ_VERSION:-}" ]]
then
    rabbitmq_image="rabbitmq:$RABBITMQ_VERSION"
    echo "[INFO] rabbitmq image (from RABBITMQ_VERSION): '$rabbitmq_image'"
else
    rabbitmq_image='rabbitmq:latest'
    echo "[INFO] rabbitmq image: '$rabbitmq_image'"
fi
readonly rabbitmq_image

readonly rabbitmq_conf="$script_dir/rabbitmq.conf"
echo '[INFO] RabbitMQ configuration:'
cat "$rabbitmq_conf"

docker rm --force rabbitmq >/dev/null 2>&1 || true
docker run --detach \
    --name rabbitmq \
    --hostname pika-rabbitmq \
    --publish 5672:5672 \
    --publish 5671:5671 \
    --volume "$pika_dir/tests/certs:/etc/rabbitmq/certs:ro" \
    --volume "$rabbitmq_conf:/etc/rabbitmq/rabbitmq.conf:ro" \
    "$rabbitmq_image"

declare -i count=60
until docker exec --user rabbitmq rabbitmq rabbitmqctl await_startup >/dev/null 2>&1
do
    if (( --count == 0 ))
    then
        echo '[ERROR] RabbitMQ did not start in time' >&2
        docker ps -a
        docker logs rabbitmq 2>&1 || true
        docker inspect rabbitmq --format '{{.State.Status}} {{.State.ExitCode}} {{.State.Error}}' || true
        docker exec rabbitmq ls -la /var/lib/rabbitmq || true
        exit 1
    fi
    sleep 1
done

docker logs rabbitmq 2>&1 | tail -n 200 || true
docker exec --user rabbitmq rabbitmq rabbitmq-diagnostics listeners
docker exec --user rabbitmq rabbitmq rabbitmq-diagnostics status
