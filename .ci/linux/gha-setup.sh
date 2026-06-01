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

# The shared template references the certs by their absolute repo path
# (PIKA_DIR/tests/certs/...). Mounting the repo at the same path inside the
# container keeps those paths valid for the RabbitMQ server.
readonly rabbitmq_conf="${RUNNER_TEMP:-/tmp}/rabbitmq.conf"
sed "s|PIKA_DIR|$pika_dir|g" "$script_dir/../rabbitmq.conf.in" > "$rabbitmq_conf"
echo '[INFO] RabbitMQ configuration:'
cat "$rabbitmq_conf"

echo '[INFO] Starting RabbitMQ...'
docker run --detach --name rabbitmq \
    --publish 5672:5672 \
    --publish 5671:5671 \
    --volume "$pika_dir:$pika_dir:ro" \
    --volume "$rabbitmq_conf:/etc/rabbitmq/rabbitmq.conf:ro" \
    "$rabbitmq_image"

echo '[INFO] Waiting for RabbitMQ to start...'
declare -i count=24
while (( count > 0 )) && ! docker exec rabbitmq rabbitmqctl await_startup
do
    echo '[WARNING] RabbitMQ is not started just yet...'
    sleep 5
    count=$((count - 1))
done

if (( count == 0 ))
then
    echo '[ERROR] RabbitMQ failed to start' >&2
    docker logs rabbitmq
    exit 1
fi

docker exec rabbitmq rabbitmqctl status
