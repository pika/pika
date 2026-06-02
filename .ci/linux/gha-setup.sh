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

# Mount only the certs (at a fixed /certs) rather than the whole repo: a
# read-only bind mount of the repo at its host path inside the container
# intermittently triggers an ".erlang.cookie: eacces" crash in the rabbitmq
# image's entrypoint. The shared template's PIKA_DIR/tests/certs paths are
# rewritten to /certs to match the mount.
readonly rabbitmq_conf="${RUNNER_TEMP:-/tmp}/rabbitmq.conf"
sed "s|PIKA_DIR/tests/certs|/certs|g" "$script_dir/../rabbitmq.conf.in" > "$rabbitmq_conf"
echo '[INFO] RabbitMQ configuration:'
cat "$rabbitmq_conf"

# The rabbitmq image intermittently crashes on startup reading its own
# .erlang.cookie ("eacces"); per upstream the documented recovery is to
# restart so the cookie is regenerated. Recreate the container until it boots.
start_rabbitmq() {
    docker rm --force rabbitmq >/dev/null 2>&1 || true
    docker run --detach --name rabbitmq \
        --publish 5672:5672 \
        --publish 5671:5671 \
        --volume "$pika_dir/tests/certs:/certs:ro" \
        --volume "$rabbitmq_conf:/etc/rabbitmq/rabbitmq.conf:ro" \
        "$rabbitmq_image"
}

readonly max_attempts=8
started=false
for (( attempt = 1; attempt <= max_attempts; attempt++ ))
do
    echo "[INFO] Starting RabbitMQ (attempt $attempt/$max_attempts)..."
    start_rabbitmq
    for (( i = 0; i < 30; i++ ))
    do
        if docker exec rabbitmq rabbitmqctl await_startup >/dev/null 2>&1
        then
            started=true
            break
        fi
        if [[ "$(docker inspect --format '{{.State.Status}}' rabbitmq 2>/dev/null)" != running ]]
        then
            echo '[WARNING] RabbitMQ container exited during startup:'
            docker logs rabbitmq 2>&1 | tail -n 5
            break
        fi
        sleep 2
    done
    [[ "$started" == true ]] && break
done

if [[ "$started" != true ]]
then
    echo "[ERROR] RabbitMQ failed to start after $max_attempts attempts" >&2
    docker logs rabbitmq 2>&1 || true
    exit 1
fi

docker exec rabbitmq rabbitmqctl status
