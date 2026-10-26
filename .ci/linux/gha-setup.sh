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

# The examples job also needs the management API, which .ci/run_examples.py
# resets the broker through, and SASL EXTERNAL.
readonly examples="${PIKA_CI_EXAMPLES:-false}"
examples_args=()
if [[ "$examples" == 'true' ]]
then
    examples_args=(
        --publish 15672:15672
        --volume "$script_dir/examples.conf:/etc/rabbitmq/conf.d/90-examples.conf:ro"
    )
fi

docker rm --force rabbitmq >/dev/null 2>&1 || true
docker run --detach \
    --name rabbitmq \
    --hostname pika-rabbitmq \
    --publish 5672:5672 \
    --publish 5671:5671 \
    --volume "$pika_dir/tests/certs:/etc/rabbitmq/certs:ro" \
    --volume "$rabbitmq_conf:/etc/rabbitmq/rabbitmq.conf:ro" \
    "${examples_args[@]}" \
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

if [[ "$examples" == 'true' ]]
then
    readonly cert_user='O=client,CN=shostakovich'
    docker exec --user rabbitmq rabbitmq rabbitmq-plugins enable rabbitmq_management rabbitmq_auth_mechanism_ssl
    # tls_mutual_authentication_twisted.py logs in as its client certificate's
    # name, by the certificate; nobody knows the password.
    docker exec --user rabbitmq rabbitmq rabbitmqctl add_user "$cert_user" "$(openssl rand -hex 16)"
    docker exec --user rabbitmq rabbitmq rabbitmqctl set_permissions -p / "$cert_user" '.*' '.*' '.*'
fi

docker logs rabbitmq 2>&1 | tail -n 200 || true
docker exec --user rabbitmq rabbitmq rabbitmq-diagnostics listeners
docker exec --user rabbitmq rabbitmq rabbitmq-diagnostics status
