# 1. Create certificate tree at location determined by get_ssl_test_vars.sh
# 2. Create a rabbitmq configuration file with SSL settings
# 3. Restart rabbitmq so that it picks up the new configuration file
#
# ASSUMES:
#
# On entry, working directory is the pika project directory
#
#
# ON EXIT: the working directory is preserved


set -o errexit
set -o pipefail
set -o nounset

set -o verbose
set -o xtrace


(
  _PROJECT_DIR=$(pwd)

  _CI_UTILS_DIR="${_PROJECT_DIR}/ci/travis"

  # Get certificate authority and client certificate directory paths
  # PIKA_SSL_TEST_CA_DIR and PIKA_SSL_TEST_CLIENT_CERT_DIR
  source "${_CI_UTILS_DIR}/get_ssl_test_vars.sh"

  #
  # Create the Certificate Authority and broker certificate files
  #
  echo "Generating Certificate Authority in ${PIKA_SSL_TEST_CA_DIR}..." >&2
  mkdir -p "${PIKA_SSL_TEST_CA_DIR}"
  cd "${PIKA_SSL_TEST_CA_DIR}"
  mkdir certs private
  chmod 700 private
  echo 01 > serial
  touch index.txt

  cp "${_CI_UTILS_DIR}/openssl.cnf" .

  # Generate the key and certificates that our test Certificate Authority will
  # use. Still within the testca directory:
  openssl req -x509 -config openssl.cnf -newkey rsa:2048 -days 365 \
      -out cacert.pem -outform PEM -subj /CN=MyTestCA/ -nodes
  openssl x509 -in cacert.pem -out cacert.cer -outform DER

# Generate the server certs
  cd ..
  _CERT_PARENT_DIR=$(pwd)
  mkdir server
  cd server
  _SERVER_CERT_DIR=$(pwd)
  echo "Generating server certificates in ${_SERVER_CERT_DIR}..." >&2
  openssl genrsa -out key.pem 2048
  openssl req -new -key key.pem -out req.pem -outform PEM \
      -subj /CN=$(hostname)/O=server/ -nodes
  cd "${PIKA_SSL_TEST_CA_DIR}"
  openssl ca -config openssl.cnf -in ../server/req.pem -out \
      ../server/cert.pem -notext -batch -extensions server_ca_extensions
  cd ../server
  openssl pkcs12 -export -out keycert.p12 -in cert.pem -inkey key.pem -passout pass:MySecretPassword


  #
  # Create the client cert files
  #
  echo "Generating client certificates in ${PIKA_SSL_TEST_CLIENT_CERT_DIR}..." >&2
  mkdir -p "${PIKA_SSL_TEST_CLIENT_CERT_DIR}"
  cd "${PIKA_SSL_TEST_CLIENT_CERT_DIR}"
  openssl genrsa -out key.pem 2048
  openssl req -new -key key.pem -out req.pem -outform PEM \
      -subj /CN=$(hostname)/O=client/ -nodes
  cd "${PIKA_SSL_TEST_CA_DIR}"
  openssl ca -config openssl.cnf -in ../client/req.pem -out \
      ../client/cert.pem -notext -batch -extensions client_ca_extensions
  cd "${PIKA_SSL_TEST_CLIENT_CERT_DIR}"
  openssl pkcs12 -export -out keycert.p12 -in cert.pem -inkey key.pem -passout pass:MySecretPassword

  # Get the directory where rabbitmq.config belongs
  #
  # We're looking for a line that looks like this in the rabbitmq environment
  # (example taken from OS X):
  #   {enabled_plugins_file,"/usr/local/etc/rabbitmq/enabled_plugins"},
  #
  _RABBITMQ_ENVIRONMENT=$(sudo rabbitmqctl environment)
  echo "${_RABBITMQ_ENVIRONMENT}" >&2
  _ENABLED_PLUGINS_FILEPATH=$(echo "${_RABBITMQ_ENVIRONMENT}" | grep enabled_plugins_file | cut -d "\"" -f 2)
  _RABBITMQ_CONFIG_DIR=$(dirname ${_ENABLED_PLUGINS_FILEPATH})
  echo "Discovered RabbitMQ Config directory: ${_RABBITMQ_CONFIG_DIR}" >&2
  ls "${_RABBITMQ_CONFIG_DIR}" >&2

  #
  # Generate the rabbitmq configuration with ssl settings
  #
  _RABBITMQ_CONFIG_FILE_PATH="${_RABBITMQ_CONFIG_DIR}/rabbitmq.config"
  if [ -f "${_RABBITMQ_CONFIG_FILE_PATH}" ]; then
    echo "rabbitmq.config exists: ${_RABBITMQ_CONFIG_FILE_PATH}; dumping contents...";
    cat "${_RABBITMQ_CONFIG_FILE_PATH}";
  fi

  echo "Generating the rabbitmq configuration with ssl settings..." >&2
  _RABBITMQ_CONFIG_CONTENT="
[
  {rabbit,
    [
      {ssl_listeners, [{\"127.0.0.1\", 5671}]},

      %% Configuring SSL.
      %% See http://www.rabbitmq.com/ssl.html for full documentation.
      %%
      {ssl_options, [{cacertfile,           \"${PIKA_SSL_CACERTS_FILEPATH}\"},
                     {certfile,             \"${_SERVER_CERT_DIR}/cert.pem\"},
                     {keyfile,              \"${_SERVER_CERT_DIR}/key.pem\"},
                     {verify,               verify_peer},
                     {fail_if_no_peer_cert, false}]}
    ]
  }
].
"

  #
  # Restart rabbitmq broker so that it will pick up the new configuration
  #

)