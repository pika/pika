# Exports the following environment variables
#
# NOTE: the locations correlate to the testca tree created by
#    configure_rabbitmq_for_ssl.sh
#
# Reference https://www.rabbitmq.com/ssl.html
#
# PIKA_SSL_TEST_CA_DIR: directory where to create the certificate authority files
# PIKA_SSL_TEST_CLIENT_CERT_DIR: directory where to create client certs
#
# PIKA_SSL_CACERTS_FILEPATH: path of the cacert.pem file
#
# PIKA_SSL_CLIENT_KEY_FILEPATH: path of the client private key file
# PIKA_SSL_CLIENT_CERT_FILEPATH: path of the client public certificate file


  pushd $HOME
  _CERT_ROOT_DIR="$(pwd)/rabbitmqcerts"
  popd


  export PIKA_SSL_TEST_CA_DIR="${_CERT_ROOT_DIR}/testca"

  export PIKA_SSL_CACERTS_FILEPATH="${PIKA_SSL_TEST_CA_DIR}/cacert.pem"

  export PIKA_SSL_TEST_CLIENT_CERT_DIR="${_CERT_ROOT_DIR}/client"
  export PIKA_SSL_CLIENT_KEY_FILEPATH="${PIKA_SSL_TEST_CLIENT_CERT_DIR}/key.pem"
  export PIKA_SSL_CLIENT_CERT_FILEPATH="${PIKA_SSL_TEST_CLIENT_CERT_DIR}/cert.pem"
