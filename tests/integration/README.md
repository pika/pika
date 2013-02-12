## Pika Integration Tests

Install the necessary packages with `pip install -r tests/integration/requirements.txt`.
Next copy `tests/integration/broker.conf.in` to `tests/integration/broker.conf` and 
tweak settings to suit. The tests can be run with `make integration` or with
`nosetests -w tests/integration`.

### Dependencies
* nose
* PyYAML

### Default config settings

#### username
admin

#### password
secret

#### host
localhost

#### port
5672

#### virtual_host
testing
