## Pika Integration Tests

Install the necessary packages with `pip install -r testing/integration/requirements.txt`.
Next copy `testing/integration/broker.conf.in` to `testing/integration/broker.conf` and 
tweak settings to suit. The tests can be run with `make integration` or with
`nosetests -w testing/integration`.

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
