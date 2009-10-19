SIBLING_CODEGEN_DIR=../rabbitmq-codegen/
AMQP_CODEGEN_DIR=$(shell [ -d $(SIBLING_CODEGEN_DIR) ] && echo $(SIBLING_CODEGEN_DIR) || echo codegen)
AMQP_SPEC_JSON_PATH=$(AMQP_CODEGEN_DIR)/amqp-0.8.json

PYTHON=python

all: pika/spec.py

pika/spec.py: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_PATH)
	$(PYTHON) codegen.py body $(AMQP_SPEC_JSON_PATH) $@

clean:
	rm -f pika/spec.py
	rm -f pika/*.pyc 
	rm -f tests/*.pyc tests/.coverage

codegen:
	mkdir -p $@
	cp -r "$(AMQP_CODEGEN_DIR)"/* $@
	$(MAKE) -C $@ clean

tests: test

test: all
	cd tests && PYTHONPATH=.. $(PYTHON) run.py ../pika pika
