SIBLING_CODEGEN_DIR=../rabbitmq-codegen/
AMQP_CODEGEN_DIR=$(shell [ -d $(SIBLING_CODEGEN_DIR) ] && echo $(SIBLING_CODEGEN_DIR) || echo codegen)
AMQP_SPEC_JSON_FILES=$(AMQP_CODEGEN_DIR)/amqp-rabbitmq-0.9.1.json

ifeq ($(shell python -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python
else
ifeq ($(shell python2.6 -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python2.6
else
ifeq ($(shell python2.5 -c 'import simplejson' 2>/dev/null && echo yes),yes)
PYTHON=python2.5
else
# Hmm. Missing simplejson?
PYTHON=python
endif
endif
endif

all: pika/spec.py

pika/spec.py: codegen.py $(AMQP_CODEGEN_DIR)/amqp_codegen.py $(AMQP_SPEC_JSON_FILES)
	$(PYTHON) codegen.py spec $(AMQP_SPEC_JSON_FILES) $@

# For dev work, when working from a git checkout
codegen/amqp_codegen.py:
	if [ -d codegen ]; then rmdir codegen; else true; fi
	curl http://hg.rabbitmq.com/rabbitmq-codegen/archive/default.tar.bz2 | tar -jxvf -
	mv rabbitmq-codegen-default codegen

regenclean: clean
	rm -f pika/spec.py

clean:
	rm -f pika/*.pyc 
	rm -f tests/*.pyc tests/.coverage

# For building a releasable tarball
codegen:
	mkdir -p $@
	cp -r "$(AMQP_CODEGEN_DIR)"/* $@
	$(MAKE) -C $@ clean

tests: test

test: all
	cd tests && PYTHONPATH=.. $(PYTHON) run.py ../pika pika
