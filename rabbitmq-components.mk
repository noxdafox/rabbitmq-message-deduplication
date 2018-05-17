# Inspired by erlang.mk bootstrap Makefile.
# Fetch updated rabbitmq-components.mk from rabbitmq-server.

RABBITMQ_SERVER_DIR ?= .rabbitmq-components.mk.build

rabbitmq-components.mk: rabbitmq-components-bootstrap
	git clone https://github.com/rabbitmq/rabbitmq-server $(RABBITMQ_SERVER_DIR)
	cp $(RABBITMQ_SERVER_DIR)/rabbitmq-components.mk ./rabbitmq-components.mk
	rm -rf $(RABBITMQ_SERVER_DIR)

.PHONY: rabbitmq-components-bootstrap
rabbitmq-components-bootstrap: ;
