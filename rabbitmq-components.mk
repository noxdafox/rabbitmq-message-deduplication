# Inspired by erlang.mk bootstrap Makefile.
# Fetch updated rabbitmq-components.mk from rabbitmq-common.

RABBITMQ_COMMON_DIR ?= .rabbitmq-components.mk.build

rabbitmq-components.mk: rabbitmq-components-bootstrap
	git clone https://github.com/rabbitmq/rabbitmq-common $(RABBITMQ_COMMON_DIR)
	cp $(RABBITMQ_COMMON_DIR)/mk/rabbitmq-components.mk ./rabbitmq-components.mk
	rm -rf $(RABBITMQ_COMMON_DIR)

.PHONY: rabbitmq-components-bootstrap
rabbitmq-components-bootstrap: ;
