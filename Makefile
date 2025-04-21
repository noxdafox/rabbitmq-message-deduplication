PROJECT = rabbitmq_message_deduplication

RABBITMQ_VERSION ?= v4.1.x
current_rmq_ref = $(RABBITMQ_VERSION)

# The Application needs to depend on `rabbit` in order to be detected as a plugin.
DEPS = rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk

# Mix customizations
MIX_ENV      ?= dev
override MIX := mix

# RMQ `dist` target removes anything within the `plugins` folder
# which is not managed by erlang.mk.
# We need to instruct the `rabbitmq-dist:do-dist` target to not
# remove our plugin and related dependencies.
EXTRA_DIST_EZS = $(shell find $(DIST_DIR) -name '*.ez')

app:: deps
	$(MIX) make_app

tests::
	$(MIX) make_tests

dist:: app
	mkdir -p $(DIST_DIR)
	$(MIX) make_archives

clean::
	@rm -fr _build

include rabbitmq-components.mk
include erlang.mk
