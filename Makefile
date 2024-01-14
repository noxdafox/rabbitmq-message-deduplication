PROJECT = rabbitmq_message_deduplication

DEPS = rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk
DEP_EARLY_PLUGINS = rabbit_common/mk/rabbitmq-early-plugin.mk

# Mix customizations

MIX_ENV      ?= dev
override MIX := mix
elixir_srcs  := mix.exs

# RMQ `dist` target removes anything within the `plugins` folder
# which is not managed by erlang.mk.
# We need to instruct the `rabbitmq-dist:do-dist` target to not
# remove our plugin and related dependencies.
ELIXIR_ARCHIVE  = $(shell ls plugins/elixir-*.ez)
PROJECT_ARCHIVE = $(shell ls plugins/$(PROJECT)-*.ez)
EXTRA_DIST_EZS  = $(ELIXIR_ARCHIVE) $(PROJECT_ARCHIVE)

app:: $(elixir_srcs) deps
	$(MIX) make_app

dist:: app
	mkdir -p $(DIST_DIR)
	$(MIX) make_archives
	cp -r _build/$(MIX_ENV)/archives/elixir-*.ez $(DIST_DIR)
	cp -r _build/$(MIX_ENV)/archives/$(PROJECT)-*.ez $(DIST_DIR)

test-build:: app
	mkdir -p $(DIST_DIR)
	$(MIX) make_archives
	cp -r _build/$(MIX_ENV)/archives/elixir-*.ez $(DIST_DIR)
	cp -r _build/$(MIX_ENV)/archives/$(PROJECT)-*.ez $(DIST_DIR)

tests:: $(elixir_srcs) deps
	MIX_ENV=test $(MIX) make_tests

clean::
	@rm -fr _build

# erlang.mk modules

include rabbitmq-components.mk
include erlang.mk
