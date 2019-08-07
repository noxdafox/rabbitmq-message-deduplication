PROJECT = rabbitmq_message_deduplication

BUILD_DEPS = rabbitmq_cli
DEPS = rabbit_common rabbit
TEST_DEPS = rabbitmq_ct_helpers rabbitmq_ct_client_helpers amqp_client

DEP_PLUGINS = rabbit_common/mk/rabbitmq-plugin.mk

override MIX := mix
elixir_srcs  := mix.exs

app:: $(elixir_srcs) deps
	$(MIX) make_all

tests:: $(elixir_srcs) deps
	MIX_ENV=test $(MIX) make_tests

# FIXME: Use erlang.mk patched for RabbitMQ, while waiting for PRs to be
# reviewed and merged.

ERLANG_MK_REPO = https://github.com/rabbitmq/erlang.mk.git
ERLANG_MK_COMMIT = rabbitmq-tmp

include rabbitmq-components.mk
include erlang.mk
