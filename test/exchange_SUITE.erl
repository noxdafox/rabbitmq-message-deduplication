% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2025, Matteo Cafasso.
% All rights reserved.

-module(exchange_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               disable_enable,
                               declare_exchanges,
                               deduplicate_message,
                               deduplicate_message_ttl,
                               deduplicate_message_cache_overflow,
                               exchange_policy
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{rmq_nodename_suffix, ?MODULE}]),
    rabbit_ct_helpers:run_setup_steps(Config1,
                                      rabbit_ct_broker_helpers:setup_steps() ++
                                      rabbit_ct_client_helpers:setup_steps()).

end_per_suite(Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_group(_, Config) -> Config.

end_per_group(_, Config) -> Config.

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    amqp_channel:call(Channel, #'exchange.delete'{exchange = <<"test">>}),
    amqp_channel:call(Channel, #'queue.delete'{queue = <<"test">>}),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% Basic smoke test that it is possible to disable, then enable the plugin
disable_enable(Config) ->
    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, rabbitmq_message_deduplication),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, rabbitmq_message_deduplication).

declare_exchanges(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    DeclareShort = #'exchange.declare'{exchange = <<"test_exchange_short">>,
                                       type = <<"x-message-deduplication">>,
                                       auto_delete = true,
                                       arguments = [{<<"x-cache-size">>, short, 10},
                                                    {<<"x-cache-ttl">>, short, 1000},
                                                    {<<"x-cache-persistence">>, longstr, "memory"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareShort),

    DeclareLong = #'exchange.declare'{exchange = <<"test_exchange_long">>,
                                      type = <<"x-message-deduplication">>,
                                      auto_delete = true,
                                      arguments = [{<<"x-cache-size">>, long, 10},
                                                   {<<"x-cache-ttl">>, long, 1000},
                                                   {<<"x-cache-persistence">>, longstr, "memory"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareLong),

    DeclareSigned = #'exchange.declare'{exchange = <<"test_exchange_signed">>,
                                        type = <<"x-message-deduplication">>,
                                        auto_delete = true,
                                        arguments = [{<<"x-cache-size">>, signedint, 10},
                                                     {<<"x-cache-ttl">>, signedint, 1000},
                                                     {<<"x-cache-persistence">>, longstr, "memory"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareSigned),

    DeclareUnsigned = #'exchange.declare'{exchange = <<"test_exchange_unsigned">>,
                                          type = <<"x-message-deduplication">>,
                                          auto_delete = true,
                                          arguments = [{<<"x-cache-size">>, unsignedint, 10},
                                                       {<<"x-cache-ttl">>, unsignedint, 1000},
                                                       {<<"x-cache-persistence">>, longstr, "memory"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareUnsigned),

    DeclareStr = #'exchange.declare'{exchange = <<"test_exchange_strings">>,
                                     type = <<"x-message-deduplication">>,
                                     auto_delete = true,
                                     arguments = [{<<"x-cache-size">>, longstr, "10"},
                                                  {<<"x-cache-ttl">>, longstr, "1000"},
                                                  {<<"x-cache-persistence">>, longstr, "disk"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareStr),

    DeclareErr = #'exchange.declare'{exchange = <<"test_exchange_error">>,
                                     type = <<"x-message-deduplication">>,
                                     arguments = [{<<"x-cache-size">>, longstr, "foo"},
                                                  {<<"x-cache-ttl">>, longstr, "bar"}]},
    ?assertExit(_, amqp_channel:call(Channel, DeclareErr)).

deduplicate_message(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 10000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    %% Deduplication header present
    publish_messages(Channel, <<"test">>, "deduplicate-this", 5),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),

    %% Deduplication header absent
    publish_messages(Channel, <<"test">>, 2),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_ttl(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 1000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    %% Exchange default TTL
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    timer:sleep(2000),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    %% Message TTL override
    Headers = [{<<"x-cache-ttl">>, long, 500}],
    publish_messages(Channel, <<"test">>, "deduplicate-that", Headers, 1),
    timer:sleep(800),
    publish_messages(Channel, <<"test">>, "deduplicate-that", Headers, 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_cache_overflow(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 1, 10000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    % Oveflow message will lead to no deduplication
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-that", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

exchange_policy(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 1, 10000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    % Cache size is increased to 2. There should not be overflow
    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"policy-test">>,
        <<".*">>, <<"all">>, [{<<"x-cache-size">>, 5}]),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-that", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),

    % Policy is cleared, default arguments are restored
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy-test">>),
    publish_messages(Channel, <<"test">>, "deduplicate-those", 5),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get).


%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

make_exchange(Ex, Size, TTL) ->
    #'exchange.declare'{
       exchange    = Ex,
       type        = <<"x-message-deduplication">>,
       arguments   = [{<<"x-cache-size">>, long, Size},
                      {<<"x-cache-ttl">>, long, TTL}]}.

bind_new_queue(Ch, Ex, Q) ->
    Queue = #'queue.declare'{queue = <<"test">>, auto_delete = true},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, Queue),

    Binding = #'queue.bind'{queue = Q, exchange = Ex, routing_key = <<"#">>},
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding).

publish_messages(_Ch, _Ex, 0) ->
    ok;
publish_messages(Ch, Ex, N) ->
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg),
    publish_messages(Ch, Ex, N-1).

publish_messages(Ch, Ex, D, N) ->
    publish_messages(Ch, Ex, D, [], N).

publish_messages(_Ch, _Ex, _D, _H, 0) ->
    ok;
publish_messages(Ch, Ex, D, H, N) ->
    Headers = [{<<"x-deduplication-header">>, longstr, D}] ++ H,
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Props = #'P_basic'{headers = Headers},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg),
    publish_messages(Ch, Ex, D, H, N-1).
