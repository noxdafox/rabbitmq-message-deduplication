% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2018, Matteo Cafasso.
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
                               declare_exchanges,
                               deduplicate_message,
                               deduplicate_message_ttl,
                               deduplicate_message_cache_overflow
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
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),

    %% Deduplication header absent
    publish_message(Channel, <<"test">>),
    publish_message(Channel, <<"test">>),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_ttl(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 1000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    %% Exchange default TTL
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    timer:sleep(2000),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    %% Message TTL override
    Headers = [{<<"x-cache-ttl">>, long, 500}],
    publish_message(Channel, <<"test">>, "deduplicate-that", Headers),
    timer:sleep(800),
    publish_message(Channel, <<"test">>, "deduplicate-that", Headers),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_cache_overflow(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 1, 10000)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-that"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

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

publish_message(Ch, Ex) ->
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).

publish_message(Ch, Ex, D) ->
    publish_message(Ch, Ex, D, []).

publish_message(Ch, Ex, D, H) ->
    Headers = [{<<"x-deduplication-header">>, longstr, D}] ++ H,
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Props = #'P_basic'{headers = Headers},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).
