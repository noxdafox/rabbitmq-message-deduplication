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
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

declare_exchanges(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    DeclareInt = #'exchange.declare'{exchange = <<"test_exchange_integers">>,
                                     type = <<"x-message-deduplication">>,
                                     auto_delete = true,
                                     arguments = [{<<"x-cache-size">>, long, 10},
                                                  {<<"x-cache-ttl">>, long, 1000},
                                                  {<<"x-cache-persistence">>, longstr, "memory"}]},
    #'exchange.declare_ok'{} = amqp_channel:call(Channel, DeclareInt),

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
    ?assertExit(_, amqp_channel:call(Channel, DeclareErr)),

    ok.

deduplicate_message(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 10)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    timer:sleep(2000),

    Get = #'basic.get'{queue = <<"test">>},
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),

    delete_exchange(Channel, <<"test">>),
    delete_queue(Channel, <<"test">>),

    ok.

deduplicate_message_ttl(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 1)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),
    timer:sleep(2000),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    Get = #'basic.get'{queue = <<"test">>},
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    delete_exchange(Channel, <<"test">>),
    delete_queue(Channel, <<"test">>),

    ok.

deduplicate_message_cache_overflow(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 1, 10)),
    bind_new_queue(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-that"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    timer:sleep(2000),

    Get = #'basic.get'{queue = <<"test">>},
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    delete_exchange(Channel, <<"test">>),
    delete_queue(Channel, <<"test">>),

    ok.

%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

make_exchange(Ex, Size, TTL) ->
    #'exchange.declare'{
       exchange    = Ex,
       type        = <<"x-message-deduplication">>,
       arguments   = [{<<"x-cache-size">>, long, Size},
                      {<<"x-cache-ttl">>, long, TTL}]}.

delete_exchange(Ch, Ex) ->
    Delete = #'exchange.delete'{exchange = Ex},
    #'exchange.delete_ok'{} = amqp_channel:call(Ch, Delete).

bind_new_queue(Ch, Ex, Q) ->
    Queue = #'queue.declare'{queue = <<"test">>, auto_delete = true},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, Queue),

    Binding = #'queue.bind'{queue = Q, exchange = Ex, routing_key = <<"#">>},
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding).

delete_queue(Ch, Q) ->
    #'queue.delete_ok'{} = amqp_channel:call(Ch, #'queue.delete'{queue = Q}).

publish_message(Ch, Ex, D) ->
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Props = #'P_basic'{headers = [{<<"x-deduplication-header">>, longstr, D}]},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).
