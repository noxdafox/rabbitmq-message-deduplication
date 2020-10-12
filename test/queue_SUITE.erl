% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2018, Matteo Cafasso.
% All rights reserved.

-module(queue_SUITE).

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
                               deduplicate_message,
                               deduplicate_message_ttl,
                               deduplicate_message_confirm,
                               message_acknowledged,
                               queue_overflow,
                               dead_letter
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

deduplicate_message(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(Channel, make_queue(<<"test">>)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    %% Deduplication header present
    %% String
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{delivery_tag = Tag1}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag1}),

    %% Integer
    publish_message(Channel, <<"test">>, 42),
    publish_message(Channel, <<"test">>, 42),

    {#'basic.get_ok'{delivery_tag = Tag2}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag2}),

    %% Float
    publish_message(Channel, <<"test">>, 4.2),
    publish_message(Channel, <<"test">>, 4.2),

    {#'basic.get_ok'{delivery_tag = Tag3}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag3}),

    %% None/null/nil/void/undefined
    publish_message(Channel, <<"test">>, undefined),
    publish_message(Channel, <<"test">>, undefined),

    {#'basic.get_ok'{delivery_tag = Tag4}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag4}),

    %% Deduplication header absent
    publish_message(Channel, <<"test">>),
    publish_message(Channel, <<"test">>),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_ttl(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    Args = [{<<"x-message-ttl">>, long, 1000}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
                                              make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    %% Queue default TTL
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    timer:sleep(2000),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    %% Message TTL override
    publish_message(Channel, <<"test">>, "deduplicate-that", <<"500">>),
    timer:sleep(800),
    publish_message(Channel, <<"test">>, "deduplicate-that", <<"500">>),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

deduplicate_message_confirm(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),
    #'confirm.select_ok'{} = amqp_channel:call(Channel, #'confirm.select'{}),

    Args = [{<<"x-message-ttl">>, long, 1000}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
                                              make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    %% Publish and wait for confirmation
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    true = amqp_channel:wait_for_confirms(Channel, 3),
    publish_message(Channel, <<"test">>, "deduplicate-this"),
    false = amqp_channel:wait_for_confirms(Channel, 3),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

message_acknowledged(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(Channel, make_queue(<<"test">>)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{delivery_tag = Tag}, _} = amqp_channel:call(Channel, Get),

    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

queue_overflow(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    Args = [{<<"x-max-length">>, long, 1}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
                                              make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),
    publish_message(Channel, <<"test">>, "deduplicate-that"),
    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{headers = [
                                             {<<"x-deduplication-header">>,
                                              longstr,
                                              <<"deduplicate-this">>}
                                            ]
                                 }}} = amqp_channel:call(Channel, Get).

dead_letter(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    Args = [{<<"x-dead-letter-exchange">>, longstr, "amq.direct"}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
                                              make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{delivery_tag = Tag}, _} = amqp_channel:call(Channel, Get),

    amqp_channel:cast(Channel, #'basic.reject'{delivery_tag = Tag, requeue = false}),

    publish_message(Channel, <<"test">>, "deduplicate-this"),

    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

make_queue(Q) ->
    #'queue.declare'{
       queue       = Q,
       arguments   = [{<<"x-message-deduplication">>, bool, true}]}.

make_queue(Q, Args) ->
    #'queue.declare'{
       queue       = Q,
       arguments   = [{<<"x-message-deduplication">>, bool, true} | Args]}.

bind_new_exchange(Ch, Ex, Q) ->
    Exchange = #'exchange.declare'{exchange = Ex, type = <<"direct">>},
    #'exchange.declare_ok'{} = amqp_channel:call(Ch, Exchange),

    Binding = #'queue.bind'{queue = Q, exchange = Ex, routing_key = <<"#">>},
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding).

publish_message(Ch, Ex) ->
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).

publish_message(Ch, Ex, D) ->
    Type = case D of
               D when is_integer(D) -> long;
               D when is_float(D) -> float;
               D when is_list(D) -> longstr;
               undefined -> void
           end,
    Props = #'P_basic'{headers = [{<<"x-deduplication-header">>, Type, D}]},
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).

publish_message(Ch, Ex, D, E) ->
    Props = #'P_basic'{headers = [{<<"x-deduplication-header">>, longstr, D}],
                       expiration = E},
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg).
