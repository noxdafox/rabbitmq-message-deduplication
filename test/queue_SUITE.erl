% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2025, Matteo Cafasso.
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
                               dead_letter,
                               consume_no_ack,
                               queue_policy
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
    publish_messages(Channel, <<"test">>, "deduplicate-this", 13),
    {#'basic.get_ok'{delivery_tag = Tag1}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag1}),

    %% %% Integer
    publish_messages(Channel, <<"test">>, 42, 3),
    {#'basic.get_ok'{delivery_tag = Tag2}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag2}),

    %% %% Float
    publish_messages(Channel, <<"test">>, 4.2, 3),
    {#'basic.get_ok'{delivery_tag = Tag3}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag3}),

    %% %% None/null/nil/void/undefined
    publish_messages(Channel, <<"test">>, undefined, 3),
    {#'basic.get_ok'{delivery_tag = Tag4}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag4}),

    %% Deduplication header absent
    publish_messages(Channel, <<"test">>, 2),
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
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    timer:sleep(2000),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),

    %% Message TTL override
    publish_messages(Channel, <<"test">>, "deduplicate-that", <<"500">>, 1),
    timer:sleep(800),
    publish_messages(Channel, <<"test">>, "deduplicate-that", <<"500">>, 1),
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
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    true = amqp_channel:wait_for_confirms(Channel, 3),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    false = amqp_channel:wait_for_confirms(Channel, 3),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    false = amqp_channel:wait_for_confirms(Channel, 3),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

message_acknowledged(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(Channel, make_queue(<<"test">>)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    %% Acked message does not get deduplicated
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{delivery_tag = Tag}, _} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get).

queue_overflow(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    Args = [{<<"x-max-length">>, long, 1}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel,
                                              make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    %% If queue overflows, same happens for cache
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-that", 1),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{},
     #amqp_msg{props = #'P_basic'{headers = [
                                             {<<"x-deduplication-header">>,
                                              longstr,
                                              <<"deduplicate-this">>}
                                            ]
                                 }}} = amqp_channel:call(Channel, Get).

dead_letter(Config) ->
    Get = #'basic.get'{queue = <<"test">>},
    DLGet = #'basic.get'{queue = <<"dead-letter-queue">>},
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(Channel, make_queue(<<"dead-letter-queue">>)),
    bind_new_exchange(Channel, <<"dead-letter-exchange">>, <<"dead-letter-queue">>),

    Args = [{<<"x-dead-letter-exchange">>, longstr, "dead-letter-exchange"}],
    #'queue.declare_ok'{} = amqp_channel:call(Channel, make_queue(<<"test">>, Args)),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{delivery_tag = Tag}, _} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.reject'{delivery_tag = Tag, requeue = false}),

    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, Get),
    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    {#'basic.get_ok'{}, _} = amqp_channel:call(Channel, DLGet).

consume_no_ack(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{queue = Q} = amqp_channel:call(Channel, make_queue(<<"no-ack-queue">>)),
    bind_new_exchange(Channel, <<"test">>, <<"no-ack-queue">>),

    #'basic.consume_ok'{consumer_tag = _Tag} =
        amqp_channel:subscribe(Channel, #'basic.consume'{queue = Q, no_ack = true}, self()),
    receive
        #'basic.consume_ok'{} -> ok
    end,

    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    receive
        {#'basic.deliver'{}, _} -> ok
    after 1000 ->
        error(message_not_received)
    end,

    publish_messages(Channel, <<"test">>, "deduplicate-this", 1),
    receive
        {#'basic.deliver'{}, _} -> ok
    after 1000 ->
        error(message_not_received)
    end.

queue_policy(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = <<"test">>}),
    bind_new_exchange(Channel, <<"test">>, <<"test">>),

    rabbit_ct_broker_helpers:set_policy(Config, 0, <<"policy-test">>,
        <<".*">>, <<"all">>, [{<<"x-message-deduplication">>, true}]),
    %% Wait for policy propagation
    timer:sleep(1000),

    publish_messages(Channel, <<"test">>, "deduplicate-this", 2),
    Get = #'basic.get'{queue = <<"test">>},
    {#'basic.get_ok'{delivery_tag = Tag}, _} = amqp_channel:call(Channel, Get),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag}),

    %% Policy is applied to new queues
    #'queue.declare_ok'{} = amqp_channel:call(Channel, #'queue.declare'{queue = <<"test0">>}),
    bind_new_exchange(Channel, <<"test0">>, <<"test0">>),

    publish_messages(Channel, <<"test0">>, "deduplicate-this", 2),
    Get0 = #'basic.get'{queue = <<"test0">>},
    {#'basic.get_ok'{delivery_tag = Tag0}, _} = amqp_channel:call(Channel, Get0),
    #'basic.get_empty'{} = amqp_channel:call(Channel, Get0),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag0}),

    amqp_channel:call(Channel, #'exchange.delete'{exchange = <<"test0">>}),
    amqp_channel:call(Channel, #'queue.delete'{queue = <<"test0">>}),

    % Policy is cleared, default arguments are restored
    rabbit_ct_broker_helpers:clear_policy(Config, 0, <<"policy-test">>),

    publish_messages(Channel, <<"test">>, "deduplicate-this", 2),
    Get = #'basic.get'{queue = <<"test">>},
    {#'basic.get_ok'{delivery_tag = Tag1}, _} = amqp_channel:call(Channel, Get),
    {#'basic.get_ok'{delivery_tag = Tag2}, _} = amqp_channel:call(Channel, Get),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag1}),
    amqp_channel:cast(Channel, #'basic.ack'{delivery_tag = Tag2}).


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

publish_messages(_Ch, _Ex, 0) ->
    ok;
publish_messages(Ch, Ex, N) ->
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg),
    publish_messages(Ch, Ex, N-1).

publish_messages(_Ch, _Ex, _D, 0) ->
    ok;
publish_messages(Ch, Ex, D, N) ->
    Type = case D of
               D when is_integer(D) -> long;
               D when is_float(D) -> float;
               D when is_list(D) -> longstr;
               undefined -> void
           end,
    Props = #'P_basic'{headers = [{<<"x-deduplication-header">>, Type, D}]},
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg),
    publish_messages(Ch, Ex, D, N-1).

publish_messages(_Ch, _Ex, _D, _E, 0) ->
    ok;
publish_messages(Ch, Ex, D, E, N) ->
    Props = #'P_basic'{headers = [{<<"x-deduplication-header">>, longstr, D}],
                       expiration = E},
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Msg = #amqp_msg{props = Props, payload = <<"payload">>},
    amqp_channel:cast(Ch, Publish, Msg),
    publish_messages(Ch, Ex, D, E, N-1).
