% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2026, Matteo Cafasso.
% All rights reserved.

-module(registration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(CACHE, cache_exchange__test).
-define(CACHE_MANAGER, 'Elixir.RabbitMQMessageDeduplication.CacheManager').
-define(EXCHANGE, 'Elixir.RabbitMQMessageDeduplication.Exchange').
-define(PLUGIN, rabbitmq_message_deduplication).

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               cache_manager_precedes_registration,
                               reconfigure_caches_at_registration
                              ]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    %% The plugin owns the Mnesia backend only when Khepri is the metadata
    %% store, which is what leaves the caches to be re-created at registration.
    Config1 = rabbit_ct_helpers:set_config(Config,
                                           [{metadata_store, khepri},
                                            {rmq_nodename_suffix, ?MODULE}]),
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
    Exchange = rpc(Config, rabbit_misc, r, [<<"/">>, exchange, <<"test">>]),
    ok = rpc(Config, rabbit_exchange, ensure_deleted,
             [Exchange, false, <<"acting-user">>]),

    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% Registering the exchange type re-creates the caches through the cache
%% manager, which therefore has to be up by then.
cache_manager_precedes_registration(Config) ->
    Steps = [Step || {_App, Step, _Attributes} <-
                         rpc(Config, rabbit_boot_steps, find_steps, [])],

    ?assert(position(?CACHE_MANAGER, Steps) < position(?EXCHANGE, Steps)).

%% The plugin brings its own Mnesia backend up empty, so the caches of the
%% deduplication exchanges which already exist have to be created back when
%% the exchange type is registered. Caches missing from the caches table are
%% never visited by the maintenance routine and keep their expired entries.
reconfigure_caches_at_registration(Config) ->
    Channel = rabbit_ct_client_helpers:open_channel(Config),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel, make_exchange(<<"test">>, 10, 10000)),
    ?assertEqual([?CACHE], registered_caches(Config)),

    ok = rabbit_ct_broker_helpers:disable_plugin(Config, 0, ?PLUGIN),
    ok = rabbit_ct_broker_helpers:enable_plugin(Config, 0, ?PLUGIN),

    ?assertEqual([?CACHE], registered_caches(Config)).

%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

make_exchange(Ex, Size, TTL) ->
    #'exchange.declare'{
       exchange    = Ex,
       type        = <<"x-message-deduplication">>,
       arguments   = [{<<"x-cache-size">>, long, Size},
                      {<<"x-cache-ttl">>, long, TTL}]}.

rpc(Config, Module, Function, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, Module, Function, Args).

registered_caches(Config) ->
    rpc(Config, ?CACHE_MANAGER, caches, []).

position(Element, List) ->
    position(Element, List, 1).

position(Element, [], _Position) ->
    ct:fail({boot_step_not_found, Element});
position(Element, [Element | _Rest], Position) ->
    Position;
position(Element, [_Other | Rest], Position) ->
    position(Element, Rest, Position + 1).
