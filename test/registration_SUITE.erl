% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2026, Matteo Cafasso.
% All rights reserved.

-module(registration_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-compile(export_all).

-define(CACHE_MANAGER, 'Elixir.RabbitMQMessageDeduplication.CacheManager').
-define(EXCHANGE, 'Elixir.RabbitMQMessageDeduplication.Exchange').

all() ->
    [
     {group, non_parallel_tests}
    ].

groups() ->
    [
     {non_parallel_tests, [], [
                               cache_manager_precedes_registration
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

%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

rpc(Config, Module, Function, Args) ->
    rabbit_ct_broker_helpers:rpc(Config, 0, Module, Function, Args).

position(Element, List) ->
    position(Element, List, 1).

position(Element, [], _Position) ->
    ct:fail({boot_step_not_found, Element});
position(Element, [Element | _Rest], Position) ->
    Position;
position(Element, [_Other | Rest], Position) ->
    position(Element, Rest, Position + 1).
