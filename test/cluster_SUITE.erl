% This Source Code Form is subject to the terms of the Mozilla Public
% License, v. 2.0. If a copy of the MPL was not distributed with this
% file, You can obtain one at http://mozilla.org/MPL/2.0/.
%
% Copyright (c) 2017-2026, Matteo Cafasso.
% All rights reserved.

%% Multi node test suite.
%%
%% When Khepri is the metadata store the broker no longer provides a clustered
%% Mnesia, so the plugin sets up and clusters its own. These tests cover the two
%% orders in which that can happen:
%%
%%   * the plugin is enabled while the nodes are already clustered
%%     (a plugin enabled on a running cluster, or any node restart once the
%%     `khepri_db` feature flag is enabled);
%%   * the plugin is enabled on standalone nodes which are clustered afterwards
%%     (a brand new cluster, or a node added to an existing one).
%%
%% Both must end up with a single deduplication cache shared by every node.
%% The first case is also tested with the plugin enabled on every node at the
%% same time.

-module(cluster_SUITE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

-compile(export_all).

-define(PLUGIN, rabbitmq_message_deduplication).
-define(CACHES, message_deduplication_caches).
-define(EXCHANGE, <<"cluster_test_exchange">>).
-define(QUEUE, <<"cluster_test_queue">>).
-define(NODES, 3).

all() ->
    [
     {group, plugin_enabled_after_clustering},
     {group, plugin_enabled_before_clustering},
     {group, plugin_enabled_concurrently}
    ].

groups() ->
    [
     {plugin_enabled_after_clustering, [], [enable_plugin_on_clustered_nodes]},
     {plugin_enabled_before_clustering, [], [cluster_nodes_with_plugin_enabled]},
     {plugin_enabled_concurrently, [], [enable_plugin_concurrently]}
    ].

%% -------------------------------------------------------------------
%% Testsuite setup/teardown.
%% -------------------------------------------------------------------

init_per_suite(Config) ->
    rabbit_ct_helpers:log_environment(),
    Config.

end_per_suite(Config) ->
    Config.

%% Each group starts its own cluster. The brokers are started with every plugin
%% disabled so that the plugin can be enabled at the point each group needs it,
%% rather than at boot.
init_per_group(Group, Config) ->
    Clustered = Group =/= plugin_enabled_before_clustering,
    Config1 = rabbit_ct_helpers:set_config(
                Config,
                [{rmq_nodename_suffix, Group},
                 {rmq_nodes_count, ?NODES},
                 {rmq_nodes_clustered, Clustered},
                 {start_rmq_with_plugins_disabled, true}]),
    rabbit_ct_helpers:run_setup_steps(
      Config1,
      rabbit_ct_broker_helpers:setup_steps() ++
          rabbit_ct_client_helpers:setup_steps()).

end_per_group(_, Config) ->
    rabbit_ct_helpers:run_teardown_steps(
      Config, rabbit_ct_client_helpers:teardown_steps() ++
          rabbit_ct_broker_helpers:teardown_steps()).

init_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_started(Config, Testcase).

end_per_testcase(Testcase, Config) ->
    rabbit_ct_helpers:testcase_finished(Config, Testcase).

%% -------------------------------------------------------------------
%% Testcases.
%% -------------------------------------------------------------------

%% The nodes are clustered first, then the plugin is enabled on each of them.
%% This is what happens when a user enables the plugin on a running cluster and
%% when a node restarts once the `khepri_db` feature flag is enabled.
enable_plugin_on_clustered_nodes(Config) ->
    case khepri_enabled(Config) of
        false ->
            {skip, "Khepri is not the metadata store on this broker"};
        true ->
            ok = assert_clustered(Config),

            ok = enable_plugin_everywhere(Config),

            ok = assert_nodes_running(Config),
            ok = assert_cache_table_everywhere(Config),
            ok = assert_deduplicates_across_nodes(Config)
    end.

%% The plugin is enabled while the nodes are still standalone, and they are
%% clustered afterwards. This is the order a new cluster is built in, and the
%% order a node is added to an existing cluster.
cluster_nodes_with_plugin_enabled(Config) ->
    case khepri_enabled(Config) of
        false ->
            {skip, "Khepri is not the metadata store on this broker"};
        true ->
            ok = enable_plugin_everywhere(Config),
            ok = cluster_nodes(Config),

            ok = assert_nodes_running(Config),
            ok = assert_cache_table_everywhere(Config),
            ok = assert_deduplicates_across_nodes(Config)
    end.

enable_plugin_concurrently(Config) ->
    case khepri_enabled(Config) of
        false ->
            {skip, "Khepri is not the metadata store on this broker"};
        true ->
            ok = assert_clustered(Config),

            ok = enable_plugin_everywhere_concurrently(Config),

            ok = assert_nodes_running(Config),
            ok = assert_cache_table_everywhere(Config),
            ok = assert_deduplicates_across_nodes(Config)
    end.

%% -------------------------------------------------------------------
%% Assertions.
%% -------------------------------------------------------------------

%% These tests only make sense when the plugin manages its own Mnesia, which is
%% the case when Khepri is the metadata store. On a Mnesia backed broker the
%% plugin piggybacks on the broker's own clustered Mnesia and none of this
%% applies.
khepri_enabled(Config) ->
    rabbit_ct_broker_helpers:rpc(
      Config, 0, rabbit_feature_flags, is_enabled, [khepri_db]).

assert_nodes_running(Config) ->
    lists:foreach(
      fun(N) ->
              Node = rabbit_ct_broker_helpers:get_node_config(Config, N, nodename),
              ?assertEqual(
                 pong, net_adm:ping(Node),
                 lists:flatten(
                   io_lib:format("node ~tp is not reachable", [Node]))),
              ?assert(rabbit_ct_broker_helpers:rpc(
                        Config, N, rabbit, is_running, []))
      end, node_indices()),
    ok.

%% `mnesia:system_info(tables)` also lists tables without a local copy, so the
%% copies are checked instead.
assert_cache_table_everywhere(Config) ->
    Nodes = lists:sort(nodenames(Config)),
    lists:foreach(
      fun(N) ->
              DbNodes = rabbit_ct_broker_helpers:rpc(
                          Config, N, mnesia, system_info, [running_db_nodes]),
              ?assertEqual(Nodes, lists:sort(DbNodes)),
              Copies = rabbit_ct_broker_helpers:rpc(
                         Config, N, mnesia, table_info, [?CACHES, ram_copies]),
              ?assertEqual(
                 Nodes, lists:sort(Copies),
                 lists:flatten(
                   io_lib:format(
                     "node ~b does not see a copy of the ~tp table on every node",
                     [N, ?CACHES])))
      end, node_indices()),
    ok.

%% The point of a clustered cache: a message deduplicated on one node must be
%% recognised as a duplicate when published through any other node.
assert_deduplicates_across_nodes(Config) ->
    Channel0 = rabbit_ct_client_helpers:open_channel(Config, 0),
    Channel1 = rabbit_ct_client_helpers:open_channel(Config, 1),

    #'exchange.declare_ok'{} = amqp_channel:call(
                                 Channel0, make_exchange(?EXCHANGE)),
    ok = bind_new_queue(Channel0, ?EXCHANGE, ?QUEUE),

    Header = <<"deduplicate-across-nodes">>,
    ok = publish_message(Channel0, ?EXCHANGE, Header),
    ok = publish_message(Channel1, ?EXCHANGE, Header),

    Get = #'basic.get'{queue = ?QUEUE},
    ?assertMatch({#'basic.get_ok'{}, _}, amqp_channel:call(Channel0, Get)),
    ?assertMatch(#'basic.get_empty'{}, amqp_channel:call(Channel0, Get),
                 "the duplicate published through the second node was routed, "
                 "the deduplication cache is not shared across the cluster"),
    ok.

assert_clustered(Config) ->
    Members = rabbit_ct_broker_helpers:rpc(
                Config, 0, rabbit_nodes, list_members, []),
    ?assertEqual(?NODES, length(Members)),
    ok.

%% -------------------------------------------------------------------
%% Utility functions.
%% -------------------------------------------------------------------

node_indices() ->
    lists:seq(0, ?NODES - 1).

nodenames(Config) ->
    rabbit_ct_broker_helpers:get_node_configs(Config, nodename).

enable_plugin_everywhere(Config) ->
    lists:foreach(
      fun(N) ->
              ?assertEqual(
                 ok,
                 rabbit_ct_broker_helpers:enable_plugin(Config, N, ?PLUGIN),
                 lists:flatten(
                   io_lib:format("could not enable the plugin on node ~b", [N])))
      end, node_indices()),
    ok.

enable_plugin_everywhere_concurrently(Config) ->
    Self = self(),
    Pids = [spawn_link(
              fun() ->
                      Result = rabbit_ct_broker_helpers:enable_plugin(
                                 Config, N, ?PLUGIN),
                      Self ! {self(), N, Result}
              end) || N <- node_indices()],
    lists:foreach(
      fun(Pid) ->
              receive
                  {Pid, N, Result} ->
                      ?assertEqual(
                         ok, Result,
                         lists:flatten(
                           io_lib:format(
                             "could not enable the plugin on node ~b", [N])))
              end
      end, Pids),
    ok.

%% Clustering is done from within the testcase so that a failure is reported as
%% a test failure rather than as a group setup error.
cluster_nodes(Config) ->
    try rabbit_ct_broker_helpers:cluster_nodes(Config) of
        {skip, Reason} -> ct:fail("could not cluster the nodes: ~tp", [Reason]);
        _ -> ok
    catch
        Class:Reason:Stacktrace ->
            ct:fail("clustering the nodes failed: ~tp:~tp~n~tp",
                    [Class, Reason, Stacktrace])
    end.

make_exchange(Ex) ->
    #'exchange.declare'{
       exchange  = Ex,
       type      = <<"x-message-deduplication">>,
       arguments = [{<<"x-cache-size">>, long, 100},
                    {<<"x-cache-ttl">>, long, 60000}]}.

bind_new_queue(Ch, Ex, Q) ->
    Queue = #'queue.declare'{queue = Q, durable = true},
    #'queue.declare_ok'{} = amqp_channel:call(Ch, Queue),

    Binding = #'queue.bind'{queue = Q, exchange = Ex, routing_key = <<"#">>},
    #'queue.bind_ok'{} = amqp_channel:call(Ch, Binding),
    ok.

%% Published with confirms so that the two publishes cannot race each other:
%% the duplicate must reach the exchange after the original was cached.
publish_message(Ch, Ex, Header) ->
    #'confirm.select_ok'{} = amqp_channel:call(Ch, #'confirm.select'{}),
    Headers = [{<<"x-deduplication-header">>, longstr, Header}],
    Publish = #'basic.publish'{exchange = Ex, routing_key = <<"#">>},
    Props = #'P_basic'{headers = Headers},
    ok = amqp_channel:call(Ch, Publish, #amqp_msg{props = Props,
                                                  payload = <<"payload">>}),
    true = amqp_channel:wait_for_confirms(Ch, 30),
    ok.
