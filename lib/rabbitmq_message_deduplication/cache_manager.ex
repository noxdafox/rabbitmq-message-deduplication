# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2026, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager do
  @moduledoc """
  The Cache Manager takes care of creating, maintaining and destroying caches.
  """

  use GenServer

  require Logger
  require RabbitMQMessageDeduplication.Cache

  alias :os, as: OS
  alias :erpc, as: ERPC
  alias :timer, as: Timer
  alias :global, as: Global
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.Common, as: Common
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManagerState

  @caches :message_deduplication_caches

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {
    __MODULE__,
    [description: "message deduplication plugin cache maintenance process",
     mfa: {:rabbit_sup, :start_child, [__MODULE__]},
     cleanup: {:rabbit_sup, :stop_child, [__MODULE__]},
     requires: :database,
     enables: :external_infrastructure]}

  defstruct last_log: 0, managed_mnesia: true

  def start_link(testing \\ false) do
    init_arg = case testing do
                 true -> {[], true}
                 false -> {Common.cluster_nodes(), Common.managed_mnesia()}
               end

    GenServer.start_link(__MODULE__, init_arg, name: __MODULE__)
  end

  @doc """
  Create the cache and register it within the maintenance process.
  """
  @spec create(atom, list) :: :ok | { :error, any }
  def create(cache, options) do
    try do
      timeout = Common.cache_wait_time() + Timer.seconds(5)

      GenServer.call(__MODULE__, {:create, cache, options}, timeout)
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  @doc """
  Destroy the cache and remove it from the maintenance process.
  """
  @spec destroy(atom) :: :ok | { :error, any }
  def destroy(cache) do
    try do
      GenServer.call(__MODULE__, {:destroy, cache})
    catch
      :exit, {:noproc, _} -> {:error, :noproc}
    end
  end

  ## Server Callbacks

  # Initialize Mnesia backend and start maintenance routine
  def init({cluster_nodes, managed_mnesia}) do
    :ok = if managed_mnesia, do: init_mnesia(cluster_nodes)
    {:ok, _} = Mnesia.subscribe(:system)

    Process.flag(:trap_exit, true)
    Process.send_after(__MODULE__, :maintenance, Common.maintenance_period())

    {:ok, %CacheManagerState{last_log: log_status(0), managed_mnesia: managed_mnesia}}
  end

  def terminate(reason, state) do
    Logger.debug("Terminating Cache Manager, reason: #{inspect(reason)}")

    {:ok, _} = Mnesia.unsubscribe(:system)
    # Stop Mnesia asynchronously to avoid deadlocks during process termination
    if state.managed_mnesia, do: spawn(fn() -> Mnesia.stop() end)
  end

  # Create the cache and add it to the Mnesia caches table
  def handle_call({:create, cache, options}, _from, state) do
    function = fn -> Mnesia.write({@caches, cache, :nil}) end

    with :ok <- Cache.create(cache, options),
         {:atomic, result} <- Mnesia.transaction(function)
    do
      {:reply, result, state}
    else
      {:aborted, reason} -> {:reply, {:error, reason}, state}
      error -> {:reply, error, state}
    end
  end

  # Drop the cache and remove it from the Mnesia caches table
  def handle_call({:destroy, cache}, _from, state) do
    function = fn -> Mnesia.delete({@caches, cache}) end

    with :ok <- Cache.drop(cache),
         {:atomic, result} <- Mnesia.transaction(function)
    do
      {:reply, result, state}
    else
      {:aborted, reason} -> {:reply, {:error, reason}, state}
      error -> {:reply, error, state}
    end
  end

  # Periodic Mnesia maintenance loop
  def handle_info(:maintenance, state) do
    # Ensure system events subscription persistence
    Mnesia.subscribe(:system)

    # Remove expired entries from all caches
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    Enum.each(caches, &Cache.delete_expired_entries/1)

    Process.send_after(__MODULE__, :maintenance, Common.maintenance_period())

    {:noreply, %{state | last_log: log_status(state.last_log)}}
  end

  # On node addition distribute cache tables
  def handle_info({:mnesia_system_event, {:mnesia_up, _}}, state) do
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    Enum.each(caches, &Cache.rebalance_replicas/1)

    {:noreply, state}
  end

  # Inconsistent database detected, attempt reconciliation
  def handle_info({:mnesia_system_event, {:inconsistent_database, _, node}}, state) do
    if state.managed_mnesia do
      Global.trans({__MODULE__, self()}, fn -> attempt_reconciliation(node) end)
    end

    {:noreply, state}
  end

  def handle_info({:mnesia_system_event, _}, state), do: {:noreply, state}

  # Run Mnesia functions handling output
  defmacro mnesia_wrap(function) do
    quote do
      case unquote(function) do
        :yes -> :ok                                    # force load table
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, _}} -> :ok        # table already exists
        {:aborted, {:already_exists, _, _}} -> :ok     # table copy already exists
        {:aborted, {:already_exists, _, _, _}} -> :ok  # schema already exists
        error -> error
      end
    end
  end

  # Initialize Mnesia DB or join existing cluster
  defp init_mnesia(cluster_nodes) do
    case cluster_nodes do
      [] -> init_cluster()
      [_ | _] -> join_cluster(cluster_nodes)
    end
  end

  # Initialize Mnesia DB
  defp init_cluster() do
    Logger.debug("Initializing Mnesia cluster on node #{inspect(node())}")

    with :ok <- Mnesia.start(),
         :ok <- mnesia_wrap(Mnesia.change_table_copy_type(:schema, node(), :disc_copies)),
         :ok <- mnesia_wrap(Mnesia.create_table(@caches, [])),
         :ok <- Mnesia.wait_for_tables([@caches], Common.cache_wait_time())
    do
      Logger.info("Mnesia cluster initialized on node #{inspect node()}")
    else
      {:timeout, [@caches]} ->
        Logger.warning("Forcing the load of Mnesia table: #{inspect(@caches)}")
        mnesia_wrap(Mnesia.force_load_table(@caches))
      error ->
        Logger.error("Unable to initialize Mnesia cluster, error: #{inspect(error)}")
        error
    end
  end

  # Join existing Mnesia cluster
  defp join_cluster(cluster_nodes) do
    Logger.debug("Joining Mnesia cluster nodes #{inspect(cluster_nodes)}")

    with :stopped <- Mnesia.stop(),
         :ok <- Mnesia.set_master_nodes(cluster_nodes),
         :ok <- Mnesia.start(),
         {:ok, nodes} <- Mnesia.change_config(:extra_db_nodes, cluster_nodes),
         :ok <- mnesia_wrap(Mnesia.change_table_copy_type(:schema, node(), :disc_copies)),
         :ok <- mnesia_wrap(Mnesia.add_table_copy(@caches, node(), :ram_copies)),
         :ok <- Mnesia.wait_for_tables([@caches], Common.cache_wait_time())
    do
      Logger.info("Node #{inspect(node())} joined Mnesia cluster #{inspect(nodes)}")
    else
      {:timeout, [@caches]} ->
        Logger.warning("Forcing the load of Mnesia table: #{inspect(@caches)}")
        mnesia_wrap(Mnesia.force_load_table(@caches))
      error ->
        Logger.error("Unable to join Mnesia cluster, error: #{inspect(error)}")
        error
    end
  end

  # If a network partition is detected, attempt to reconciliate the Mnesia cluster
  defp attempt_reconciliation(node) do
    case :running_db_nodes |> Mnesia.system_info() |> Enum.member?(node) do
      true ->
        Logger.debug("Node #{inspect(node)} already reconciliated with Mnesia cluster")
      false ->
        Logger.warning("Mnesia partition detected, reconciliating node: #{inspect(node)}")

        reconciliations = [@caches | find_split_tables(node)]
        |> Enum.reduce([], fn(table, acc) -> find_split_nodes(table, node, acc) end)
        |> Enum.map(&reconciliate_node/1)

        nodes = Keyword.keys(reconciliations)
        tables = reconciliations |> Keyword.values() |> List.flatten() |> Enum.uniq()

        Logger.info(
          "Reconciliated Mnesia tables #{inspect(tables)} across nodes: #{inspect(nodes)}")
    end
  end

  # For the given table, update the split nodes adding each reconciliation instruction
  # as keyword {table, master nodes}.
  defp find_split_nodes(table, node, reconciliations) do
    [master_nodes, split_nodes] = find_cluster_split(node, table)

    Enum.reduce(
      split_nodes,
      reconciliations,
      fn(split_node, acc) ->
        Keyword.update(acc,
          split_node,
          [{table, master_nodes}],
          fn(tables) -> [{table, master_nodes} | tables] end
        )
      end
    )
  end

  # Set master node for all given tables on the given node and restart Mnesia.
  defp reconciliate_node({node, tables}) do
    table_names = Enum.map(tables, fn({table, _}) -> table end)

    :stopped = mnesia_rpc_call(node, :stop, [])
    for {table, master_nodes} <- tables do
      :ok = mnesia_rpc_call(node, :set_master_nodes, [table, master_nodes])
    end
    :ok = mnesia_rpc_call(node, :start, [])
    :ok = mnesia_rpc_call(node, :wait_for_tables, [table_names, Common.cache_wait_time()])
    {:ok, ^node} = mnesia_rpc_call(node, :subscribe, [:system])

    {node, table_names}
  end

  # Find distributed Mnesia tables located on this node.
  defp find_split_tables(node) do
    Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    |> elem(1)
    |> Enum.filter(fn(table) -> Cache.option(table, :distributed) end)
    |> Enum.filter(fn(table) -> node in table_copies(table) end)
  end

  # Return two lists, master nodes and split nodes based on the longest list
  defp find_cluster_split(node, table) do
    table_nodes = table_copies(table)
    first_group = Mnesia.system_info(:running_db_nodes)
    |> Enum.filter(fn(node) -> node in table_nodes end)
    second_group = mnesia_rpc_call(node, :system_info, [:running_db_nodes])
    |> Enum.filter(fn(node) -> node in table_nodes end)

    Enum.sort_by([first_group, second_group], &length/1, :desc)
  end

  # List all nodes which have a copy of the given table.
  defp table_copies(table) do
    [:ram_copies, :disc_copies]
    |> Enum.map(fn(type) -> Mnesia.table_info(table, type) end)
    |> Enum.concat()
  end

  # Log cache manager status.
  defp log_status(timestamp) do
    now = OS.system_time(:milli_seconds)

    case now - timestamp > Common.log_interval() do
      true -> caches = Mnesia.table_info(@caches, :size)
              nodes = Mnesia.system_info(:running_db_nodes)

              Logger.debug(
                "##{caches} deduplication caches running on nodes: #{inspect(nodes)}")

              now
      false -> timestamp
    end
  end

  # Execution Mnesia function on given node.
  defp mnesia_rpc_call(node, function, parameters) do
    ERPC.call(node, Mnesia, function, parameters, Common.cache_wait_time())
  end
end
