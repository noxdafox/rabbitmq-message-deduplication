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

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.Common, as: Common

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

  def start_link(rabbit_nodes \\ []) do
    cluster_nodes = case rabbit_nodes do
                    [] -> Common.cluster_nodes()
                    [_ | _] -> rabbit_nodes
                  end

    GenServer.start_link(__MODULE__, cluster_nodes, name: __MODULE__)
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

  @doc """
  Disable the cache and terminate the manager process.
  """
  def disable() do
    {:ok, _node} = Mnesia.unsubscribe(:system)
    :ok = Supervisor.terminate_child(:rabbit_sup, __MODULE__)
    :ok = Supervisor.delete_child(:rabbit_sup, __MODULE__)
  end

  ## Server Callbacks

  # Create the cache table and start the cleanup routine.
  def init(cluster_nodes) do
    with :ok <- start_backend(cluster_nodes),
         :ok <- initialize_schema(),
         {:ok, _} <- Mnesia.subscribe(:system)
    do
      nodes = cluster_nodes ++ [node()] |> Enum.dedup()

      Logger.info("Deduplication caches running on nodes: #{inspect(nodes)}")

      Process.send_after(__MODULE__, :cleanup, Common.cleanup_period())

      {:ok, nil}
    else
      {:timeout, reason} -> {:error, reason}
      error -> error
    end
  end

  # Create the cache and add it to the Mnesia caches table
  def handle_call({:create, cache, options}, _from, state) do
    function = fn -> Mnesia.write({caches(), cache, :nil}) end

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
    function = fn -> Mnesia.delete({caches(), cache}) end

    with :ok <- Cache.drop(cache),
         {:atomic, result} <- Mnesia.transaction(function)
    do
      {:reply, result, state}
    else
      {:aborted, reason} -> {:reply, {:error, reason}, state}
      error -> {:reply, error, state}
    end
  end

  # The maintenance process deletes expired cache entries.
  def handle_info(:cleanup, state) do
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    Enum.each(caches, &Cache.delete_expired_entries/1)
    Process.send_after(__MODULE__, :cleanup, Common.cleanup_period())

    {:noreply, state}
  end

  # On node addition distribute cache tables
  def handle_info({:mnesia_system_event, {:mnesia_up, _node}}, state) do
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    Enum.each(caches, &Cache.rebalance_replicas/1)

    {:noreply, state}
  end

  def handle_info({:mnesia_system_event, _event}, state) do
    {:noreply, state}
  end

  # Run Mnesia functions handling output
  defmacro mnesia_wrap(function) do
    quote do
      case unquote(function) do
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, _}} -> :ok        # table already exists
        {:aborted, {:already_exists, _, _}} -> :ok     # table copy already exists
        {:aborted, {:already_exists, _, _, _}} -> :ok  # schema already exists
        error -> error
      end
    end
  end

  # Start Mnesia DB, connect to cluster nodes and create schema
  defp start_backend(extra_nodes) do
    with :ok <- Mnesia.start(),
         {:ok, nodes} <- Mnesia.change_config(:extra_db_nodes, extra_nodes)
    do
      case nodes do
        [_ | _] ->
          mnesia_wrap(Mnesia.add_table_copy(:schema, node(), :disc_copies))
          Mnesia.wait_for_tables([:schema], Common.cache_wait_time())
        [] ->
          mnesia_wrap(Mnesia.change_table_copy_type(:schema, node(), :disc_copies))
      end
    else
      error -> {:error, error}
    end
  end

  # Create supporting tables
  defp initialize_schema() do
    with :ok <- mnesia_wrap(Mnesia.create_table(caches(), [])),
         :ok <- mnesia_wrap(Mnesia.add_table_copy(caches(), node(), :ram_copies)),
         :ok <- Mnesia.wait_for_tables([caches()], Common.cache_wait_time())
    do
      :ok
    else
      error -> error
    end
  end

  defp caches(), do: :message_deduplication_caches
end
