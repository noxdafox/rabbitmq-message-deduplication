# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2020, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager do
  @moduledoc """
  The Cache Manager takes care of creating, maintaining and destroying caches.
  """

  use GenServer

  require RabbitMQMessageDeduplication.Cache

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache

  @caches :message_deduplication_caches
  @cleanup_period Timer.seconds(3)
  @table_wait_time Timer.seconds(30)

  def start_link() do
    GenServer.start_link(__MODULE__, %{}, name: __MODULE__)
  end

  @doc """
  Create the cache and register it within the maintenance process.
  """
  @spec create(atom, list) :: :ok | { :error, any }
  def create(cache, options) do
    try do
      GenServer.call(__MODULE__, {:create, cache, options})
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
    # GenServer.call(__MODULE__, {:disable})
    :ok = Supervisor.terminate_child(:rabbit_sup, __MODULE__)
    :ok = Supervisor.delete_child(:rabbit_sup, __MODULE__)
  end

  ## Server Callbacks

  # Run Mnesia creation functions handling output
  defmacro mnesia_create(function) do
    quote do
      case unquote(function) do
        {:atomic, :ok} -> :ok
        {:aborted, {:already_exists, _}} -> :ok
        {:aborted, {:already_exists, _, _}} -> :ok
        error -> error
      end
    end
  end

  # Create the cache table and start the cleanup routine.
  def init(state) do
    Mnesia.start()

    with :ok <- mnesia_create(Mnesia.create_table(@caches, [])),
         :ok <- mnesia_create(Mnesia.add_table_copy(@caches, node(), :ram_copies)),
         :ok <- Mnesia.wait_for_tables([@caches], @table_wait_time)
    do
      Process.send_after(__MODULE__, :cleanup, @cleanup_period)
      {:ok, state}
    else
      {:timeout, reason} -> {:error, reason}
      error -> error
    end
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

  # The maintenance process deletes expired cache entries.
  def handle_info(:cleanup, state) do
    {:atomic, caches} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    Enum.each(caches, &Cache.delete_expired_entries/1)
    Process.send_after(__MODULE__, :cleanup, @cleanup_period)

    {:noreply, state}
  end
end
