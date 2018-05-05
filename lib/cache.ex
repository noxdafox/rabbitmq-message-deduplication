# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.


defmodule RabbitMQ.MessageDeduplicationPlugin.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia.

  Values can be stored within the cache with a given TTL.
  After the TTL expires the values will be transparently removed.

  The cache does not implement a FIFO mechanism due to Mnesia API limitations.
  An FIFO mechanism could be implemented using ordered_sets
  but performance should be evaluated.

  """

  use GenServer

  alias :os, as: Os
  alias :timer, as: Timer
  alias :mnesia, as: Mnesia

  ## Client API

  @doc """
  Create a new cache and start it.
  """
  def start_link(cache, options) do
    GenServer.start_link(__MODULE__, {cache, options}, name: cache)
  end

  @doc """
  Put the given value into the cache.
  """
  def put(cache, value, ttl \\ nil) do
    GenServer.call(cache, {:put, cache, value, ttl})
  end

  @doc """
  Delete the given value from the cache.
  """
  def delete(cache, value) do
    GenServer.call(cache, {:delete, cache, value})
  end

  @doc """
  True if the value is contained within the cache.
  """
  def member?(cache, value) do
    GenServer.call(cache, {:member?, cache, value})
  end

  @doc """
  Flush the cache content.
  """
  def flush(cache) do
    GenServer.call(cache, {:flush, cache})
  end

  @doc """
  Drop the cache with all its content.
  """
  def drop(cache) do
    GenServer.call(cache, {:drop, cache})
  end

  ## Server Callbacks

  # Creates the Mnesia table and starts the janitor process.
  def init({cache, options}) do
    Mnesia.start()

    :ok = cache_create(cache, options)

    Process.send_after(cache, {:cache, cache}, Timer.seconds(3))

    {:ok, %{}}
  end

  # The janitor process deletes expired cache entries.
  def handle_info({:cache, cache}, state) do
    {_, result} = cache_delete_expired(cache)
    if (result == :ok) do
      Process.send_after(cache, {:cache, cache}, Timer.seconds(3))
    end

    {:noreply, state}
  end

  # Puts a new entry in the cache.
  # If the cache is full, remove an element to make space.
  def handle_call({:put, cache, value, ttl}, _from, state) do
    if cache_full?(cache) do
      cache_delete_first(cache)
    end

    Mnesia.transaction(fn ->
      Mnesia.write({cache, value, entry_expiration(cache, ttl)})
    end)

    {:reply, :ok, state}
  end

  # Removes the given entry from the cache.
  def handle_call({:delete, cache, value}, _from, state) do
    Mnesia.transaction(fn ->
      Mnesia.delete({cache, value})
    end)

    {:reply, :ok, state}
  end

  # True if the value is in the cache.
  def handle_call({:member?, cache, value}, _from, state) do
    {:reply, cache_member?(cache, value), state}
  end

  # Flush the Mnesia cache table.
  def handle_call({:flush, cache}, _from, state) do
    case Mnesia.clear_table(cache) do
      {:atomic, :ok} -> {:reply, :ok, state}
      _ -> {:reply, :error, state}
    end
  end

  # Drop the Mnesia cache table.
  def handle_call({:drop, cache}, _from, state) do
    case Mnesia.delete_table(cache) do
      {:atomic, :ok} -> {:reply, :ok, state}
      _ -> {:reply, :error, state}
    end
  end

  ## Utility functions

  # Mnesia cache table creation.
  defp cache_create(cache, options) do
    persistence = case Keyword.get(options, :persistence) do
                    :disk -> :disc_copies
                    :memory -> :ram_copies
                  end
    options = [{:attributes, [:value, :expiration]},
               {persistence, [node()]},
               {:index, [:expiration]},
               {:user_properties, [{:limit, Keyword.get(options, :size)},
                                   {:default_ttl, Keyword.get(options, :ttl)}]}]

    Mnesia.create_table(cache, options)
    Mnesia.add_table_copy(cache, node(), persistence)
    Mnesia.wait_for_tables([cache], Timer.seconds(30))
  end

  # Mnesia cache lookup. The entry is not returned if expired.
  defp cache_member?(cache, value) do
    {:atomic, entries} = Mnesia.transaction(fn -> Mnesia.read(cache, value) end)

    case List.keyfind(entries, value, 1) do
      {_, _, expiration} -> expiration > Os.system_time(:seconds)
      nil -> false
    end
  end

  # Remove all expired entries from the Mnesia cache.
  defp cache_delete_expired(cache) do
    select = fn ->
      Mnesia.select(cache, [{{cache, :"$1", :_, :"$3"},
                             [{:>, Os.system_time(:seconds), :"$3"}],
                             [:"$1"]}])
    end

    case Mnesia.transaction(select) do
      {:atomic, expired} ->
        Mnesia.transaction(
          fn ->
            Enum.each(expired, fn e -> Mnesia.delete({cache, e}) end)
          end)
      {:aborted, {:no_exists, _}} -> {:aborted, :no_cache}
    end
  end

  # Delete the first element from the cache.
  # As the Mnesia Set is not ordered, the first element is random.
  defp cache_delete_first(cache) do
    Mnesia.transaction(fn -> Mnesia.delete({cache, Mnesia.first(cache)}) end)
  end

  defp cache_full?(cache) do
    Mnesia.table_info(cache, :size) >= cache_property(cache, :limit)
  end

  # Calculate the expiration given a TTL or the cache default TTL
  defp entry_expiration(cache, ttl) do
    default = cache_property(cache, :default_ttl)

    cond do
      ttl != nil -> Os.system_time(:seconds) + ttl
      default != nil -> Os.system_time(:seconds) + default
      true -> nil
    end
  end

  defp cache_property(cache, property) do
    {^property, value} =
      cache
      |> Mnesia.table_info(:user_properties)
      |> Enum.find(fn(element) -> match?({^property, _}, element) end)

    value
  end
end
