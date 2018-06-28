# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.


defmodule RabbitMQ.MessageDeduplicationPlugin.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia.

  Entrys can be stored within the cache with a given TTL.
  After the TTL expires the entrys will be transparently removed.

  The cache does not implement a FIFO mechanism due to Mnesia API limitations.
  An FIFO mechanism could be implemented using ordered_sets
  but performance should be evaluated.

  """

  use GenServer

  alias :os, as: Os
  alias :timer, as: Timer
  alias :erlang, as: Erlang
  alias :mnesia, as: Mnesia

  ## Client API

  @doc """
  Create a new cache and start it.
  """
  @spec start_link(atom, list) :: :ok | { :error, any }
  def start_link(cache, options) do
    GenServer.start_link(__MODULE__, {cache, options}, name: cache)
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.
  """
  @spec insert(atom, any, integer | nil) ::
    :ok | { :error, :already_exists | any }
  def insert(cache, entry, ttl \\ nil) do
    GenServer.call(cache, {:insert, cache, entry, ttl})
  end

  @doc """
  Delete the given entry from the cache.
  """
  @spec delete(atom, any) :: :ok | { :error, any }
  def delete(cache, entry) do
    GenServer.call(cache, {:delete, cache, entry})
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | { :error, any }
  def flush(cache) do
    GenServer.call(cache, {:flush, cache})
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | { :error, any }
  def drop(cache) do
    GenServer.call(cache, {:drop, cache})
  end

  @doc """
  Return information related to the given cache.
  """
  @spec drop(atom) :: list
  def info(cache) do
    GenServer.call(cache, {:info, cache})
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

  # Inserts the entry if it doesn't exist.
  # If the cache is full, it removes an element to make space.
  def handle_call({:insert, cache, entry, ttl}, _from, state) do
    function = fn ->
      if cache_member?(cache, entry) do
        {:error, :already_exists}
      else
        if cache_full?(cache) do
          cache_delete_first(cache)
        end

        Mnesia.write({cache, entry, entry_expiration(cache, ttl)})
      end
    end

    case Mnesia.transaction(function) do
      {:atomic, retval} -> {:reply, retval, state}
      {:aborted, reason} -> {:reply, {:error, reason}, state}
    end
  end

  # Removes the given entry from the cache.
  def handle_call({:delete, cache, entry}, _from, state) do
    Mnesia.transaction(fn ->
      Mnesia.delete({cache, entry})
    end)

    {:reply, :ok, state}
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

  # Return cache information: number of elements and max size
  def handle_call({:info, cache}, _from, state) do
    info = [
      bytes: Mnesia.table_info(cache, :memory) * Erlang.system_info(:wordsize),
      entries: Mnesia.table_info(cache, :size)
    ]
    info = case cache_property(cache, :limit) do
             number when is_integer(number) -> [size: number] ++ info
             nil -> info
           end

    {:reply, info, state}
  end

  ## Utility functions

  # Mnesia cache table creation.
  defp cache_create(cache, options) do
    persistence = case Keyword.get(options, :persistence) do
                    :disk -> :disc_copies
                    :memory -> :ram_copies
                  end
    options = [{:attributes, [:entry, :expiration]},
               {persistence, [node()]},
               {:index, [:expiration]},
               {:user_properties, [{:limit, Keyword.get(options, :size)},
                                   {:default_ttl, Keyword.get(options, :ttl)}]}]

    Mnesia.create_table(cache, options)
    Mnesia.add_table_copy(cache, node(), persistence)
    Mnesia.wait_for_tables([cache], Timer.seconds(30))
  end

  # Lookup the entry within the cache, deletes the entry if expired
  # Must be included within transaction.
  defp cache_member?(cache, entry) do
    case cache |> Mnesia.read(entry) |> List.keyfind(entry, 1) do
      {_, _, expiration} -> if expiration <= Os.system_time(:millisecond) do
                              Mnesia.delete({cache, entry})
                              false
                            else
                              true
                            end
      nil -> false
    end
  end

  # Remove all expired entries from the Mnesia cache.
  defp cache_delete_expired(cache) do
    select = fn ->
      Mnesia.select(cache, [{{cache, :"$1", :"$2"},
                             [{:>, Os.system_time(:millisecond), :"$2"}],
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
  # Must be included within transaction.
  defp cache_delete_first(cache) do
    Mnesia.delete({cache, Mnesia.first(cache)})
  end

  # True if the cache is full, false otherwise.
  defp cache_full?(cache) do
    Mnesia.table_info(cache, :size) >= cache_property(cache, :limit)
  end

  # Calculate the expiration given a TTL or the cache default TTL
  defp entry_expiration(cache, ttl) do
    default = cache_property(cache, :default_ttl)

    cond do
      ttl != nil -> Os.system_time(:millisecond) + ttl
      default != nil -> Os.system_time(:millisecond) + default
      true -> nil
    end
  end

  # Retrieve the given property from the Mnesia user_properties field
  defp cache_property(cache, property) do
    {^property, entry} =
      cache
      |> Mnesia.table_info(:user_properties)
      |> Enum.find(fn(element) -> match?({^property, _}, element) end)

    entry
  end
end
