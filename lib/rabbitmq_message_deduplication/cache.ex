# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2020, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia.

  Entries can be stored within the cache with a given TTL.
  After the TTL expires the entrys will be transparently removed.

  When the cache is full, a random element is removed to make space to a new one.
  A FIFO approach would be preferrable but impractical by now due to Mnesia limitations.

  """

  alias :os, as: Os
  alias :timer, as: Timer
  alias :erlang, as: Erlang
  alias :mnesia, as: Mnesia

  @table_wait_time Timer.seconds(30)

  @doc """
  Create a new cache with the given name and options.
  """
  @spec create(atom, list) :: :ok | { :error, any }
  def create(cache, options) do
    Mnesia.start()

    case cache_create(cache, options) do
      {_, reason} -> {:error, reason}
      result -> result
    end
  end

  @doc """
  Insert the given entry into the cache if it doesn't exist.
  The TTL controls the lifetime in milliseconds of the entry.

  If the cache is full, an entry will be removed to make space.

  """
  @spec insert(atom, any, integer | nil) ::
    { :ok, :inserted | :exists } | { :error, any }
  def insert(cache, entry, ttl \\ nil) do
    function = fn ->
      if cache_member?(cache, entry) do
        :exists
      else
        if cache_full?(cache) do
          cache_delete_first(cache)
        end

        Mnesia.write({cache, entry, entry_expiration(cache, ttl)})

        :inserted
      end
    end

    case Mnesia.transaction(function) do
      {:atomic, result} -> {:ok, result}
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Delete the given entry from the cache.
  """
  @spec delete(atom, any) :: :ok | { :error, any }
  def delete(cache, entry) do
    case Mnesia.transaction(fn -> Mnesia.delete({cache, entry}) end) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Flush the cache content.
  """
  @spec flush(atom) :: :ok | { :error, any }
  def flush(cache) do
    case Mnesia.clear_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Drop the cache with all its content.
  """
  @spec drop(atom) :: :ok | { :error, any }
  def drop(cache) do
    case Mnesia.delete_table(cache) do
      {:atomic, :ok} -> :ok
      {:aborted, reason} -> {:error, reason}
    end
  end

  @doc """
  Remove all entries which TTL has expired.
  """
  @spec delete_expired_entries(atom) :: :ok | { :error, any }
  def delete_expired_entries(cache) do
    select = fn ->
      Mnesia.select(cache, [{{cache, :"$1", :"$2"},
                             [{:>, Os.system_time(:millisecond), :"$2"}],
                             [:"$1"]}])
    end

    delete = fn x -> Enum.each(x, fn e -> Mnesia.delete({cache, e}) end) end

    case Mnesia.transaction(select) do
      {:atomic, expired} -> case Mnesia.transaction(delete, [expired], 1) do
                              {:atomic, :ok} -> :ok
                              {:aborted, reason} -> {:error, reason}
                            end
      {:aborted, {:no_exists, _}} -> {:error, :no_cache}
    end
  end

  @doc """
  Return information related to the given cache.
  """
  @spec info(atom) :: list
  def info(cache) do
    info = [
      bytes: Mnesia.table_info(cache, :memory) * Erlang.system_info(:wordsize),
      entries: Mnesia.table_info(cache, :size)
    ]

    case cache_property(cache, :limit) do
      number when is_integer(number) -> [size: number] ++ info
      nil -> info
    end
  end

  ## Utility functions

  # Mnesia cache table creation.
  defp cache_create(cache, options) do
    persistence = case Keyword.get(options, :persistence) do
                    :disk -> :disc_copies
                    :memory -> :ram_copies
                  end
    options = [{:attributes, [:entry, :expiration]},
               {persistence, cache_replicas()},
               {:index, [:expiration]},
               {:user_properties, [{:limit, Keyword.get(options, :size)},
                                   {:default_ttl, Keyword.get(options, :ttl)}]}]

    case Mnesia.create_table(cache, options) do
      {:atomic, :ok} ->
        Mnesia.wait_for_tables([cache], @table_wait_time)
      {:aborted, {:already_exists, _}} ->
        Mnesia.wait_for_tables([cache], @table_wait_time)
      error -> error
    end
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

  # List the nodes on which to create the cache replicas.
  # Cache is replicated on two-third of the cluster nodes.
  defp cache_replicas() do
    nodes = [Node.self() | Node.list()]
    nodes |> Enum.split(round((length(nodes) * 2) / 3)) |> elem(0)
  end
end
