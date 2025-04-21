# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
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
  alias :erlang, as: Erlang
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Common, as: Common

  @options [:size, :ttl, :distributed, :limit, :default_ttl]

  @doc """
  Create a new cache with the given name and options.

  A distributed cache is replicated across multiple nodes.

  """
  @spec create(atom, boolean, list) :: :ok | { :error, any }
  def create(cache, distributed, options) do
    Mnesia.start()

    case cache_create(cache, distributed, options) do
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
    with entries when is_integer(entries) <- Mnesia.table_info(cache, :size),
         words when is_integer(words) <- Mnesia.table_info(cache, :memory)
    do
      {_, nodes} = cache_layout(cache)
      bytes = words * Erlang.system_info(:wordsize)

      case cache_property(cache, :size) do
        nil -> [entries: entries, bytes: bytes, nodes: nodes]
        size -> [entries: entries, bytes: bytes, nodes: nodes, size: size]
      end
    else
      :undefined -> []
    end
  end

  @doc """
  Rebalance cache replicas.
  """
  @spec rebalance_replicas(atom) :: any
  def rebalance_replicas(cache) do
    if cache_property(cache, :distributed) do
      cache_rebalance(cache)
    end
  end

  @doc """
  Change cache options.
  """
  @spec change_option(atom, atom, any) :: :ok | { :error, any }
  def change_option(cache, option, value) when option in @options do
    :ok = cache_property(cache, option, value)
  end
  def change_option(_, option, _), do: {:error, {:invalid, option}}

  ## Utility functions

  # Mnesia cache table creation.
  defp cache_create(cache, distributed, options) do
    persistence = case Keyword.get(options, :persistence) do
                    :disk -> :disc_copies
                    :memory -> :ram_copies
                  end
    replicas = if distributed, do: cache_replicas(), else: [Node.self()]
    options = [{:attributes, [:entry, :expiration]},
               {persistence, replicas},
               {:index, [:expiration]},
               {:user_properties, [{:distributed, distributed},
                                   {:size, Keyword.get(options, :size)},
                                   {:ttl, Keyword.get(options, :ttl)}]}]

    case Mnesia.create_table(cache, options) do
      {:atomic, :ok} ->
        wait_for_cache(cache)
      {:aborted, reason} when elem(reason, 0) == :already_exists ->
        maybe_reconfigure(cache, distributed)
      error ->
        error
    end
  end

  # Wait for the table to be loaded and force it in case of timeout
  defp wait_for_cache(cache) do
    case Mnesia.wait_for_tables([cache], Common.cache_wait_time()) do
      {:timeout, [cache]} -> Mnesia.force_load_table(cache)
      result -> result
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
    Mnesia.table_info(cache, :size) >= cache_property(cache, :size)
  end

  # Calculate the expiration given a TTL or the cache default TTL
  defp entry_expiration(cache, ttl) do
    default = cache_property(cache, :ttl)

    cond do
      ttl != nil -> Os.system_time(:millisecond) + ttl
      default != nil -> Os.system_time(:millisecond) + default
      true -> nil
    end
  end

  # Retrieve the given property from the Mnesia user_properties field
  defp cache_property(cache, property) do
    cache |> Mnesia.table_info(:user_properties) |> Keyword.get(property)
  end

  # Set the given Mnesia user_properties field
  defp cache_property(cache, property, value) when property in @options do
    case Mnesia.write_table_property(cache, {property, value}) do
      {:atomic, :ok} -> :ok
      {:aborted, error} -> {:error, error}
    end
  end

  # Rebalance a distributed cache across the cluster nodes
  defp cache_rebalance(cache) do
    {storage_type, cache_nodes} = cache_layout(cache)

    for node <- cache_replicas(cache_nodes) do
      case Mnesia.add_table_copy(cache, node, storage_type) do
        {:atomic, :ok} ->
          wait_for_cache(cache)
        {:aborted, reason} when elem(reason, 0) == :already_exists ->
          maybe_reconfigure(cache, true)
      end
    end
  end

  # List the nodes on which to create the cache replicas.
  # Distributed caches are replicated on two-thirds of the cluster nodes.
  defp cache_replicas(cache_nodes \\ []) do
    cluster_nodes = Mnesia.system_info(:running_db_nodes)
    replica_number = floor((length(cluster_nodes) * 2) / 3)

    Enum.take(cache_nodes ++ (cluster_nodes -- cache_nodes), replica_number)
  end

  # Returns a tuple {persistence, nodes}
  defp cache_layout(cache) do
    case Mnesia.table_info(cache, :ram_copies) do
      [] -> {:disc_copies, Mnesia.table_info(cache, :disc_copies)}
      nodes -> {:ram_copies, nodes}
    end
  end

  # Caches created prior to v0.6.0 need to be reconfigured.
  defp maybe_reconfigure(cache, distributed) do
    if cache_property(cache, :distributed) == nil do
      cache_property(cache, :distributed, distributed)
      cache_property(cache, :size, cache_property(cache, :limit))
      cache_property(cache, :ttl, cache_property(cache, :default_ttl))

      Mnesia.delete_table_property(cache, :limit)
      Mnesia.delete_table_property(cache, :default_ttl)
    end

    wait_for_cache(cache)
  end
end
