defmodule RabbitMQ.Cache do
  @moduledoc """
  Simple cache implemented on top of Mnesia.

  Values can be stored within the cache with a given TTL.
  After the TTL expires the values will be transparently removed.

  The cache does not implement a LRU mechanism due to Mnesia API limitations.
  An LRU mechanism could be implemented using ordered_sets
  but performance should be evaluated.

  """

  use GenServer

  require Record

  alias :os, as: Os
  alias :mnesia, as: Mnesia

  ## Client API

  @doc """
  Create a new cache and start it.
  """
  def start_link(cache, size, ttl \\ nil) do
    GenServer.start_link(__MODULE__, {cache, size, ttl}, name: cache)
  end

  @doc """
  Put the given value into the cache.
  """
  def put(cache, value, ttl \\ nil) do
    GenServer.call(cache, {:put, cache, value, ttl})
  end

  @doc """
  True if the value is contained within the cache.
  """
  def member?(cache, value) do
    GenServer.call(cache, {:member?, cache, value})
  end

  @doc """
  Drop the cache with all its content.
  """
  def drop(cache) do
    GenServer.call(cache, {:drop, cache})
  end

  ## Server Callbacks

  def init({cache, size, ttl}) do
    Mnesia.start()

    cache_create(cache, size, ttl)

    Process.send_after(cache, {:cache, cache}, 3)

    {:ok, %{}}
  end

  def handle_info({:cache, cache}, state) do
    {_, result} = cache_delete_expired(cache)
    if (result == :ok) do
      Process.send_after(cache, {:cache, cache}, 3)
    end

    {:noreply, state}
  end

  def handle_call({:put, cache, value, ttl}, _from, state) do
    {:default_ttl, default_ttl} = cache_default_ttl(cache)
    expiration = cond do
      ttl != nil -> Os.system_time(:seconds) + ttl
      default_ttl != nil -> Os.system_time(:seconds) + default_ttl
      true -> nil
    end

    Mnesia.transaction(fn ->
      Mnesia.write({cache, value, expiration})
    end)

    size = Mnesia.table_info(cache, :size)
    {:limit, limit} = cache_limit(cache)
    if size > limit do
      cache_delete_first(cache)
    end

    {:reply, :ok, state}
  end

  def handle_call({:member?, cache, value}, _from, state) do
    {:reply, cache_member?(cache, value), state}
  end

  def handle_call({:drop, cache}, _from, state) do
    {:reply, Mnesia.delete_table(cache), state}
  end

  ## Utility functions

  defp cache_create(cache, size, ttl) do
    with {:atomic, :ok} <- Mnesia.create_table(cache,
              [attributes: [:value, :expiration],
               index: [:expiration],
               user_properties: [limit: size, default_ttl: ttl]]),
         {:atomic, :ok} <- Mnesia.add_table_copy(cache, node(), :ram_copies),
         {:atomic, :ok} <- Mnesia.wait_for_tables([cache], :timer.seconds(30)),
      do: :ok
  end

  defp cache_member?(cache, value) do
    {:atomic, entries} = Mnesia.transaction(fn -> Mnesia.read(cache, value) end)

    case List.keyfind(entries, value, 1) do
      {_, _, expiration} -> expiration > Os.system_time(:seconds)
      nil -> false
    end
  end

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

  defp cache_delete_first(cache) do
    Mnesia.transaction(
      fn ->
        Mnesia.delete({cache, Mnesia.first(cache)})
      end)
  end

  defp cache_limit(cache) do
    Enum.find(Mnesia.table_info(cache, :user_properties),
      fn(element) -> match?({:limit, _}, element) end)
  end

  defp cache_default_ttl(cache) do
    Enum.find(Mnesia.table_info(cache, :user_properties),
      fn(element) -> match?({:default_ttl, _}, element) end)
  end
end
