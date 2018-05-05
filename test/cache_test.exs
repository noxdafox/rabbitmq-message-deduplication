defmodule RabbitMQ.MessageDeduplicationPlugin.Cache.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: MessageCache

  setup do
    cache = :test_cache
    cache_ttl = :test_cache_ttl

    on_exit fn ->
      Mnesia.delete_table(cache)
      Mnesia.delete_table(cache_ttl)
    end

    cache_options = [size: 1, ttl: nil, persistence: :memory]
    cache_ttl_options = [size: 1, ttl: 1, persistence: :memory]

    start_supervised!(%{id: cache,
                        start: {MessageCache,
                                :start_link,
                                [cache, cache_options]}})
    start_supervised!(%{id: cache_ttl,
                        start: {MessageCache,
                                :start_link,
                                [cache_ttl, cache_ttl_options]}})

    %{cache: cache, cache_ttl: cache_ttl}
  end

  test "basic insertion and lookup", %{cache: cache, cache_ttl: _} do
    assert MessageCache.member?(cache, "foo") == false

    MessageCache.put(cache, "foo")
    assert MessageCache.member?(cache, "foo") == true
  end

  test "TTL at insertion", %{cache: cache, cache_ttl: _} do
    assert MessageCache.member?(cache, "foo") == false

    MessageCache.put(cache, "foo", 1)
    assert MessageCache.member?(cache, "foo") == true

    1 |> Timer.seconds() |> Timer.sleep()

    assert MessageCache.member?(cache, "foo") == false
  end

  test "TTL at table creation", %{cache: _, cache_ttl: cache} do
    assert MessageCache.member?(cache, "foo") == false

    MessageCache.put(cache, "foo")
    assert MessageCache.member?(cache, "foo") == true

    1 |> Timer.seconds() |> Timer.sleep()

    assert MessageCache.member?(cache, "foo") == false
  end

  test "entries are deleted if cache is full", %{cache: cache, cache_ttl: _} do
    assert MessageCache.member?(cache, "foo") == false
    assert MessageCache.member?(cache, "bar") == false

    MessageCache.put(cache, "foo")
    assert MessageCache.member?(cache, "foo") == true
    MessageCache.put(cache, "bar")
    assert MessageCache.member?(cache, "bar") == true

    assert MessageCache.member?(cache, "foo") == false
  end

  test "cache entry deletion", %{cache: cache, cache_ttl: _} do
    MessageCache.put(cache, "foo")
    assert MessageCache.member?(cache, "foo") == true

    MessageCache.delete(cache, "foo")

    assert MessageCache.member?(cache, "foo") == false
  end

  test "cache information", %{cache: cache, cache_ttl: _} do
    MessageCache.put(cache, "foo")

    assert MessageCache.info(cache) == [size: 1, entries: 1]
  end

  test "flush the cache", %{cache: cache, cache_ttl: _} do
    MessageCache.put(cache, "foo")
    assert MessageCache.member?(cache, "foo") == true

    :ok = MessageCache.flush(cache)

    assert MessageCache.member?(cache, "foo") == false
  end

  test "drop the cache", %{cache: cache, cache_ttl: _} do
    :ok = MessageCache.drop(cache)

    assert Enum.member?(Mnesia.system_info(:tables), cache) == false
  end
end
