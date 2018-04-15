defmodule RabbitMQ.CacheTest do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia

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
                        start: {RabbitMQ.Cache,
                                :start_link,
                                [cache, cache_options]}})
    start_supervised!(%{id: cache_ttl,
                        start: {RabbitMQ.Cache,
                                :start_link,
                                [cache_ttl, cache_ttl_options]}})

    %{cache: cache, cache_ttl: cache_ttl}
  end

  test "basic insertion and lookup", %{cache: cache, cache_ttl: _} do
    assert RabbitMQ.Cache.member?(cache, "foo") == false

    RabbitMQ.Cache.put(cache, "foo")
    assert RabbitMQ.Cache.member?(cache, "foo") == true
  end

  test "TTL at insertion", %{cache: cache, cache_ttl: _} do
    assert RabbitMQ.Cache.member?(cache, "foo") == false

    RabbitMQ.Cache.put(cache, "foo", 1)
    assert RabbitMQ.Cache.member?(cache, "foo") == true

    1 |> Timer.seconds() |> Timer.sleep()

    assert RabbitMQ.Cache.member?(cache, "foo") == false
  end

  test "TTL at table creation", %{cache: _, cache_ttl: cache} do
    assert RabbitMQ.Cache.member?(cache, "foo") == false

    RabbitMQ.Cache.put(cache, "foo")
    assert RabbitMQ.Cache.member?(cache, "foo") == true

    1 |> Timer.seconds() |> Timer.sleep()

    assert RabbitMQ.Cache.member?(cache, "foo") == false
  end

  test "entries are deleted if cache is full", %{cache: cache, cache_ttl: _} do
    assert RabbitMQ.Cache.member?(cache, "foo") == false
    assert RabbitMQ.Cache.member?(cache, "bar") == false

    RabbitMQ.Cache.put(cache, "foo")
    assert RabbitMQ.Cache.member?(cache, "foo") == true
    RabbitMQ.Cache.put(cache, "bar")
    assert RabbitMQ.Cache.member?(cache, "bar") == true

    assert RabbitMQ.Cache.member?(cache, "foo") == false
  end

  test "drop the cache", %{cache: cache, cache_ttl: _} do
    :ok = RabbitMQ.Cache.drop(cache)

    assert Enum.member?(Mnesia.system_info(:tables), cache) == false
  end
end
