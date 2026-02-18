# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2023, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Cache.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache

  setup do
    cache = :test_cache
    cache_ttl = :test_cache_ttl
    cache_simple = :cache_simple

    Mnesia.create_schema([Node.self()])  # Disk tables require persistent schema
    Mnesia.start()

    on_exit fn ->
      Mnesia.delete_table(cache)
      Mnesia.delete_table(cache_ttl)
      Mnesia.delete_table(cache_simple)
    end

    cache_simple_options = [distributed: true,
                            persistence: :memory]
    cache_options = [distributed: true,
                     size: 1,
                     ttl: nil,
                     persistence: :disk]
    cache_ttl_options = [distributed: false,
                         size: 1,
                         ttl: Timer.seconds(1),
                         persistence: :memory]

    :ok = Cache.create(cache, cache_options)
    :ok = Cache.create(cache_ttl, cache_ttl_options)
    :ok = Cache.create(cache_simple, cache_simple_options)

    %{cache: cache, cache_ttl: cache_ttl, cache_simple: cache_simple}
  end

  test "cache creation",
    %{cache: cache, cache_ttl: cache_ttl, cache_simple: _} do
    seconds = Timer.seconds(1)

    :disc_copies = Mnesia.table_info(cache, :storage_type)
    {:size, 1} = Mnesia.read_table_property(cache, :size)
    {:distributed, true} = Mnesia.read_table_property(cache, :distributed)

    :ram_copies = Mnesia.table_info(cache_ttl, :storage_type)
    {:size, 1} = Mnesia.read_table_property(cache_ttl, :size)
    {:ttl, seconds} = Mnesia.read_table_property(cache_ttl, :ttl)
    {:distributed, false} = Mnesia.read_table_property(cache_ttl, :distributed)
  end

  test "basic insertion",
      %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")
    {:ok, :exists} = Cache.insert(cache, "foo")
  end

  test "TTL at insertion",
      %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo", Timer.seconds(1))
    {:ok, :exists} = Cache.insert(cache, "foo")

    1 |> Timer.seconds() |> Timer.sleep()

    :ok = Cache.delete_expired_entries(cache)

    {:ok, :inserted} = Cache.insert(cache, "foo")
  end

  test "TTL at table creation",
      %{cache: _, cache_ttl: cache, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")
    {:ok, :exists} = Cache.insert(cache, "foo")

    1 |> Timer.seconds() |> Timer.sleep()

    :ok = Cache.delete_expired_entries(cache)

    {:ok, :inserted} = Cache.insert(cache, "foo")
  end

  test "entries are deleted after TTL",
      %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo", Timer.seconds(1))
    {:ok, :exists} = Cache.insert(cache, "foo")

    Timer.sleep(1200)

    :ok = Cache.delete_expired_entries(cache)

    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(cache) end)
  end

  test "entries are deleted if cache is full",
      %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")
    {:ok, :exists} = Cache.insert(cache, "foo")
    {:ok, :inserted} = Cache.insert(cache, "bar")
    {:ok, :exists} = Cache.insert(cache, "bar")

    {:ok, :inserted} = Cache.insert(cache, "foo")
  end

  test "cache entry deletion", %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")
    {:ok, :exists} = Cache.insert(cache, "foo")

    Cache.delete(cache, "foo")

    {:ok, :inserted} = Cache.insert(cache, "foo")
  end

  test "cache information",
      %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")

    [entries: 1, bytes: _, nodes: _, size: 1] = Cache.info(cache)
  end

  test "simple cache information",
      %{cache: _, cache_ttl: _, cache_simple: cache_simple} do
    {:ok, :inserted} = Cache.insert(cache_simple, "foo")

    [entries: 1, bytes: _, nodes: _] = Cache.info(cache_simple)
  end

  test "flush the cache", %{cache: cache, cache_ttl: _, cache_simple: _} do
    {:ok, :inserted} = Cache.insert(cache, "foo")
    {:ok, :exists} = Cache.insert(cache, "foo")

    :ok = Cache.flush(cache)

    {:ok, :inserted} = Cache.insert(cache, "foo")
  end

  test "drop the cache", %{cache: cache, cache_ttl: _, cache_simple: _} do
    :ok = Cache.drop(cache)

    assert Enum.member?(Mnesia.system_info(:tables), cache) == false
  end

  test "reconfigure the cache", %{cache: cache, cache_ttl: _, cache_simple: _} do
    :ok = Cache.change_option(cache, :size, 10)

    [entries: _, bytes: _, nodes: _, size: 10] = Cache.info(cache)

    {:error, {:invalid, :wrong_key}} = Cache.change_option(cache, :wrong_key, 10)
  end

  test "reconfigure old cache on creation", %{cache: cache, cache_ttl: _, cache_simple: _} do
    Mnesia.delete_table_property(cache, :distributed)
    Mnesia.delete_table_property(cache, :size)
    Mnesia.delete_table_property(cache, :ttl)

    Mnesia.write_table_property(cache, {:limit, 100})
    Mnesia.write_table_property(cache, {:default_ttl, 100})

    cache_options = [distributed: true, size: 1, ttl: nil, persistence: :disk]

    Cache.create(cache, cache_options)

    {:ttl, 100} = Mnesia.read_table_property(cache, :ttl)
    {:size, 100} = Mnesia.read_table_property(cache, :size)
    {:distributed, true} = Mnesia.read_table_property(cache, :distributed)
  end
end
