# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager

  # Run Mnesia functions handling output
  defmacro mnesia_wrap(function) do
    quote do
      case unquote(function) do
        :ok -> :ok
        :stopped -> :ok
        {:error, {_, {:already_exists, _}}} -> :ok
        error -> error
      end
    end
  end

  setup do
    start_supervised!(%{id: :cache_manager,
                        start: {CacheManager,
                                :start_link,
                                []}})

    # Since disk tables require persistent schema, we need to re-configure Mnesia
    :ok = mnesia_wrap(Mnesia.stop())
    :ok = mnesia_wrap(Mnesia.create_schema([Node.self()]))
    :ok = mnesia_wrap(Mnesia.start())

    %{}
  end

  test "cache creation", %{} do
    options = [distributed: false, persistence: :disk]

    CacheManager.create(:cache, options)
    :disc_copies = Mnesia.table_info(:cache, :storage_type)
    CacheManager.destroy(:cache)

    options = [distributed: false, persistence: :memory]

    CacheManager.create(:cache, options)
    :ram_copies = Mnesia.table_info(:cache, :storage_type)
    CacheManager.destroy(:cache)
  end

  test "cache deletion", %{} do
    options = [distributed: false, persistence: :memory]

    :ok = CacheManager.create(:cache, options)
    {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
    :ok = CacheManager.destroy(:cache)
    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(caches()) end)
  end

  test "cache cleanup routine", %{} do
    options = [distributed: false, persistence: :memory]

    :ok = CacheManager.create(:cache, options)

    {:ok, :inserted} = Cache.insert(:cache, "foo", 1000)

    Timer.sleep(3200)

    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(:cache) end)
    {:ok, :inserted} = Cache.insert(:cache, "foo")

    :ok = CacheManager.destroy(:cache)
  end

  def caches(), do: :message_deduplication_caches
  def cluster_nodes_stub(), do: []
end
