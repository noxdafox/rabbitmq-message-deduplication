# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2020, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.CacheManager.Test do
  use ExUnit.Case

  alias :timer, as: Timer
  alias :mnesia, as: Mnesia
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager

  @caches :message_deduplication_caches

  setup do
    start_supervised!(%{id: :cache_manager,
                        start: {CacheManager,
                                :start_link,
                                []}})

    %{}
  end

  test "cache creation", %{} do
    options = [persistence: :memory]

    CacheManager.create(:cache, options)
    {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    CacheManager.destroy(:cache)
  end

  test "cache deletion", %{} do
    options = [persistence: :memory]

    :ok = CacheManager.create(:cache, options)
    {:atomic, [:cache]} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
    :ok = CacheManager.destroy(:cache)
    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(@caches) end)
  end

  test "cache cleanup routine", %{} do
    options = [persistence: :memory]

    :ok = CacheManager.create(:cache, options)

    {:ok, :inserted} = Cache.insert(:cache, "foo", 1000)

    Timer.sleep(3200)

    {:atomic, []} = Mnesia.transaction(fn -> Mnesia.all_keys(:cache) end)

    :ok = CacheManager.destroy(:cache)
  end

end
