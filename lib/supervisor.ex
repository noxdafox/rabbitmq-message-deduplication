# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.


defmodule RabbitMQ.MessageDeduplicationPlugin.Supervisor do
  @moduledoc """
  The Cache Supervisor supervises the Cache GenServer Processes.
  """

  use DynamicSupervisor

  Module.register_attribute __MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true

  @rabbit_boot_step {
    __MODULE__,
    [description: "message deduplication plugin cache supervisor",
     mfa: {:rabbit_sup, :start_child, [__MODULE__]},
     requires: :database,
     enables: :external_infrastructure]}

  @doc """
  Start the Supervisor process.
  """
  def start_link() do
    DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__)
  end

  @doc """
  Starts a new cache with the given name and options.
  """
  def start_cache(cache, options) do
    specifications = %{id: cache,
                       start: {RabbitMQ.MessageDeduplicationPlugin.Cache,
                               :start_link, [cache, options]}}

    case DynamicSupervisor.start_child(__MODULE__, specifications) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      _ -> :error
    end
  end

  @doc """
  Stops the given cache.
  """
  def stop_cache(cache) do
    DynamicSupervisor.terminate_child(__MODULE__, Process.whereis(cache))
  end

  @doc """
  Supervisor initialization callback.
  """
  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
