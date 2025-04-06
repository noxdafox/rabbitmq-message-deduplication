# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Exchange do
  @moduledoc """
  This module adds support for deduplication exchanges.

  Messages carrying the `x-deduplication-header` header will be deduplicated
  if a message with the same header value was already seen before.

  When a message is routed within the exchange, it's checked against duplicates.
  If no duplicate is found, the message is routed and its deduplication header
  cached. If a TTL was set, the header is removed once expired.
  If the deduplication cache fills up, old elements will be removed
  to make space to new onces.

  This module implements the `rabbit_exchange_type` behaviour.

  """

  import Record, only: [defrecord: 2, extract: 2]

  require RabbitMQMessageDeduplication.Cache
  require RabbitMQMessageDeduplication.Common

  alias :rabbit_log, as: RabbitLog
  alias :rabbit_misc, as: RabbitMisc
  alias :rabbit_policy, as: RabbitPolicy
  alias :rabbit_router, as: RabbitRouter
  alias :rabbit_exchange, as: RabbitExchange
  alias :rabbit_registry, as: RabbitRegistry
  alias RabbitMQMessageDeduplication.Common, as: Common
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager

  @behaviour :rabbit_exchange_type

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @exchange_type <<"x-message-deduplication">>
  @rabbit_boot_step {__MODULE__,
                     [{:description, "exchange type x-message-deduplication"},
                      {:mfa, {__MODULE__, :register, []}},
                      {:requires, :rabbit_registry},
                      {:enables, :kernel_ready}]}

  defrecord :exchange, extract(
    :exchange, from_lib: "rabbit_common/include/rabbit.hrl")

  @doc """
  Register the exchange type within the Broker.
  """
  @spec register() :: :ok
  def register() do
    RabbitRegistry.register(:exchange, @exchange_type, __MODULE__)
    maybe_reconfigure_caches()
  end

  @doc """
  Unregister the exchange type from the Broker.
  """
  @spec unregister() :: :ok
  def unregister() do
    RabbitRegistry.unregister(:exchange, @exchange_type)
  end

  @impl :rabbit_exchange_type
  def description() do
    [
      {:name, @exchange_type},
      {:description, <<"Message Deduplication Exchange.">>}
    ]
  end

  @impl :rabbit_exchange_type
  def serialise_events() do
    false
  end

  @impl :rabbit_exchange_type
  def route(exchange(name: name), msg, _opts) do
    if route?(name, msg) do
      RabbitRouter.match_routing_key(name, [:_])
    else
      []
    end
  end

  @impl :rabbit_exchange_type
  def validate(exchange(arguments: args)) do
    case List.keyfind(args, "x-cache-size", 0) do
      {"x-cache-size", _, val} when is_integer(val) and val > 0 -> :ok
      {"x-cache-size", :longstr, val} ->
        case Integer.parse(val, 10) do
          :error -> RabbitMisc.protocol_error(
                      :precondition_failed,
                      "Missing or invalid argument, \
                      'x-cache-size' must be an integer greater than 0", [])
          _ -> :ok
        end
      _ ->
        RabbitMisc.protocol_error(
          :precondition_failed,
          "Missing or invalid argument, \
          'x-cache-size' must be an integer greater than 0", [])
    end

    case List.keyfind(args, "x-cache-ttl", 0) do
      nil -> :ok
      {"x-cache-ttl", _, val} when is_integer(val) and val > 0 -> :ok
      {"x-cache-ttl", :longstr, val} ->
        case Integer.parse(val, 10) do
          :error -> RabbitMisc.protocol_error(
                      :precondition_failed,
                      "Invalid argument, \
                      'x-cache-ttl' must be an integer greater than 0", [])
          _ -> :ok
        end
      _ -> RabbitMisc.protocol_error(
             :precondition_failed,
             "Invalid argument, \
             'x-cache-ttl' must be an integer greater than 0", [])
    end

    case List.keyfind(args, "x-cache-persistence", 0) do
      nil -> :ok
      {"x-cache-persistence", :longstr, "disk"} -> :ok
      {"x-cache-persistence", :longstr, "memory"} -> :ok
      _ -> RabbitMisc.protocol_error(
             :precondition_failed,
             "Invalid argument, \
             'x-cache-persistence' must be either 'disk' or 'memory'", [])
    end
  end

  @impl :rabbit_exchange_type
  def validate_binding(_ex, _bs) do
    :ok
  end

  @impl :rabbit_exchange_type
  def create(_sr, exchange(name: name, arguments: args)) do
    cache = Common.cache_name(name)
    options = format_options(args)

    RabbitLog.debug(
      "Starting exchange deduplication cache ~s with options ~p~n",
      [cache, options])

    CacheManager.create(cache, true, options)
  end

  @impl :rabbit_exchange_type
  def delete(_sr, exchange(name: name)) do
    name |> Common.cache_name() |> CacheManager.destroy()
  end

  def delete(_tx, exchange, _bs), do: delete(:none, exchange)

  @impl :rabbit_exchange_type
  def policy_changed(_ex, exchange(name: name, arguments: args, policy: :undefined)) do
    cache = Common.cache_name(name)

    RabbitLog.debug(
      "All policies for exchange ~p were deleted, resetting to defaults ~p ~n",
      [name, format_options(args)])

    reset_arguments(cache, args)
  end

  @impl :rabbit_exchange_type
  def policy_changed(_ex, exch = exchange(name: name, arguments: args, policy: policy)) do
    cache = Common.cache_name(name)

    RabbitLog.debug("Applying ~s policy to exchange ~p ~n",
      [RabbitPolicy.name(exch), name])

    # We need to remove old policy before applying new one
    reset_arguments(cache, args)

    for policy_definition <- policy[:definition] do
      case policy_definition do
        {"x-cache-ttl", value} -> Cache.change_option(cache, :ttl, value)
        {"x-cache-size", value} -> Cache.change_option(cache, :size, value)
        {"x-cache-persistence", value} -> Cache.change_option(cache, :persistence, value)
      end
    end
  end

  @impl :rabbit_exchange_type
  def add_binding(_tx, _ex, _bs), do: :ok

  @impl :rabbit_exchange_type
  def remove_bindings(_tx, _ex, _bs), do: :ok

  @impl :rabbit_exchange_type
  def assert_args_equivalence(exch, args) do
    RabbitExchange.assert_args_equivalence(exch, args)
  end

  @impl :rabbit_exchange_type
  def info(exchange) do
    info(exchange, [:cache_info])
  end

  @impl :rabbit_exchange_type
  def info(exchange(name: name), [:cache_info]) do
    [cache_info: name |> Common.cache_name() |> Cache.info()]
  end

  @impl :rabbit_exchange_type
  def info(_ex, _it) do
    []
  end

  # Utility functions

  # Whether to route the message or not.
  defp route?(exchange_name, message) do
    ttl = Common.message_header(message, "x-cache-ttl")
    not Common.duplicate?(exchange_name, message, ttl)
  end

  # Format arguments into options
  defp format_options(args) do
    [size: Common.rabbit_argument(
        args, "x-cache-size", type: :number),
     ttl: Common.rabbit_argument(
       args, "x-cache-ttl", type: :number),
     persistence: Common.rabbit_argument(
       args, "x-cache-persistence", type: :atom, default: "memory")]
  end

  # Reconfigure cache to default arguments
  defp reset_arguments(cache, args) do
    for {key, value} <- format_options(args) do
      Cache.change_option(cache, key, value)
    end
  end

  # Caches created prior to v0.6.0 need to be reconfigured.
  defp maybe_reconfigure_caches() do
    RabbitLog.debug("Deduplication Exchanges startup, reconfiguring old caches")

    RabbitExchange.list()
    |> Enum.filter(fn(exchange(name: type)) -> type == @exchange_type end)
    |> Enum.map(fn(exchange) -> create(:none, exchange) end)

    :ok
  end
end
