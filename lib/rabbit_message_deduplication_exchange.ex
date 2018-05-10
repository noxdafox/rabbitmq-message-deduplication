# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.


defmodule RabbitMQ.MessageDeduplicationPlugin.Exchange do
  import Record, only: [defrecord: 2, extract: 2]

  require RabbitMQ.MessageDeduplicationPlugin.Cache
  require RabbitMQ.MessageDeduplicationPlugin.Supervisor

  alias :rabbit_log, as: RabbitLog
  alias :rabbit_misc, as: RabbitMisc
  alias :rabbit_router, as: RabbitRouter
  alias :rabbit_exchange, as: RabbitExchange
  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: MessageCache
  alias RabbitMQ.MessageDeduplicationPlugin.Supervisor, as: CacheSupervisor

  @behaviour :rabbit_exchange_type

  Module.register_attribute __MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true

  @rabbit_boot_step {__MODULE__,
                     [{:description, "exchange type x-message-deduplication"},
                      {:mfa, {:rabbit_registry, :register,
                              [:exchange, <<"x-message-deduplication">>,
                               __MODULE__]}},
                      {:requires, :rabbit_registry},
                      {:enables, :kernel_ready}]}

  defrecord :exchange, extract(
    :exchange, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :delivery, extract(
    :delivery, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :binding, extract(
    :binding, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  def description() do
    [
      {:name, <<"x-message-deduplication">>},
      {:description, <<"Message Deduplication Exchange.">>}
    ]
  end

  def serialise_events() do
    false
  end

  def route(exchange(name: name),
      delivery(message: basic_message(content: content))) do
    case route?(cache_name(name), content) do
      true -> RabbitRouter.match_routing_key(name, [:_])
      false -> []
    end
  end

  def validate(exchange(arguments: args)) do
    case List.keyfind(args, "x-cache-size", 0) do
      {"x-cache-size", :long, val} when val > 0 -> :ok
      _ ->
        RabbitMisc.protocol_error(
          :precondition_failed,
          "Missing or invalid argument, \
          'x-cache-size' must be an integer greater than 0",
          [])
    end

    case List.keyfind(args, "x-cache-ttl", 0) do
      nil -> :ok
      {"x-cache-ttl", :long, val} when val > 0 -> :ok
      _ -> RabbitMisc.protocol_error(
             :precondition_failed,
             "Invalid argument, \
             'x-cache-ttl' must be an integer greater than 0",
             [])
    end

    case List.keyfind(args, "x-cache-persistence", 0) do
      nil -> :ok
      {"x-cache-persistence", :longstr, "disk"} -> :ok
      {"x-cache-persistence", :longstr, "memory"} -> :ok
      _ -> RabbitMisc.protocol_error(
             :precondition_failed,
             "Invalid argument, \
             'x-cache-persistence' must be either 'disk' or 'memory'",
             [])
    end
  end

  def validate_binding(_ex, _bs) do
    :ok
  end

  def create(:transaction, exchange(name: name, arguments: args)) do
    cache = cache_name(name)
    ttl = rabbitmq_keyfind(args, "x-cache-ttl")
    size = rabbitmq_keyfind(args, "x-cache-size")
    persistence =
      args
      |> rabbitmq_keyfind("x-cache-persistence", "memory")
      |> String.to_atom()
    options = [size: size, ttl: ttl, persistence: persistence]

    RabbitLog.debug("Starting exchange deduplication cache ~s~n", [cache])

    CacheSupervisor.start_cache(cache, options)
  end

  def create(:none, _ex) do
    :ok
  end

  def delete(:transaction, exchange(name: name), _bs) do
    cache = cache_name(name)

    :ok = MessageCache.drop(cache)

    CacheSupervisor.stop_cache(cache)
  end

  def delete(:none, _ex, _bs) do
    :ok
  end

  def policy_changed(_x1, _x2) do
    :ok
  end

  def add_binding(_tx, _ex, _bs) do
    :ok
  end

  def remove_bindings(_tx, _ex, _bs) do
    :ok
  end

  def assert_args_equivalence(exchange, args) do
    RabbitExchange.assert_args_equivalence(exchange, args)
  end

  def info(exchange) do
    info(exchange, [:cache_info])
  end

  def info(exchange(name: name), [:cache_info]) do
    [cache_info: name |> cache_name() |> MessageCache.info()]
  end

  def info(_ex, _it) do
    []
  end

  # Utility functions

  # Whether to route the message or not.
  defp route?(cache, message) do
    case message_headers(message) do
      headers when is_list(headers) -> not cached?(cache, headers)
      :undefined -> true
    end
  end

  # Returns true if the message includes the header `x-deduplication-header`
  # and its value is already present in the deduplication cache.
  #
  # false otherwise.
  #
  # If `x-deduplication-header` value is not present in the cache, it is added.
  defp cached?(cache, headers) do
    case rabbitmq_keyfind(headers, "x-deduplication-header") do
      nil -> false
      key -> case MessageCache.member?(cache, key) do
               true -> true
               false -> cache_put(cache, key, headers)
                        false
             end
    end
  end

  # Puts the key and related headers in the cache
  defp cache_put(cache, key, headers) do
    case rabbitmq_keyfind(headers, "x-cache-ttl") do
      nil -> MessageCache.put(cache, key)
      ttl -> MessageCache.put(cache, key, ttl)
    end
  end

  # Returns a sanitized atom composed by the resource and exchange name
  defp cache_name({:resource, resource, :exchange, exchange}) do
    resource = sanitize_string(resource)
    exchange = sanitize_string(exchange)

    String.to_atom("cache_exchange_#{resource}_#{exchange}")
  end

  # Returns the value given a key from a RabbitMQ list [{"key", :type, value}]
  defp rabbitmq_keyfind(list, key, default \\ nil) do
    case List.keyfind(list, key, 0) do
      {_key, _type, value} -> value
      _ -> default
    end
  end

  # Unpacks the message headers
  defp message_headers(message) do
    message |> elem(2) |> elem(3)
  end

  defp sanitize_string(string) do
    string
    |> String.replace(~r/[-\. ]/, "_")
    |> String.replace("/", "")
    |> String.downcase()
  end
end
