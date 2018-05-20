# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQ.MessageDeduplicationPlugin.Common do
  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  require RabbitMQ.MessageDeduplicationPlugin.Cache

  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: MessageCache

  defrecord :content, extract(
    :content, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :amqqueue, extract(
    :amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")

  @type basic_message :: record(:basic_message)
  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_properties, :P_basic, extract(
    :P_basic, from_lib: "rabbit_common/include/rabbit_framing.hrl")

  @doc """
  Retrieve Cache related information from a list of exchange or queue arguments.
  """
  @spec cache_argument(list, String.t, atom, any) :: String.t | nil
  def cache_argument(arguments, argument, type \\ nil, default \\ nil) do
    case type do
      :number -> case rabbitmq_keyfind(arguments, argument, default) do
                   number when is_bitstring(number) -> String.to_integer(number)
                   integer when is_integer(integer) -> integer
                   ^default -> default
                 end
      :atom -> case rabbitmq_keyfind(arguments, argument, default) do
                 string when is_bitstring(string) -> String.to_atom(string)
                 ^default -> default
               end
      nil -> rabbitmq_keyfind(arguments, argument, default)
    end
  end

  @doc """
  Retrieve the given header from the message.
  """
  @spec message_header(basic_message, String.t) :: String.t | nil
  def message_header(basic_message(content:
        content(properties: properties)), header) do
    case properties do
      basic_properties(headers: headers) when is_list(headers) ->
        rabbitmq_keyfind(headers, header)
      basic_properties(headers: :undefined) -> nil
      :undefined -> nil
    end
  end

  @doc """
  Check if the routed/queued message is a duplicate.

  If not, it adds it to the cache with the corresponding name.
  """
  @spec duplicate?(tuple, basic_message, integer | nil) :: boolean
  def duplicate?(name, message, ttl \\ nil) do
    cache = cache_name(name)

    case message_header(message, "x-deduplication-header") do
      key when not is_nil(key) ->
        case MessageCache.member?(cache, key) do
          false -> MessageCache.put(cache, key, ttl)
                   false
          true -> true
        end
      nil -> false
    end
  end

  @doc """
  Constructs a sanitized cache name from a tuple containing
  the VHost and the exchange/queue name.
  """
  @spec cache_name({:resource, String.t, :exchange | :queue, String.t}) :: atom
  def cache_name({:resource, resource, :exchange, exchange}) do
    resource = sanitize_string(resource)
    exchange = sanitize_string(exchange)

    String.to_atom("cache_exchange_#{resource}_#{exchange}")
  end

  def cache_name({:resource, resource, :queue, queue}) do
    resource = sanitize_string(resource)
    queue = sanitize_string(queue)

    String.to_atom("cache_queue_#{resource}_#{queue}")
  end

  defp rabbitmq_keyfind(list, key, default \\ nil) do
    case List.keyfind(list, key, 0) do
      {_key, _type, value} -> value
      _ -> default
    end
  end

  defp sanitize_string(string) do
    string
    |> String.replace(~r/[-\. ]/, "_")
    |> String.replace("/", "")
    |> String.downcase()
  end
end
