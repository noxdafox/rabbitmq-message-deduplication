# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQ.MessageDeduplicationPlugin.Common do
  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  defrecord :content, extract(
    :content, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :amqqueue, extract(
    :amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_properties, :P_basic, extract(
    :P_basic, from_lib: "rabbit_common/include/rabbit_framing.hrl")

  def cache_argument(arguments, argument, type, default \\ nil) do
    case type do
      :number -> case rabbitmq_keyfind(arguments, argument, default) do
                   number when is_bitstring(number) -> String.to_integer(number)
                   integer when is_integer(integer) -> integer
                   nil -> nil
                 end
      :atom -> case rabbitmq_keyfind(arguments, argument, default) do
                 string when is_bitstring(string) -> String.to_atom(string)
                 nil -> nil
               end
    end
  end

  def message_header(basic_message(content:
        content(properties: properties)), header) do
    case properties do
      basic_properties(headers: headers) when is_list(headers) ->
        rabbitmq_keyfind(headers, header)
      basic_properties(headers: :undefined) -> nil
      :undefined -> nil
    end
  end

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
