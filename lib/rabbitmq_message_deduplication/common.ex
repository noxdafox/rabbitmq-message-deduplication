# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Common do
  @moduledoc """
  Common functions shared between the exchange and the queue
  behaviour implementations.

  """

  import Record, only: [defrecord: 2, defrecord: 3, extract: 2]

  require RabbitMQMessageDeduplication.Cache

  alias :rabbit_binary_parser, as: RabbitBinaryParser
  alias RabbitMQMessageDeduplication.Cache, as: Cache

  defrecord :content, extract(
    :content, from_lib: "rabbit_common/include/rabbit.hrl")

  @type basic_message :: record(:basic_message)
  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_properties, :P_basic, extract(
    :P_basic, from_lib: "rabbit_common/include/rabbit_framing.hrl")

  @default_arguments %{type: nil, default: nil}

  @doc """
  Retrieve a configuration value from a list of exchange or queue arguments.
  """
  @spec rabbit_argument(list, String.t, List.t) :: String.t | nil
  def rabbit_argument(arguments, argument, opts \\ []) do
    %{type: type, default: default} = Enum.into(opts, @default_arguments)

    case type do
      :number -> case rabbit_keyfind(arguments, argument, default) do
                   number when is_bitstring(number) -> String.to_integer(number)
                   integer when is_integer(integer) -> integer
                   ^default -> default
                 end
      :atom -> case rabbit_keyfind(arguments, argument, default) do
                 string when is_bitstring(string) -> String.to_atom(string)
                 ^default -> default
               end
      nil -> rabbit_keyfind(arguments, argument, default)
    end
  end

  @doc """
  Retrieve the given header from the message.
  """
  @spec message_header(basic_message, String.t) :: String.t | nil
  def message_header(basic_message(content: message_content), header) do
    message_content = RabbitBinaryParser.ensure_content_decoded(message_content)

    case content(message_content, :properties) do
      basic_properties(headers: headers) when is_list(headers) ->
        rabbit_keyfind(headers, header)
      basic_properties(headers: :undefined) -> nil
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
      key when not is_nil(key) -> case Cache.insert(cache, key, ttl) do
                                    {:ok, :exists} -> true
                                    {:ok, :inserted} -> false
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

  defp rabbit_keyfind(list, key, default \\ nil) do
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
