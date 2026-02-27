# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2026, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Common do
  @moduledoc """
  Common functions shared between the exchange and the queue
  behaviour implementations.

  """

  require RabbitMQMessageDeduplication.Cache

  alias :mc, as: MC
  alias :timer, as: Timer
  alias :rabbit_nodes, as: RabbitNodes

  @default_arguments %{type: nil, default: nil}

  @doc """
  Retrieve a configuration value from a list of arguments or policies.
  """
  @spec rabbit_argument(List.t, String.t, List.t) :: String.t | nil
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
  @spec message_header(MC.state, String.t) ::
    String.t | integer | float | boolean | :undefined | nil
  def message_header(message, header) do
    case MC.x_header(header, message) do
      {_type, value} when not is_list(value) and not is_tuple(value) ->
	# list and tuple values have type-tagged elements
	# that would need to be untagged recursively
	# we don't expect to use such headers, so those cases are not handled
	value
      :null ->
	# header value in AMQP message was {:void, :undefined}

	# pre-3.13 version of this function used rabbit_keyfind/2
	# which returned :undefined instead of nil or :void. We have to
	# keep this value as this is used in keys to cache the message
	# and is preserved during a rolling upgrade in a replicated
	# Mnesia table
	:undefined
      :undefined -> nil
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

  @doc """
  Retrieve the RabbitMQ cluster nodes list excluding the calling node.
  """
  @spec cluster_nodes() :: List.t
  def cluster_nodes() do
    RabbitNodes.list_running() |> List.delete(node())
  end

  def rabbit_keyfind(list, key, default \\ nil) do
    case List.keyfind(list, key, 0) do
      {_key, _type, value} -> value
      {_key, value} -> value
      _ -> default
    end
  end

  def cache_wait_time() do
    Application.get_env(appname(), :cache_wait_time, Timer.seconds(30))
  end

  def cleanup_period() do
    Application.get_env(appname(), :cache_cleanup_period, Timer.seconds(3))
  end

  defp sanitize_string(string) do
    string
    |> String.replace(~r/[-\. ]/, "_")
    |> String.replace("/", "")
    |> String.downcase()
  end

  defp appname(), do: Application.get_application(__MODULE__)
end
