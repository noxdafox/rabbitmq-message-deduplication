defmodule RabbitMQ.ExchangeTypeMessageDeduplication do
  import Record, only: [defrecord: 2, extract: 2]

  require RabbitMQ.Cache
  require RabbitMQ.Supervisor

  alias :rabbit_misc, as: RabbitMisc
  alias :rabbit_router, as: RabbitRouter
  alias :rabbit_exchange, as: RabbitExchange

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
    if route?(cache_name(name), content) do
      RabbitRouter.match_routing_key(name, [:_])
    else
      []
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
  end

  def validate_binding(_ex, _bs) do
    :ok
  end

  def create(_tx, exchange(name: name, arguments: args)) do
    cache = cache_name(name)
    size = args |> List.keyfind("x-cache-size", 0) |> elem(2)
    options = case List.keyfind(args, "x-cache-ttl", 0) do
      {_h, _t, ttl} -> [cache, size, ttl]
      nil -> [cache, size]
    end
    specifications = %{id: cache,
                       start: {RabbitMQ.Cache,
                               :start_link,
                               options}}

    case RabbitMQ.Supervisor.start_child(specifications) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
    end
  end

  def delete(_tx, exchange(name: name), _bs) do
    cache = cache_name(name)

    {_, :ok} = RabbitMQ.Cache.drop(cache)
    :ok = RabbitMQ.Supervisor.terminate_child(cache)
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

  def info(_x) do
    []
  end

  def info(_x, _y) do
    []
  end

  # Utility functions

  # Whether to route the message or not.
  defp route?(cache, message) do
    case message_headers(message) do
      headers when is_list(headers) -> not cached_message?(cache, headers)
      :undefined -> true
    end
  end

  # Returns true if the message includes the header `x-deduplication-header`
  # and its value is already present in the deduplication cache.
  #
  # false otherwise.
  #
  # If `x-deduplication-header` value is not present in the cache, it is added.
  defp cached_message?(cache, headers) do
    case List.keyfind(headers, "x-deduplication-header", 0) do
      {_h, _t, key} ->
        if RabbitMQ.Cache.member?(cache, key) do
          true
        else
          # Add message to the cache
          case List.keyfind(headers, "x-cache-ttl", 0) do
            {_h, _t, ttl} -> RabbitMQ.Cache.put(cache, key, ttl)
            nil -> RabbitMQ.Cache.put(cache, key)
          end

          false
        end
      nil -> false
    end
  end

  # Unpacks the message headers
  defp message_headers(message) do
    message |> elem(2) |> elem(3)
  end

  # Returns an atom composed by the resource and exchange name.
  defp cache_name({:resource, resource, :exchange, exchange}) do
    String.to_atom("cache_#{resource}_#{exchange}")
  end
end
