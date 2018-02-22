defmodule RabbitExchangeTypeMessageDeduplication do
  use DynamicSupervisor

  import Cachex.Spec
  import Record, only: [defrecord: 2, extract: 2]

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

  def start(_type, args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end

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
    case List.keyfind(args, "x-cache-limit", 0) do
      {"x-cache-limit", :long, val} when val > 0 -> :ok
      _ ->
        RabbitMisc.protocol_error(
          :precondition_failed,
          "Missing or invalid argument, \
          'x-cache-limit' must be an integer greater than 0",
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
    options = [limit: args |> List.keyfind("x-cache-limit", 0) |> elem(2)]
    options = case List.keyfind(args, "x-cache-ttl", 0) do
                {_h, _t, ttl} -> exp = expiration(default: :timer.seconds(ttl),
                                                  interval: :timer.seconds(3),
                                                  lazy: true)
                                 [expiration: exp] ++ options
                nil -> options
              end

    specifications = %{id: Cachex,
                       start: {Cachex,
                               :start_link,
                               [cache_name(name), options]}}
    case DynamicSupervisor.start_child(__MODULE__, specifications) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      _ -> :error
    end
  end

  def delete(_tx, exchange(name: name), _bs) do
    DynamicSupervisor.terminate_child(__MODULE__, name)
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

  # Returns an atom composed by the resource and exchange name.
  defp cache_name({:resource, resource, :exchange, exchange}) do
    String.to_atom("#{resource}_#{exchange}")
  end

  # Whether to route the message or not.

  # Returns false if the message includes the header `x-deduplication-header`
  # and its value is already present in the deduplication cache.

  # true otherwise.

  # If `x-deduplication-header` value is not present in the cache it is added.
  defp route?(cache, message) do
    # x-deduplication-header specified but cache miss
    with headers when is_list(headers) <- message |> elem(2) |> elem(3),
         {_h, _t, key} <- List.keyfind(headers, "x-deduplication-header", 0),
         {:ok, false} <- Cachex.exists?(cache, key) do

      # per message cache ttl
      case List.keyfind(headers, "x-cache-ttl", 0) do
        {_h, _t, ttl} -> Cachex.put(cache, key, nil, ttl: ttl)
        nil -> Cachex.put(cache, key, nil)
      end

      true
    else
      {:ok, true} -> false  # cache hit
      {:error, _} -> true   # FIXME: what to do on cache error?
      _ -> true             # no x-deduplication-header
    end
  end
end
