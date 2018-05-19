# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQ.MessageDeduplicationPlugin.Queue do
  import Record, only: [defrecord: 2, defrecord: 3, defrecordp: 2, extract: 2]

  require RabbitMQ.MessageDeduplicationPlugin.Cache
  require RabbitMQ.MessageDeduplicationPlugin.Common
  require RabbitMQ.MessageDeduplicationPlugin.Supervisor

  alias :rabbit_log, as: RabbitLog
  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: MessageCache
  alias RabbitMQ.MessageDeduplicationPlugin.Common, as: Common
  alias RabbitMQ.MessageDeduplicationPlugin.Supervisor, as: CacheSupervisor

  @behaviour :rabbit_backing_queue

  Module.register_attribute __MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true

  @rabbit_boot_step {__MODULE__,
                     [{:description,
                       "message deduplication queue: supervisor"},
                      {:mfa, {__MODULE__, :enable_deduplication_queues, []}},
                      {:requires, :database},
                      {:enables, :external_infrastructure}]}

  defrecord :content, extract(
    :content, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :amqqueue, extract(
    :amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_properties, :P_basic, extract(
    :P_basic, from_lib: "rabbit_common/include/rabbit_framing.hrl")

  defrecordp :dqack, [:tag, :header]
  defrecordp :dqstate, [:queue, :queue_state]

  defmacrop passthrough(do: function) do
    quote do
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      backing_queue.unquote(function)
    end
  end

  defmacrop passthrough1(state, do: function) do
    quote do
      queue = dqstate(unquote(state), :queue)
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      queue_state = backing_queue.unquote(function)
      dqstate(queue: queue, queue_state: queue_state)
    end
  end

  defmacrop passthrough2(state, do: function) do
    quote do
      queue = dqstate(unquote(state), :queue)
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      {result, queue_state} = backing_queue.unquote(function)
      {result, dqstate(queue: queue, queue_state: queue_state)}
    end
  end

  defmacrop passthrough3(state, do: function) do
    quote do
      queue = dqstate(unquote(state), :queue)
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      {result1, result2, queue_state} = backing_queue.unquote(function)
      {result1, result2, dqstate(queue: queue, queue_state: queue_state)}
    end
  end

  def enable_deduplication_queues() do
    case Application.get_env(:rabbit, :backing_queue_module) do
      __MODULE__ -> :ok
      backing_queue ->
        RabbitLog.info(
          "Deduplication queues enabled, real BQ is ~s~n", [backing_queue])
        :application.set_env(__MODULE__, :backing_queue_module, backing_queue)
        :application.set_env(:rabbit, :backing_queue_module, __MODULE__)
    end
  end

  def start(vhost, queues) do
    passthrough do: start(vhost, queues)
  end

  def stop(vhost) do
    passthrough do: stop(vhost)
  end

  def init(queue = amqqueue(name: name, arguments: args), recovery, callback) do
    if duplicate?(queue) do
      cache = Common.cache_name(name)
      options = [ttl: Common.cache_argument(args, "x-message-ttl", :number),
                 persistence: Common.cache_argument(
                   args, "x-cache-persistence", :atom, "memory")]

      RabbitLog.debug(
        "Starting queue deduplication cache ~s with options ~p~n",
        [cache, options])

      CacheSupervisor.start_cache(cache, options)
    end

    passthrough1(dqstate(queue: queue)) do
      init(queue, recovery, callback)
    end
  end

  def terminate(any, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: terminate(any, qs))
  end

  def delete_and_terminate(
      any, state = dqstate(queue: queue, queue_state: qs)) do
    if duplicate?(queue) do
      cache = queue |> amqqueue(:name) |> Common.cache_name()

      :ok = MessageCache.drop(cache)
      :ok = CacheSupervisor.stop_cache(cache)
    end

    passthrough1(state) do
      delete_and_terminate(any, qs)
    end
  end

  def delete_crashed(queue) do
    passthrough do: delete_crashed(queue)
  end

  def purge(state = dqstate(queue: queue, queue_state: qs)) do
    if duplicate?(queue) do
      cache = queue |> amqqueue(:name) |> Common.cache_name()

      :ok = MessageCache.flush(cache)
    end

    passthrough2(state, do: purge(qs))
  end

  def purge_acks(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: purge_acks(qs))
  end

  def publish(message, properties, boolean, pid, flow,
              state = dqstate(queue_state: qs)) do
    passthrough1(state) do
      publish(message, properties, boolean, pid, flow, qs)
    end
  end

  def batch_publish([publish], pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: batch_publish([publish], pid, flow, qs))
  end

  def publish_delivered(basic_message, message_properties, pid, flow,
                        state = dqstate(queue_state: qs)) do
    passthrough2(state) do
      publish_delivered(basic_message, message_properties, pid, flow, qs)
    end
  end

  def batch_publish_delivered([delivered_publish], pid, flow,
                              state = dqstate(queue_state: qs)) do
    passthrough2(state) do
      batch_publish_delivered([delivered_publish], pid, flow, qs)
    end
  end

  def discard(msg_id, pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: discard(msg_id, pid, flow, qs))
  end

  def drain_confirmed(state = dqstate(queue_state: qs)) do
    passthrough2(state, do: drain_confirmed(qs))
  end

  # The dropwhile callback handles message TTL expiration.
  # The duplicates cache TTL mechanism is used instead.
  def dropwhile(msg_pred, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: dropwhile(msg_pred, qs))
  end

  # The fetchwhile callback handles message TTL dead lettering.
  # The duplicates cache TTL mechanism is used instead.
  def fetchwhile(msg_pred, msg_fun, A, state = dqstate(queue_state: qs)) do
    passthrough3(state, do: fetchwhile(msg_pred, msg_fun, A, qs))
  end

  def fetch(need_ack, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: fetch(need_ack, qs)) do
      {:empty, state} -> {:empty, state}
      {{message, delivery, ack_tag}, state} ->
        case duplicate?(queue) do
          false -> {{message, delivery, dqack(tag: ack_tag)}, state}
          true ->
            case need_ack do
              true ->
                head = Common.message_header(message, "x-deduplication-header")
                ack = dqack(tag: ack_tag, header: head)
                {{message, delivery, ack}, state}
              false ->
                maybe_delete_cache_entry(queue, message)

                {{message, delivery, dqack(tag: ack_tag)}, state}
            end
        end
    end
  end

  # TODO: this is a bit of a hack.
  # As the drop callback returns only the message id, we can't retrieve
  # the message deduplication header. As a workaround fetch is used.
  # This assumes the backing queue drop and fetch behaviours are the same.
  # A better solution would be to store the message IDs in a dedicated index.
  def drop(need_ack, state = dqstate(queue: queue, queue_state: qs)) do
    case duplicate?(queue) do
      false -> passthrough2(state, do: drop(need_ack, qs))
      true ->
        case fetch(need_ack, state) do
          {:empty, state} -> {:empty, state}
          {{message = basic_message(id: id), _, ack_tag}, state} ->
            maybe_delete_cache_entry(queue, message)

            {{id, ack_tag}, state}
        end
    end
  end

  def ack(acks, state = dqstate(queue: queue, queue_state: qs)) do
    acks =
      case duplicate?(queue) do
        false -> Enum.map(acks, fn(dqack(tag: ack)) -> ack end)
        true ->
          Enum.map(acks, fn(dqack(tag: ack, header: header)) ->
            maybe_delete_cache_entry(queue, header)
            ack
          end)
      end

    passthrough2(state, do: ack(acks, qs))
  end

  def requeue([ack], state = dqstate(queue_state: qs)) do
    passthrough2(state, do: requeue([ack], qs))
  end

  def ackfold(function, A, state = dqstate(queue_state: qs), [ack]) do
    passthrough2(state, do: ackfold(function, A, qs, [ack]))
  end

  def fold(function, A, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: fold(function, A, qs))
  end

  def len(dqstate(queue_state: qs)) do
    passthrough do: len(qs)
  end

  def is_empty(dqstate(queue_state: qs)) do
    passthrough do: is_empty(qs)
  end

  def depth(dqstate(queue_state: qs)) do
    passthrough do: depth(qs)
  end

  def set_ram_duration_target(duration, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_ram_duration_target(duration, qs))
  end

  def ram_duration(state = dqstate(queue_state: qs)) do
    passthrough2(state, do: ram_duration(qs))
  end

  def needs_timeout(dqstate(queue_state: qs)) do
    passthrough do: needs_timeout(qs)
  end

  def timeout(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: timeout(qs))
  end

  def handle_pre_hibernate(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: handle_pre_hibernate(qs))
  end

  def resume(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: resume(qs))
  end

  def msg_rates(dqstate(queue_state: qs)) do
    passthrough do: msg_rates(qs)
  end

  def info(atom, dqstate(queue_state: qs)) do
    passthrough do: info(atom, qs)
  end

  def invoke(atom, function, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: invoke(atom, function, qs))
  end

  def is_duplicate(message, state = dqstate(queue: queue, queue_state: qs)) do
    duplicate = duplicate?(queue, message)

    case passthrough2(state, do: is_duplicate(message, qs)) do
      {true, state} -> {true, state}
      {false, state} -> {duplicate, state}
    end
  end

  def set_queue_mode(queue_mode, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(queue_mode, qs))
  end

  def zip_msgs_and_acks(delivered_publish, [ack],
                        A, dqstate(queue_state: qs)) do
    passthrough do: info(delivered_publish, [ack], A, qs)
  end

  def handle_info(term, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(term, qs))
  end

  # Utility functions

  # Returns true if the queue supports message deduplication
  defp duplicate?(amqqueue(arguments: args)) do
    Common.cache_argument(args, "x-message-deduplication", nil, false)
  end

  # Returns true if the queue supports message deduplication
  # and the message is a duplicate.
  defp duplicate?(queue = amqqueue(name: name),
      message = basic_message(content: content(properties: properties))) do
    ttl = case properties do
            basic_properties(expiration: ttl) when is_bitstring(ttl) ->
              String.to_integer(ttl)
            basic_properties(expiration: :undefined) -> nil
            :undefined -> nil
          end

    case duplicate?(queue) do
      true -> Common.duplicate?(name, message, ttl)
      false -> false
    end
  end

  # Remove the message deduplication header from the cache
  def maybe_delete_cache_entry(queue = amqqueue(), message = basic_message()) do
    header = Common.message_header(message, "x-deduplication-header")

    maybe_delete_cache_entry(queue, header)
  end

  def maybe_delete_cache_entry(queue, header) when is_bitstring(header) do
    queue
    |> amqqueue(:name)
    |> Common.cache_name()
    |> MessageCache.delete(header)
  end

  def maybe_delete_cache_entry(_queue, header) when is_nil(header) do end
end
