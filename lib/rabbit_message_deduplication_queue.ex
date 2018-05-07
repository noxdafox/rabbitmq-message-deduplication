# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2018, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQ.MessageDeduplicationPlugin.Queue do
  import Record, only: [defrecord: 2, defrecordp: 2, extract: 2]

  require RabbitMQ.MessageDeduplicationPlugin.Cache
  require RabbitMQ.MessageDeduplicationPlugin.Supervisor

  alias :rabbit_log, as: RabbitLog
  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: MessageCache
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

  defrecord :amqqueue, extract(
    :amqqueue, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

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

  def init(queue, recovery, callback) do
    if duplicate?(queue) do
      cache = queue |> amqqueue(:name) |> cache_name()
      persistence =
        queue
        |> amqqueue(:arguments)
        |> rabbitmq_keyfind("x-cache-persistence", "memory")
        |> String.to_atom()
      options = [persistence: persistence]

      RabbitLog.debug("Starting queue deduplication cache ~s~n", [cache])

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
    cache = queue |> amqqueue(:name) |> cache_name()

    :ok = MessageCache.drop(cache)
    :ok = CacheSupervisor.stop_cache(cache)

    passthrough1(state) do
      delete_and_terminate(any, qs)
    end
  end

  def delete_crashed(queue) do
    passthrough do: delete_crashed(queue)
  end

  def purge(state = dqstate(queue: queue, queue_state: qs)) do
    cache = queue |> amqqueue(:name) |> cache_name()

    :ok = MessageCache.flush(cache)

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

  def dropwhile(msg_pred, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: dropwhile(msg_pred, qs))
  end

  def fetchwhile(msg_pred, msg_fun, A, state = dqstate(queue_state: qs)) do
    passthrough3(state, do: fetchwhile(msg_pred, msg_fun, A, qs))
  end

  def fetch(need_ack, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: fetch(need_ack, qs)) do
      {:empty, state} -> {:empty, state}
      {{message, delivery, ack_tag}, state} ->
        case duplicate?(queue, message) do
          false -> {{message, delivery, dqack(tag: ack_tag)}, state}
          true ->
            header =
              message
              |> basic_message(:content)
              |> message_headers()
              |> rabbitmq_keyfind("x-deduplication-header")

            {{message, delivery, dqack(tag: ack_tag, header: header)}, state}
        end
    end
  end

  def drop(need_ack, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: drop(need_ack, qs))
  end

  def ack(acks, state = dqstate(queue: queue, queue_state: qs)) do
    acks = case duplicate?(queue) do
             false -> Enum.map(acks, fn(dqack(tag: ack)) -> ack end)
             true ->
               cache = queue |> amqqueue(:name) |> cache_name()

               Enum.map(acks, fn(dqack(tag: ack, header: header)) ->
                 if not is_nil(header) do
                   MessageCache.delete(cache, header)
                 end

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
  defp duplicate?(amqqueue(arguments: arguments)) do
    rabbitmq_keyfind(arguments, "x-message-deduplication", false)
  end

  # Returns true if the queue supports message deduplication
  # and the message is a duplicate.
  # The message is added to the deduplication cache if missing.
  defp duplicate?(amqqueue(name: name, arguments: arguments), message) do
    with true <- rabbitmq_keyfind(arguments, "x-message-deduplication", false),
         header when not is_nil(header) <- deduplication_header(message) do
      name |> cache_name() |> cached?(header)
    else
      _ -> false
    end
  end

  # Returns true if the key is is already present in the deduplication cache.
  # Otherwise, it adds it to the cache and returns false.
  defp cached?(cache, key) do
    case MessageCache.member?(cache, key) do
      true -> true
      false -> MessageCache.put(cache, key)
               false
    end
  end

  # Return the deduplication header of the given message, nil if none.
  defp deduplication_header(basic_message(content: content)) do
    case message_headers(content) do
      headers when is_list(headers) ->
        rabbitmq_keyfind(headers, "x-deduplication-header")
      _ -> nil
    end
  end

  # Returns a sanitized atom composed by the resource and exchange name
  defp cache_name({:resource, resource, :queue, queue}) do
    resource = sanitize_string(resource)
    queue = sanitize_string(queue)

    String.to_atom("cache_queue_#{resource}_#{queue}")
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
