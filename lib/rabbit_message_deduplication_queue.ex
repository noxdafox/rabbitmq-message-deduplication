# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2019, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQ.MessageDeduplicationPlugin.Queue do
  @moduledoc """
  This module adds support for deduplication queues.

  Messages carrying the `x-deduplication-header` header will be deduplicated
  if a message with the same header value is already present within the queue.

  When a message is published within the queue, it's checked against duplicates.
  If no duplicate is found, the message is inserted and its deduplication header
  cached. Once the message is acknowledged or dropped, the header is removed
  from the cache.

  This module implements the `rabbit_backing_queue` behaviour delegating
  all the queue related operation to the underlying backing queue.

  """

  import Record, only: [defrecord: 2, defrecord: 3, defrecordp: 2, extract: 2]

  require RabbitMQ.MessageDeduplicationPlugin.Cache
  require RabbitMQ.MessageDeduplicationPlugin.Common

  alias :amqqueue, as: AMQQueue
  alias :rabbit_log, as: RabbitLog
  alias RabbitMQ.MessageDeduplicationPlugin.Common, as: Common
  alias RabbitMQ.MessageDeduplicationPlugin.Cache, as: Cache
  alias RabbitMQ.MessageDeduplicationPlugin.CacheManager, as: CacheManager

  @behaviour :rabbit_backing_queue

  Module.register_attribute __MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true

  @rabbit_boot_step {__MODULE__,
                     [{:description, "message deduplication queue"},
                      {:mfa, {__MODULE__, :enable_deduplication_queues, []}},
                      {:requires, :database},
                      {:enables, :external_infrastructure}]}

  defrecord :content, extract(
    :content, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_message, extract(
    :basic_message, from_lib: "rabbit_common/include/rabbit.hrl")

  defrecord :basic_properties, :P_basic, extract(
    :P_basic, from_lib: "rabbit_common/include/rabbit_framing.hrl")

  defrecordp :dqack, [:tag, :header]
  defrecordp :dqstate, [:queue, :queue_state]

  # The passthrough macros call the underlying backing queue functions
  # The suffixes indicate the arity of the return values
  # of the backing queue functions they are wrapping.

  # Backing queue functions returning one value, does not change the queue state
  defmacrop passthrough(do: function) do
    quote do
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      backing_queue.unquote(function)
    end
  end

  # Backing queue functions returning the state
  defmacrop passthrough1(state, do: function) do
    quote do
      queue = dqstate(unquote(state), :queue)
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      queue_state = backing_queue.unquote(function)
      dqstate(queue: queue, queue_state: queue_state)
    end
  end

  # Backing queue functions returning a tuple {result, state}
  defmacrop passthrough2(state, do: function) do
    quote do
      queue = dqstate(unquote(state), :queue)
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      {result, queue_state} = backing_queue.unquote(function)
      {result, dqstate(queue: queue, queue_state: queue_state)}
    end
  end

  # Backing queue functions returning a tuple {result1, result2, state}
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
    name = AMQQueue.get_name(queue)
    args = AMQQueue.get_arguments(queue)

    if duplicate?(queue) do
      cache = Common.cache_name(name)
      options = [ttl: Common.rabbit_argument(
                   args, "x-message-ttl", type: :number),
                 persistence: :memory]

      RabbitLog.debug(
        "Starting queue deduplication cache ~s with options ~p~n",
        [cache, options])

      :ok = CacheManager.create(cache, options)
      :ok = Cache.flush(cache)
    end

    passthrough1(dqstate(queue: queue)) do
      init(queue, recovery, callback)
    end
  end

  def terminate(any, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: terminate(any, qs))
  end

  def delete_and_terminate(any, state) do
    dqstate(queue: queue, queue_state: qs) = state

    if duplicate?(queue) do
      queue
      |> AMQQueue.get_name()
      |> Common.cache_name()
      |> CacheManager.destroy()
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
      cache = queue |> AMQQueue.get_name() |> Common.cache_name()

      :ok = Cache.flush(cache)
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

  def batch_publish(batch, pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: batch_publish(batch, pid, flow, qs))
  end

  # Optimization for cases in which the queue is empty and the message
  # is delivered straight to the client. Acknowledgement is enabled.
  def publish_delivered(message, message_properties, pid, flow, state) do
    dqstate(queue: queue, queue_state: qs) = state

    {ack_tag, state} = passthrough2(state) do
      publish_delivered(message, message_properties, pid, flow, qs)
    end

    if duplicate?(queue) do
      head = Common.message_header(message, "x-deduplication-header")
      {dqack(tag: ack_tag, header: head), state}
    else
      {ack_tag, state}
    end
  end

  def batch_publish_delivered(batch, pid, flow, state) do
    dqstate(queue_state: qs) = state

    passthrough2(state) do
      batch_publish_delivered(batch, pid, flow, qs)
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
  def fetchwhile(msg_pred, msg_fun, acc, state = dqstate(queue_state: qs)) do
    passthrough3(state, do: fetchwhile(msg_pred, msg_fun, acc, qs))
  end

  def fetch(needs_ack, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: fetch(needs_ack, qs)) do
      {:empty, state} -> {:empty, state}
      {{message, delivery, ack_tag}, state} ->
        if duplicate?(queue) do
          if needs_ack do
            head = Common.message_header(message, "x-deduplication-header")
            {{message, delivery, dqack(tag: ack_tag, header: head)}, state}
          else
            maybe_delete_cache_entry(queue, message)
            {{message, delivery, ack_tag}, state}
          end
        else
          {{message, delivery, ack_tag}, state}
        end
    end
  end

  # TODO: this is a bit of a hack.
  # As the drop callback returns only the message id, we can't retrieve
  # the message deduplication header. As a workaround `fetch` is used.
  # This assumes the backing queue drop and fetch behaviours are the same.
  # A better solution would be to store the message IDs in a dedicated index.
  def drop(need_ack, state = dqstate(queue: queue, queue_state: qs)) do
    if duplicate?(queue) do
      case fetch(need_ack, state) do
        {:empty, state} -> {:empty, state}
        {{message = basic_message(id: id), _, ack_tag}, state} ->
          maybe_delete_cache_entry(queue, message)

          {{id, ack_tag}, state}
      end
    else
      passthrough2(state, do: drop(need_ack, qs))
    end
  end

  def ack(acks = [dqack() | _], state) do
    dqstate(queue: queue, queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag, header: header)) ->
                            maybe_delete_cache_entry(queue, header)
                            ack_tag
                          end)

    passthrough2(state, do: ack(acks, qs))
  end

  def ack(acks, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: ack(acks, qs))
  end

  def requeue(acks = [dqack() | _], state = dqstate(queue_state: qs)) do
    acks = Enum.map(acks, fn(dqack(tag: ack_tag)) -> ack_tag end)

    passthrough2(state, do: requeue(acks, qs))
  end

  def requeue(acks, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: requeue(acks, qs))
  end

  def ackfold(function, acc, state, acks = [dqack() | _]) do
    dqstate(queue: queue, queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag, header: header)) ->
                            maybe_delete_cache_entry(queue, header)
                            ack_tag
                          end)

    passthrough2(state, do: ackfold(function, acc, qs, acks))
  end

  def ackfold(function, acc, state = dqstate(queue_state: qs), acks) do
    passthrough2(state, do: ackfold(function, acc, qs, acks))
  end

  def fold(function, acc, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: fold(function, acc, qs))
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

  def info(:backing_queue_status, dqstate(queue: queue, queue_state: qs)) do
    args = AMQQueue.get_arguments(queue)
    queue_info = passthrough do: info(:backing_queue_status, qs)
    priority = Common.rabbit_argument(args, "x-max-priority", default: false)

    if duplicate?(queue) and !priority do
      [message_deduplication_cache_info: cache_info(queue)] ++ queue_info
    else
      queue_info
    end
  end

  def info(atom, dqstate(queue_state: qs)) do
    passthrough do: info(atom, qs)
  end

  def invoke(atom, function, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: invoke(atom, function, qs))
  end

  def is_duplicate(message, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: is_duplicate(message, qs)) do
      {true, state} -> {true, state}
      {false, state} -> {duplicate?(queue, message), state}
    end
  end

  def set_queue_mode(queue_mode, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(queue_mode, qs))
  end

  def zip_msgs_and_acks(delivered_publish, acks = [dqack() | _], acc, state) do
    dqstate(queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag)) -> ack_tag end)

    passthrough do: info(delivered_publish, acks, acc, qs)
  end

  def zip_msgs_and_acks(delivered_publish, acks, acc, state) do
    dqstate(queue_state: qs) = state

    passthrough do: info(delivered_publish, acks, acc, qs)
  end

  def handle_info(term, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(term, qs))
  end

  # Utility functions

  # Returns true if the queue supports message deduplication
  defp duplicate?(queue) do
    args = AMQQueue.get_arguments(queue)

    Common.rabbit_argument(args, "x-message-deduplication", default: false)
  end

  # Returns true if the queue supports message deduplication
  # and the message is a duplicate.
  defp duplicate?(queue, message = basic_message()) do
    name = AMQQueue.get_name(queue)

    case duplicate?(queue) do
      true -> Common.duplicate?(name, message, message_expiration(message))
      false -> false
    end
  end

  # Returns the expiration property of the given message
  defp message_expiration(message) do
    basic_message(content: content(properties: properties)) = message

    case properties do
      basic_properties(expiration: ttl) when is_bitstring(ttl) ->
        String.to_integer(ttl)
      basic_properties(expiration: :undefined) -> nil
      :undefined -> nil
    end
  end

  # Removes the message deduplication header from the cache
  defp maybe_delete_cache_entry(queue, msg = basic_message()) do
    header = Common.message_header(msg, "x-deduplication-header")
    maybe_delete_cache_entry(queue, header)
  end

  defp maybe_delete_cache_entry(queue, header) when not is_nil(header) do
    queue
    |> AMQQueue.get_name()
    |> Common.cache_name()
    |> Cache.delete(header)
  end

  defp maybe_delete_cache_entry(_queue, header) when is_nil(header) do end

  # Returns the cache information
  defp cache_info(queue) do
    name = AMQQueue.get_name(queue)
    cache = Common.cache_name(name)

    try do
      Cache.info(cache)
    catch
      :exit, {:aborted, {:no_exists, ^cache, _}} -> []
      :exit, {:noproc, {GenServer, :call, [^cache | _]}} -> []
    end
  end
end
