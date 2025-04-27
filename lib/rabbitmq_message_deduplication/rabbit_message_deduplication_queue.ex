# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2025, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Queue do
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

  import Record, only: [defrecord: 2]

  require RabbitMQMessageDeduplication.Cache
  require RabbitMQMessageDeduplication.Common

  alias :mc, as: MC
  alias :amqqueue, as: AMQQueue
  alias :rabbit_log, as: RabbitLog
  alias :rabbit_amqqueue, as: RabbitQueue
  alias RabbitMQMessageDeduplication.Common, as: Common
  alias RabbitMQMessageDeduplication.Cache, as: Cache
  alias RabbitMQMessageDeduplication.CacheManager, as: CacheManager

  @behaviour :rabbit_backing_queue

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {__MODULE__,
                     [{:description, "message deduplication queue"},
                      {:mfa, {__MODULE__, :enable, []}},
                      {:requires, :kernel_ready},
                      {:enables, :core_initialized}]}

  defrecord :dqack, [:tag, :header]
  defrecord :dqstate, [:queue, :queue_state, dedup_enabled: false]

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
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      dqstate(queue: dqstate(unquote(state), :queue),
              queue_state: backing_queue.unquote(function),
              dedup_enabled: dqstate(unquote(state), :dedup_enabled))
      end
  end

  # Backing queue functions returning a tuple {result, state}
  defmacrop passthrough2(state, do: function) do
    quote do
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      {result, queue_state} = backing_queue.unquote(function)
      {result, dqstate(queue: dqstate(unquote(state), :queue),
                       queue_state: queue_state,
                       dedup_enabled: dqstate(unquote(state), :dedup_enabled))}
    end
  end

  # Backing queue functions returning a tuple {result1, result2, state}
  defmacrop passthrough3(state, do: function) do
    quote do
      backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
      {result1, result2, queue_state} = backing_queue.unquote(function)
      {result1, result2, dqstate(queue: dqstate(unquote(state), :queue),
                                 queue_state: queue_state,
                                 dedup_enabled: dqstate(unquote(state), :dedup_enabled))}
    end
  end

  @doc """
  Enable deduplication queues support.

  Replace the original backing queue module with this one.

  """
  @spec enable() :: :ok
  def enable() do
    case Application.get_env(:rabbit, :backing_queue_module) do
      __MODULE__ -> :ok
      backing_queue ->
        RabbitLog.info(
          "Deduplication queues enabled, real BQ is ~s~n", [backing_queue])
        Application.put_env(__MODULE__, :backing_queue_module, backing_queue)
        Application.put_env(:rabbit, :backing_queue_module, __MODULE__)
        maybe_reconfigure_caches()
    end
  end

  @doc """
  Disable deduplication queues support.

  Revert to the original backing queue module.

  """
  @spec disable() :: :ok
  def disable() do
    case Application.get_env(:rabbit, :backing_queue_module) do
      __MODULE__ ->
        backing_queue = Application.get_env(__MODULE__, :backing_queue_module)
        RabbitLog.info(
          "Deduplication queues disabled, real BQ is ~s~n", [backing_queue])
        Application.put_env(:rabbit, :backing_queue_module, backing_queue)
      _ -> :ok
    end
  end

  @impl :rabbit_backing_queue
  def start(vhost, queues) do
    passthrough do: start(vhost, queues)
  end

  @impl :rabbit_backing_queue
  def stop(vhost) do
    passthrough do: stop(vhost)
  end

  @impl :rabbit_backing_queue
  def init(queue, recovery, callback) do
    state = maybe_toggle_dedup_queue(dqstate(queue: queue))

    passthrough1(state) do
      init(queue, recovery, callback)
    end
  end

  @impl :rabbit_backing_queue
  def terminate(any, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: terminate(any, qs))
  end

  @impl :rabbit_backing_queue
  def delete_and_terminate(any, state = dqstate(queue: queue, queue_state: qs)) do
    if dedup_queue?(state) do
      :ok = delete_cache(queue)
    end

    passthrough1(state) do
      delete_and_terminate(any, qs)
    end
  end

  @impl :rabbit_backing_queue
  def delete_crashed(queue) do
    passthrough do: delete_crashed(queue)
  end

  @impl :rabbit_backing_queue
  def purge(state = dqstate(queue: queue, queue_state: qs)) do
    if dedup_queue?(state) do
      :ok = AMQQueue.get_name(queue) |> Common.cache_name() |> Cache.flush()
    end

    passthrough2(state, do: purge(qs))
  end

  @impl :rabbit_backing_queue
  def purge_acks(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: purge_acks(qs))
  end

  @impl :rabbit_backing_queue
  def publish(message, properties, boolean, pid,
              state = dqstate(queue_state: qs)) do
    passthrough1(state) do
      publish(message, properties, boolean, pid, qs)
    end
              end

  # Optimization for cases in which the queue is empty and the message
  # is delivered straight to the client. Acknowledgement is enabled.
  @impl :rabbit_backing_queue
  def publish_delivered(message, properties, pid, state) do
    dqstate(queue_state: qs) = state

    {ack_tag, state} = passthrough2(state) do
      publish_delivered(message, properties, pid, qs)
    end

    if dedup_queue?(state) do
      head = Common.message_header(message, "x-deduplication-header")
      {dqack(tag: ack_tag, header: head), state}
    else
      {ack_tag, state}
    end
  end

  @impl :rabbit_backing_queue
  def discard(msg_id, pid, state = dqstate(queue_state: qs)) do
  # v4.0.x
  def discard(msg_id, pid, state = dqstate(queue_state: qs)) when is_binary(msg_id) do
    passthrough1(state, do: discard(msg_id, pid, qs))
  end
  @impl :rabbit_backing_queue
  def discard(message, pid, state = dqstate(queue: queue, queue_state: qs)) do
    if dedup_queue?(state) do
      maybe_delete_cache_entry(queue, message)
    end

    passthrough1(state, do: discard(message, pid, qs))
  end
  # v3.13.x
  def discard(msg_id, pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: discard(msg_id, pid, flow, qs))
  end

  @impl :rabbit_backing_queue
  def drain_confirmed(state = dqstate(queue_state: qs)) do
    passthrough2(state, do: drain_confirmed(qs))
  end

  # The dropwhile callback handles message TTL expiration.
  # The duplicates cache TTL mechanism is used instead.
  @impl :rabbit_backing_queue
  def dropwhile(msg_pred, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: dropwhile(msg_pred, qs))
  end

  # The fetchwhile callback handles message TTL dead lettering.
  # The duplicates cache TTL mechanism is used instead.
  @impl :rabbit_backing_queue
  def fetchwhile(msg_pred, msg_fun, acc, state = dqstate(queue_state: qs)) do
    passthrough3(state, do: fetchwhile(msg_pred, msg_fun, acc, qs))
  end

  @impl :rabbit_backing_queue
  def fetch(needs_ack, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: fetch(needs_ack, qs)) do
      {:empty, state} -> {:empty, state}
      {{message, delivery, ack_tag}, state} ->
        if dedup_queue?(state) do
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
  @impl :rabbit_backing_queue
  def drop(need_ack, state = dqstate(queue: queue, queue_state: qs)) do
    if dedup_queue?(state) do
      case fetch(need_ack, state) do
        {:empty, state} -> {:empty, state}
        {{message, _, ack_tag}, state} ->
          maybe_delete_cache_entry(queue, message)
	  id = MC.get_annotation(:id, message)

          {{id, ack_tag}, state}
      end
    else
      passthrough2(state, do: drop(need_ack, qs))
    end
  end

  @impl :rabbit_backing_queue
  def ack(acks = [dqack() | _], state) do
    dqstate(queue: queue, queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag, header: header)) ->
                            maybe_delete_cache_entry(queue, header)
                            ack_tag
                          end)

    passthrough2(state, do: ack(acks, qs))
  end

  @impl :rabbit_backing_queue
  def ack(acks, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: ack(acks, qs))
  end

  @impl :rabbit_backing_queue
  def requeue(acks = [dqack() | _], state = dqstate(queue_state: qs)) do
    acks = Enum.map(acks, fn(dqack(tag: ack_tag)) -> ack_tag end)

    passthrough2(state, do: requeue(acks, qs))
  end

  @impl :rabbit_backing_queue
  def requeue(acks, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: requeue(acks, qs))
  end

  @impl :rabbit_backing_queue
  def ackfold(function, acc, state, acks = [dqack() | _]) do
    dqstate(queue: queue, queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag, header: header)) ->
                            maybe_delete_cache_entry(queue, header)
                            ack_tag
                          end)

    passthrough2(state, do: ackfold(function, acc, qs, acks))
  end

  @impl :rabbit_backing_queue
  def ackfold(function, acc, state = dqstate(queue_state: qs), acks) do
    passthrough2(state, do: ackfold(function, acc, qs, acks))
  end

  @impl :rabbit_backing_queue
  def fold(function, acc, state = dqstate(queue_state: qs)) do
    passthrough2(state, do: fold(function, acc, qs))
  end

  @impl :rabbit_backing_queue
  def len(dqstate(queue_state: qs)) do
    passthrough do: len(qs)
  end

  @impl :rabbit_backing_queue
  def is_empty(dqstate(queue_state: qs)) do
    passthrough do: is_empty(qs)
  end

  @impl :rabbit_backing_queue
  def depth(dqstate(queue_state: qs)) do
    passthrough do: depth(qs)
  end

  @impl :rabbit_backing_queue
  def update_rates(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: update_rates(qs))
  end

  @impl :rabbit_backing_queue
  def needs_timeout(dqstate(queue_state: qs)) do
    passthrough do: needs_timeout(qs)
  end

  @impl :rabbit_backing_queue
  def timeout(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: timeout(qs))
  end

  @impl :rabbit_backing_queue
  def handle_pre_hibernate(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: handle_pre_hibernate(qs))
  end

  @impl :rabbit_backing_queue
  def resume(state = dqstate(queue_state: qs)) do
    passthrough1(state, do: resume(qs))
  end

  @impl :rabbit_backing_queue
  def msg_rates(dqstate(queue_state: qs)) do
    passthrough do: msg_rates(qs)
  end

  @impl :rabbit_backing_queue
  def info(:backing_queue_status, state = dqstate(queue: queue, queue_state: qs)) do
    args = AMQQueue.get_arguments(queue)
    queue_info = passthrough do: info(:backing_queue_status, qs)
    priority = Common.rabbit_argument(args, "x-max-priority", default: false)

    if dedup_queue?(state) and !priority do
      [message_deduplication_cache_info: cache_info(queue)] ++ queue_info
    else
      queue_info
    end
  end

  @impl :rabbit_backing_queue
  def info(atom, dqstate(queue_state: qs)) do
    passthrough do: info(atom, qs)
  end

  @impl :rabbit_backing_queue
  def invoke(__MODULE__, function, state) do
    function.(__MODULE__, state)
  end

  @impl :rabbit_backing_queue
  def invoke(module, function, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: invoke(module, function, qs))
  end

  @impl :rabbit_backing_queue
  def is_duplicate(message, state = dqstate(queue: queue, queue_state: qs)) do
    case passthrough2(state, do: is_duplicate(message, qs)) do
      {true, state} -> {true, state}
      {false, state} -> if dedup_queue?(state) do
                          {duplicate?(queue, message), state}
                        else
                          {false, state}
                        end
    end
  end

  @impl :rabbit_backing_queue
  def set_queue_mode(queue_mode, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(queue_mode, qs))
  end

  @impl :rabbit_backing_queue
  def set_queue_version(queue_version, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_version(queue_version, qs))
  end

  @impl :rabbit_backing_queue
  def zip_msgs_and_acks(delivered_publish, acks = [dqack() | _], acc, state) do
    dqstate(queue_state: qs) = state
    acks = Enum.map(acks, fn(dqack(tag: ack_tag)) -> ack_tag end)

    passthrough do: zip_msgs_and_acks(delivered_publish, acks, acc, qs)
  end

  @impl :rabbit_backing_queue
  def zip_msgs_and_acks(delivered_publish, acks, acc, state) do
    dqstate(queue_state: qs) = state

    passthrough do: zip_msgs_and_acks(delivered_publish, acks, acc, qs)
  end

  # Not present anymore in recent versions of RMQ, keeping for old ones
  def handle_info(term, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_queue_mode(term, qs))
  end

  #
  # Compatibility with 3.13.x
  #
  @impl :rabbit_backing_queue
  def publish(message, properties, boolean, pid, flow,
              state = dqstate(queue_state: qs)) do
    passthrough1(state) do
      publish(message, properties, boolean, pid, flow, qs)
    end
  end

  @impl :rabbit_backing_queue
  def batch_publish(batch, pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: batch_publish(batch, pid, flow, qs))
  end

  # Optimization for cases in which the queue is empty and the message
  # is delivered straight to the client. Acknowledgement is enabled.
  @impl :rabbit_backing_queue
  def publish_delivered(message, properties, pid, flow, state) do
    dqstate(queue_state: qs) = state

    {ack_tag, state} = passthrough2(state) do
      publish_delivered(message, properties, pid, flow, qs)
    end

    if dedup_queue?(state) do
      head = Common.message_header(message, "x-deduplication-header")
      {dqack(tag: ack_tag, header: head), state}
    else
      {ack_tag, state}
    end
  end

  @impl :rabbit_backing_queue
  def batch_publish_delivered(batch, pid, flow, state) do
    dqstate(queue_state: qs) = state

    passthrough2(state) do
      batch_publish_delivered(batch, pid, flow, qs)
    end
  end

  @impl :rabbit_backing_queue
  def discard(msg_id, pid, flow, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: discard(msg_id, pid, flow, qs))
  end

  @impl :rabbit_backing_queue
  def set_ram_duration_target(duration, state = dqstate(queue_state: qs)) do
    passthrough1(state, do: set_ram_duration_target(duration, qs))
  end

  @impl :rabbit_backing_queue
  def ram_duration(state = dqstate(queue_state: qs)) do
    passthrough2(state, do: ram_duration(qs))
  end

  # Utility functions

  # Enable/disable queue-level deduplication
  def maybe_toggle_dedup_queue(state = dqstate(queue: queue, queue_state: qs)) do
    cond do
      enable_dedup_queue?(state) ->
        :ok = init_cache(queue)
        dqstate(queue: queue, queue_state: qs, dedup_enabled: true)
      disable_dedup_queue?(state) ->
        :ok = delete_cache(queue)
        dqstate(queue: queue, queue_state: qs, dedup_enabled: false)
      true ->
        dqstate(queue: queue, queue_state: qs, dedup_enabled: false)
    end
  end

  # Caches created prior to v0.6.0 need to be reconfigured.
  defp maybe_reconfigure_caches() do
    RabbitLog.debug("Deduplication Queues startup, reconfiguring old caches~n")

    RabbitQueue.list()
    |> Enum.filter(&dedup_arg?/1)
    |> Enum.map(&init_cache/1)

    :ok
  end

  # Initialize the deduplication cache
  defp init_cache(queue) do
    cache = queue |> AMQQueue.get_name() |> Common.cache_name()
    ttl = queue
      |> AMQQueue.get_arguments()
      |> Common.rabbit_argument("x-message-ttl", type: :number)
    options = [ttl: ttl, persistence: :memory]

    RabbitLog.debug(
      "Starting queue deduplication cache ~s with options ~p~n",
      [cache, options])

    case CacheManager.create(cache, false, options) do
      :ok -> Cache.flush(cache)
      {:error, {:already_exists, ^cache}} -> Cache.flush(cache)
      error -> error
    end
  end

  # Remove the cache and all its content
  defp delete_cache(queue) do
    cache = queue |> AMQQueue.get_name() |> Common.cache_name()

    RabbitLog.debug("Deleting queue deduplication cache ~s~n", [cache])

    CacheManager.destroy(cache)
  end

  # Returns true if the message is a duplicate.
  defp duplicate?(queue, message) do
    queue
    |> AMQQueue.get_name()
    |> Common.duplicate?(message, message_expiration(message))
  end

  # Returns the expiration property of the given message
  defp message_expiration(message) do
    case MC.ttl(message) do
      :undefined -> nil
      ttl -> ttl
    end
  end

  # Removes the message deduplication header from the cache
  defp maybe_delete_cache_entry(queue, msg) when is_tuple(msg) do
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

  # True if `x-message-deduplication` is present within queue arguments or policy
  defp dedup_arg?(queue) do
    queue_policy(queue)
    ++ AMQQueue.get_arguments(queue)
    |> Common.rabbit_argument("x-message-deduplication", default: false)
  end

  # Return the list of policy arguments assigned to the queue
  defp queue_policy(queue) do
    case AMQQueue.get_policy(queue) do
      :undefined -> []
      policy -> policy[:definition]
    end
  end

  # True if it's an active deduplication queue
  defp dedup_queue?(dqstate(dedup_enabled: val)), do: val

  # True if deduplication should be enabled for the queue
  defp enable_dedup_queue?(dqstate(dedup_enabled: true)), do: false
  defp enable_dedup_queue?(dqstate(queue: q, dedup_enabled: false)), do: dedup_arg?(q)

  # True if deduplication should be disabled for the queue
  defp disable_dedup_queue?(dqstate(dedup_enabled: false)), do: false
  defp disable_dedup_queue?(dqstate(queue: q, dedup_enabled: true)), do: not dedup_arg?(q)
end
