# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2021, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.PolicyEvent do
  @moduledoc """
  The `backing_queue_behaviour` is plagued by several issues which make
  implementing queue policy support hard to say the least.

  Firstly, the behaviour does not provide any callback allowing
  the backing queue to react to policy changes. Most importantly, policies
  are not stored within transient queues.

  This module overcomes both limitations by using :gen_event to react
  to internal RabbitMQ notifications related to policy changes.

  When a policy is changed, if it pertains the `x-message-deduplication`
  attribute, the module will apply the policy to all queues matching it.
  """

  require RabbitMQMessageDeduplication.Queue

  alias :amqqueue, as: AMQQueue
  alias :gen_event, as: GenEvent
  alias :rabbit_policy, as: RabbitPolicy
  alias :rabbit_amqqueue, as: RabbitQueue
  alias RabbitMQMessageDeduplication.Queue, as: BackingQueue

  @behaviour :gen_event

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {__MODULE__,
                     [{:description, "message deduplication policy events"},
                      {:mfa, {__MODULE__, :enable, []}},
                      {:requires, :recovery},
                      {:enables, :routing_ready}]}

  @spec enable() :: :ok
  def enable() do
    GenEvent.add_handler(:rabbit_event, __MODULE__, [])
  end

  @spec disable() :: :ok
  def disable() do
    GenEvent.delete_handler(:rabbit_event, __MODULE__, [])
  end

  @impl :gen_event
  def init(_), do: {:ok, []}

  @impl :gen_event
  def handle_event({:event, :policy_set, policy, _, _}, state) do
    case List.keyfind(policy[:definition], "x-message-deduplication", 0) do
      {"x-message-deduplication", _} -> apply_to_queues()
      nil -> 0
    end

    {:ok, state}
  end
  def handle_event(_, state), do: {:ok, state}

  @impl :gen_event
  def handle_call(_Request, state), do: {:ok, :not_understood, state}

  # Apply new policies to matching queues
  defp apply_to_queues() do
    for queue <- RabbitQueue.list() |> Enum.map(&RabbitPolicy.set/1) do
      AMQQueue.get_pid(queue)
      |> RabbitQueue.run_backing_queue(BackingQueue,
                                       fn(_, state) ->
                                         state = BackingQueue.dqstate(state, queue: queue)
                                         BackingQueue.maybe_enable_dedup_queue(state)
                                       end)
    end
  end
end
