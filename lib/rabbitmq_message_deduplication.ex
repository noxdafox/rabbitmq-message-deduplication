# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2023, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication do

  use Application

  # Start a dummy supervisor to enable the Application behaviour.
  # http://erlang.org/pipermail/erlang-questions/2010-April/050508.html
  @impl true
  def start(_, _) do
    Supervisor.start_link(__MODULE__, [], [])
  end

  @impl true
  def stop(_) do
    RabbitMQMessageDeduplication.Exchange.unregister()
    RabbitMQMessageDeduplication.Queue.disable()
    RabbitMQMessageDeduplication.PolicyEvent.disable()
  end

  def init([]) do
    Supervisor.init([], strategy: :one_for_one)
  end
end
