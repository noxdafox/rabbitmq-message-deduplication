# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2019-2020, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication do

  use Application

  def start(_, _) do
    children = [
      %{
        id: RabbitMQMessageDeduplication.CacheManager,
        start: {RabbitMQMessageDeduplication.CacheManager, :start_link, []}
      }
    ]

    supervisor = Supervisor.start_link(children, [strategy: :one_for_one])

    RabbitMQMessageDeduplication.Exchange.register()
    RabbitMQMessageDeduplication.Queue.enable()
    RabbitMQMessageDeduplication.Queue.restart_queues()

    supervisor
  end

  def stop(_) do
    RabbitMQMessageDeduplication.Exchange.unregister()
    RabbitMQMessageDeduplication.Queue.disable()
  end
end
