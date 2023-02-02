# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2023, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Policies.Test do
  use ExUnit.Case

  alias RabbitMQMessageDeduplication.Policies, as: Policies

  test "queue policy validation", %{} do
    :ok = Policies.validate_policy([{<<"x-message-deduplication">>, true},
                                    {<<"x-message-deduplication">>, false}])
    {:error, _msg, ["true"]} = Policies.validate_policy([{<<"x-message-deduplication">>, "true"}])
  end

  test "queue exchange validation", %{} do
    :ok = Policies.validate_policy([{<<"x-cache-size">>, 100},
                                    {<<"x-cache-ttl">>, 100},
                                    {<<"x-cache-persistence">>, "disk"},
                                    {<<"x-cache-persistence">>, "memory"}])
    {:error, _msg, ["100"]} = Policies.validate_policy([{<<"x-cache-size">>, "100"}])
    {:error, _msg, [0]} = Policies.validate_policy([{<<"x-cache-size">>, 0}])
    {:error, _msg, ["100"]} = Policies.validate_policy([{<<"x-cache-ttl">>, "100"}])
    {:error, _msg, [0]} = Policies.validate_policy([{<<"x-cache-ttl">>, 0}])
    {:error, _msg, ["true"]} = Policies.validate_policy([{<<"x-cache-persistence">>, "true"}])
  end
end
