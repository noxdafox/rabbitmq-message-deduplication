# This Source Code Form is subject to the terms of the Mozilla Public
# License, v. 2.0. If a copy of the MPL was not distributed with this
# file, You can obtain one at http://mozilla.org/MPL/2.0/.
#
# Copyright (c) 2017-2021, Matteo Cafasso.
# All rights reserved.

defmodule RabbitMQMessageDeduplication.Policies do
  @moduledoc """
  Implement RabbitMQ policy validator behaviour to validate
  the plugin specific policies.
  """

  alias :rabbit_registry, as: RabbitRegistry

  @behaviour :rabbit_policy_validator

  Module.register_attribute(__MODULE__,
    :rabbit_boot_step,
    accumulate: true, persist: true)

  @rabbit_boot_step {__MODULE__,
                     [{:description, "message deduplication policy validator"},
                      {:mfa, {__MODULE__, :register, []}},
                      {:requires, :rabbit_registry},
                      {:enables, :recovery}]}

  @policy_validators [policy_validator: <<"x-cache-size">>,
                      policy_validator: <<"x-cache-ttl">>,
                      policy_validator: <<"x-cache-persistence">>,
                      policy_validator: <<"x-message-deduplication">>]

  def register() do
    for {class, name} <- @policy_validators do
      RabbitRegistry.register(class, name, __MODULE__)
    end

    :ok
  end

  @doc """
  Validate through all the policies stopping at the first error.
  """
  @impl :rabbit_policy_validator
  @spec validate_policy([{Binary.t, any}]) :: :ok | {:error, String.t, [term]}
  def validate_policy(policies) do
    Enum.reduce(policies, :ok, fn {key, value}, :ok -> policy_validator(key, value)
                                  _, error -> error
                               end)
  end

  defp policy_validator(<<"x-message-deduplication">>, true), do: :ok
  defp policy_validator(<<"x-message-deduplication">>, false), do: :ok
  defp policy_validator(<<"x-message-deduplication">>, val) do
    {:error, "~t'x-message-deduplication' must be a boolean, got ~p", [val]};
  end

  defp policy_validator(<<"x-cache-size">>, val) when is_integer(val) and val > 0, do: :ok
  defp policy_validator(<<"x-cache-size">>, val) do
    {:error, "~t'x-cache-size' must be an integer greater than 0, got ~p", [val]};
  end

  defp policy_validator(<<"x-cache-ttl">>, val) when is_integer(val) and val > 0, do: :ok
  defp policy_validator(<<"x-cache-ttl">>, val) do
    {:error, "~t'x-cache-ttl' must be an integer greater than 0, got ~p", [val]};
  end

  defp policy_validator(<<"x-cache-persistence">>, "disk"), do: :ok
  defp policy_validator(<<"x-cache-persistence">>, "memory"), do: :ok
  defp policy_validator(<<"x-cache-persistence">>, val) do
    {:error, "~t'x-cache-persistence' must be either 'disk' or 'memory', got ~p", [val]};
  end
end
