defmodule RabbitMQ.Supervisor do
  use DynamicSupervisor

  def start(_type, args) do
    DynamicSupervisor.start_link(__MODULE__, args, name: __MODULE__)
  end

  def start_child(spec) do
    DynamicSupervisor.start_child(__MODULE__, spec)
  end

  def terminate_child(name) do
    DynamicSupervisor.terminate_child(__MODULE__, name)
  end

  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
