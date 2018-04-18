defmodule RabbitMQ.CacheSupervisor do
  use DynamicSupervisor

  require RabbitMQ.Cache

  def start_link() do
    case DynamicSupervisor.start_link(__MODULE__, [], name: __MODULE__) do
      {:ok, _pid} -> :ok
      _ -> :error
    end
  end

  def start_cache(cache, options) do
    specifications = %{id: cache,
                       start: {RabbitMQ.Cache, :start_link, [cache, options]}}

    case DynamicSupervisor.start_child(__MODULE__, specifications) do
      {:ok, _} -> :ok
      {:error, {:already_started, _}} -> :ok
      _ -> :error
    end
  end

  def stop_cache(cache) do
    DynamicSupervisor.terminate_child(__MODULE__, RabbitMQ.Cache.process(cache))
  end

  def init(_args) do
    DynamicSupervisor.init(strategy: :one_for_one)
  end
end
