defmodule RabbitMQ.MessageDeduplicationPlugin.Mixfile do
  use Mix.Project

  def project do
    deps_dir = case System.get_env("DEPS_DIR") do
      nil -> "deps"
      dir -> dir
    end

    [
      app: :rabbitmq_message_deduplication,
      version: "0.5.0",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps_path: deps_dir,
      deps: deps(deps_dir),
      aliases: aliases()
    ]
  end

  def application do
    applications = case Mix.env do
      :test -> [:mnesia]
      _ -> [:rabbit, :mnesia]
    end

    [
      applications: applications,
      mod: {RabbitMQMessageDeduplication, []}
    ]
  end

  defp deps(deps_dir) do
    [
      {
        :rabbit,
        path: Path.join(deps_dir, "rabbit"),
        compile: "true",
        override: true
      },
      {
        :rabbit_common,
        path: Path.join(deps_dir, "rabbit_common"),
        compile: "true",
        override: true
      }
    ]
  end

  defp aliases do
    [
      # Do not start the application during unit tests
      test: "test --no-start",
      make_deps: [
        "deps.get",
        "deps.compile"
      ],
      make_app: [
        "compile"
      ],
      make_all: [
        "deps.get",
        "deps.compile",
        "compile"
      ],
      make_tests: [
        "test"
      ]
    ]
  end
end
