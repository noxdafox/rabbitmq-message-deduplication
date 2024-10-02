defmodule RabbitMQ.MessageDeduplicationPlugin.Mixfile do
  use Mix.Project

  def project do
    [
      app: :rabbitmq_message_deduplication,
      version: "0.6.3",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps(),
      deps_path: System.get_env("DEPS_DIR", "deps"),
      aliases: aliases()
    ]
  end

  def application do
    [
      applications: [:mnesia],
      extra_applications: [:rabbit],
      mod: {RabbitMQMessageDeduplication, []},
      registered: [RabbitMQMessageDeduplication],
      broker_version_requirements: ["3.13.0-3.13.x", "4.0.0-4.0.x"]
    ]
  end

  defp deps() do
    [
      {
        :mix_task_archive_deps, "~> 1.0"
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
        "make_deps",
        "compile"
      ],
      make_archives: [
        "archive.build.elixir",
        "archive.build.all"
      ],
      make_tests: [
        "make_deps",
        "test"
      ]
    ]
  end
end
