defmodule RabbitMQ.MessageDeduplicationPlugin.Mixfile do
  use Mix.Project

  def project() do
    [
      app: :rabbitmq_message_deduplication,
      version: "0.7.0",
      build_embedded: Mix.env == :prod,
      start_permanent: Mix.env == :prod,
      deps: deps(),
      deps_path: System.get_env("DEPS_DIR", "deps"),
      aliases: aliases()
    ]
  end

  def application() do
    [
      # The Application needs to depend on `rabbit` in order to be detected as a plugin.
      extra_applications: [:mnesia, :rabbit],
      mod: {RabbitMQMessageDeduplication, []},
      registered: [RabbitMQMessageDeduplication],
      broker_version_requirements: if Mix.env == :prod do
        ["3.13.0", "4.0.0", "4.1.0"]
      else
        []
      end
    ]
  end

  defp deps() do
    [
      {:mix_task_archive_deps, github: "rabbitmq/mix_task_archive_deps", runtime: false}
    ]
  end

  defp aliases() do
    [
      make_app: [
        "deps.get",
        "deps.compile",
        "compile"
      ],
      make_archives: [
        "archive.build.deps --destination=#{dist_dir()}",
        "archive.build.elixir --destination=#{dist_dir()}",
        "archive.build.all --destination=#{dist_dir()}"
      ],
      make_tests: [
        "deps.get",
        "test"
      ],
      # Do not start the application during unit tests
      test: "test --no-start"
    ]
  end

  defp dist_dir() do
    System.get_env("DIST_DIR", "plugins")
  end
end
