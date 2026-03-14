import Config

alias :timer, as: Timer

config :logger, :default_handler, false

config :rabbitmq_message_deduplication,
  log_interval: Timer.seconds(60),
  cache_wait_time: Timer.seconds(30),
  cache_cleanup_period: Timer.seconds(3)
