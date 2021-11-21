import Config

alias :timer, as: Timer

config :rabbitmq_message_deduplication,
  cache_wait_time: Timer.seconds(30),
  cache_cleanup_period: Timer.seconds(3)
