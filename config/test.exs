import Config

config :kafka_pipe, test?: true

# Configure your database
#
# The MIX_TEST_PARTITION environment variable can be used
# to provide built-in test partitioning in CI environment.
# Run `mix help test` for more information.
config :kafka_pipe, KafkaPipe.Repo,
  username: "postgres",
  password: "postgres",
  hostname: "localhost",
  database: "kafka_pipe_test#{System.get_env("MIX_TEST_PARTITION")}",
  pool: Ecto.Adapters.SQL.Sandbox,
  pool_size: System.schedulers_online() * 2

# We don't run a server during test. If one is required,
# you can enable the server option below.
config :kafka_pipe, KafkaPipeWeb.Endpoint,
  http: [ip: {127, 0, 0, 1}, port: 4002],
  secret_key_base: "Mwij2OcIwc8G04zpdFE0r7NPagOzSa07JdE2HqmVB19sr0TrdjwMFIJIR8Kq9bmy",
  server: false

# In test we don't send emails
config :kafka_pipe, KafkaPipe.Mailer, adapter: Swoosh.Adapters.Test

# Print only warnings and errors during test
config :logger, level: :warning

# Initialize plugs at runtime for faster test compilation
config :phoenix, :plug_init_mode, :runtime

# Enable helpful, but potentially expensive runtime checks
config :phoenix_live_view,
  enable_expensive_runtime_checks: true
