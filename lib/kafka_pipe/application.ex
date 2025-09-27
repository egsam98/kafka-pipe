defmodule KafkaPipe.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = [
      KafkaPipe.Connector.Supervisor,
      KafkaPipeWeb.Telemetry,
      {DNSCluster, query: Application.get_env(:kafka_pipe, :dns_cluster_query) || :ignore},
      {Phoenix.PubSub, name: KafkaPipe.PubSub},
      # Start to serve requests, typically the last entry
      KafkaPipeWeb.Endpoint
    ]

    # See https://hexdocs.pm/elixir/Supervisor.html
    # for other strategies and supported options
    opts = [strategy: :one_for_one, name: KafkaPipe.Supervisor]
    Supervisor.start_link(children, opts)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    KafkaPipeWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
