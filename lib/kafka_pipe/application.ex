defmodule KafkaPipe.Application do
  # See https://hexdocs.pm/elixir/Application.html
  # for more information on OTP Applications
  @moduledoc false

  use Application

  @impl true
  def start(_type, _args) do
    children = if KafkaPipe.test?(),
      do: [],
      else: [
        KafkaPipe.Connector.Supervisor,
        KafkaPipeWeb.Telemetry,
        {DNSCluster, query: Application.get_env(:kafka_pipe, :dns_cluster_query) || :ignore},
        {Phoenix.PubSub, name: KafkaPipe.PubSub},
        KafkaPipeWeb.Endpoint
      ]

    Supervisor.start_link(children, strategy: :one_for_one, name: KafkaPipe.Supervisor)
  end

  # Tell Phoenix to update the endpoint configuration
  # whenever the application is updated.
  @impl true
  def config_change(changed, _new, removed) do
    KafkaPipeWeb.Endpoint.config_change(changed, removed)
    :ok
  end
end
