defmodule KafkaPipe.Connector.Sink.File do
  @behaviour KafkaPipe.Connector.Sink
  use TypedStruct

  alias KafkaPipe.Connector.Sink
  alias __MODULE__.Config
  require Logger

  @spec start_link([Sink.start_opt()]) :: GenServer.on_start() | {:error, {:start, map() | String.t()}}
  def start_link(opts), do: Sink.start_link(__MODULE__, opts)

  @impl true
  def init(_name, cfg) do
    case Config.new(cfg) do
      {:ok, %Config{batch: batch, path: path}} -> {:ok, batch, path}
      {:error, reason} -> {:error, {:start, reason}}
    end
  end

  @impl true
  def handle_messages(messages, path) do
    {:ok, file} = File.open(path, [:append])
    for msg <- messages do
      IO.write(file, [Jason.encode!(msg), "\n"])
    end
    :ok = File.close(file)

    n = length(messages)
    if n > 0, do: Logger.info("#{n} messages have been recorded to #{path}")

    {:ok, path}
  end
end
