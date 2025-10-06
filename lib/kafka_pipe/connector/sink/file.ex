defmodule KafkaPipe.Connector.Sink.File do
  @behaviour KafkaPipe.Connector.Sink
  use TypedStruct

  alias KafkaPipe.Connector.Sink
  alias __MODULE__.Config
  require Logger

  typedstruct module: State do
    field :path, Path.t()
    field :file, File.io_device()
  end

  @spec start_link([Sink.start_opt()]) :: GenServer.on_start() | {:error, {:start, map() | String.t()}}
  def start_link(opts), do: Sink.start_link(__MODULE__, opts)

  @impl true
  def init(_name, cfg) do
    with {:ok, %Config{batch: batch, path: path}} <- Config.new(cfg),
      {:ok, file} <- File.open(path, [:append])
    do
      {:ok, batch, %State{path: path, file: file}}
    else
      {:error, reason} -> {:error, {:start, reason}}
    end
  end

  @impl true
  def handle_messages(messages, %State{path: path, file: file} = state) do
    for msg <- messages do
      IO.write(file, [Jason.encode!(msg), "\n"])
    end

    n = length(messages)
    if n > 0, do: Logger.info("#{n} messages have been recorded to #{path}")

    {:ok, state}
  end

  @impl true
  def terminate(_reason, %State{file: file}) do
    with {:error, reason} <- File.close(file) do
      Logger.error("Failed to close file: #{reason}")
    end
  end
end
