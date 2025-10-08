defmodule KafkaPipe.Connector.Source.Randomizer do
  @behaviour KafkaPipe.Connector.Source

  alias KafkaPipe.Connector.{Source, Message}
  alias __MODULE__.Config
  import Rand

  @spec start_link([Source.start_opt()]) :: GenServer.on_start() | {:error, {:start, reason}} when reason: map() | any()
  def start_link(opts), do: Source.start_link(__MODULE__, opts)

  @impl true
  def init(_name, config) do
    case Config.new(config) do
      {:ok, cfg} -> {:ok, cfg}
      {:error, errors} -> {:ok, {:start, errors}}
    end
  end

  @impl true
  def handle_poll(count, %Config{topic: topic, template: tmpl} = state) do
    messages = Enum.map(1..count, fn number ->
      value = Regex.replace(~r/"%rand"/, tmpl, "\"#{rand()}\"")
      %Message{topic: topic, key: rand(), value: value, metadata: %{"number" => number}}
    end)
    {:ok, messages, state}
  end

  @impl true
  def handle_ack(_messages, state), do: state
end
