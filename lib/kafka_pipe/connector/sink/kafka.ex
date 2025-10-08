defmodule KafkaPipe.Connector.Sink.Kafka do
  @behaviour KafkaPipe.Connector.Sink
  use TypedStruct

  alias KafkaPipe.Connector.{Sink, Message}
  alias __MODULE__.Config
  require Logger
  import Bitwise

  @retry_in 5_000
  @producer_config [max_batch_size: 3 <<< 20, compression: :snappy]

  @spec start_link([Sink.start_opt()]) :: GenServer.on_start() | {:error, {:start, map() | String.t()}}
  def start_link(opts), do: Sink.start_link(__MODULE__, opts)

  @impl true
  def init(name, cfg) do
    case Config.new(cfg) do
      {:ok, cfg} -> do_init(name, cfg)
      {:error, errors} -> {:error, {:start, errors}}
    end
  end

  @impl true
  def handle_messages(messages, brod) do
    results = messages
      |> Enum.group_by(fn %Message{topic: topic} -> topic end)
      |> Task.async_stream(fn {topic, messages} -> produce(topic, messages, brod) end, timeout: :infinity)
      |> Enum.into([])

    case Keyword.fetch(results, :exit) do
      {:ok, {reason, _messages}} -> {:error, reason}
      :error ->
        n = length(messages)
        if n > 0, do: Logger.info("Produced #{n} messages")
        {:ok, brod}
    end
  end

  defp do_init(name, %Config{topics: topics, endpoints: endpoints, batch: batch}) do
    brod_endpoints = Enum.map(endpoints, fn endpoint ->
      [host, port] = String.split(endpoint, ":", parts: 2)
      {port, _} = Integer.parse(port)
      {host, port}
    end)

    with :ok <- maybe_create_topics(brod_endpoints, topics),
      {:ok, pid} <- :brod.start_link_client(brod_endpoints, :"#{name}.brod",
        auto_start_producers: true, default_producer_config: @producer_config)
    do
      {:ok, batch, pid}
    else
      {:error, reason} ->
        msg = case reason do
          reasons = [{{host, port}, {code, _stacktrace}} | _] when is_binary(host) and is_integer(port) and is_atom(code) ->
            Enum.map_join(reasons, "; ", fn {{host, port}, {code, _}} -> "#{host}:#{port} - #{code}" end)
          _ ->
            inspect(reason)
        end
        {:error, {:start, msg}}
    end
  end

  @spec produce(binary(), [Message.t()], pid()) :: :ok
  defp produce(topic, messages, brod) do
    brod_messages = Enum.map(messages, fn %Message{key: key, value: value, metadata: meta} ->
      headers = Enum.into(meta || %{}, [], fn {key, value} -> {to_string(key), to_string(value)} end)
      %{key: key, value: value, headers: headers}
    end)

    with {:error, reason} <- :brod.produce_sync(brod, topic, :hash, :undefined, brod_messages) do
      if reason == :client_down, do: raise "client down"
      Logger.error("Batch failed for topic #{topic} (#{length(messages)} messages): #{inspect(reason)}")
      Process.sleep(@retry_in)
      produce(topic, messages, brod)
    end
  end

  @spec maybe_create_topics([:brod.endpoint], [Config.Topic.t()]) :: :ok | {:error, any()}
  defp maybe_create_topics(endpoints, topic_configs) do
    brod_topic_configs = Enum.map(topic_configs, fn cfg ->
      cfg
      |> Map.from_struct()
      |> Map.put(:assignments, [])
      |> Map.update!(:configs, &Enum.map(&1, fn {name, value} -> [name: name, value: value] end))
    end)

    with {:error, reason} <- :brod.create_topics(endpoints, brod_topic_configs, %{timeout: 5_000}) do
      if is_binary(reason) and reason =~ ~r/Topic '.+' already exists/, do: :ok, else: {:error, reason}
    end
  end
end
