defmodule KafkaPipe.Connector.Sink.Kafka do
  use GenStage
  use TypedStruct

  require Logger
  alias __MODULE__.Config
  alias KafkaPipe.Connector.Message

  @retry_in 5_000

  typedstruct module: State do
    field :brod, pid(), enforce: true
    field :batch_size, pos_integer(), enforce: true
    field :batch_timeout, pos_integer(), enforce: true
    field :source, GenStage.from()
    field :timer, reference()
  end

  @type start_opt() :: {:name, atom()}
    | {:subscribe_to, [atom() | pid() | {GenServer.server(), GenStage.subscription_options()}]}
    | {:config, Config.t()}

  @spec start_link([start_opt()]) :: GenServer.on_start() | {:error, {:start, String.t()}}
  def start_link(opts), do:
    GenStage.start_link(__MODULE__, opts, name: Keyword.fetch!(opts, :name))

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    subscribe_to = Keyword.fetch!(opts, :subscribe_to)
    Logger.metadata(name: name)
    Logger.info("Start")

    %Config{
      topics: topics,
      batch_timeout: batch_timeout,
      batch_size: batch_size,
      endpoints: endpoints,
    } = Keyword.fetch!(opts, :config)

    brod_endpoints = Enum.map(endpoints, fn endpoint ->
      [host, port] = String.split(endpoint, ":", parts: 2)
      {port, _} = Integer.parse(port)
      {host, port}
    end)

    with :ok <- maybe_create_topics(brod_endpoints, topics),
      {:ok, pid} <- :brod.start_link_client(brod_endpoints, :"#{name}.brod", auto_start_producers: true)
    do
      {:consumer, %State{brod: pid, batch_size: batch_size, batch_timeout: batch_timeout}, subscribe_to: subscribe_to}
    else
      {:error, reason} ->
        msg = case reason do
          reasons = [{{host, port}, {code, _stacktrace}} | _] when is_binary(host) and is_integer(port) and is_atom(code) ->
            Enum.map_join(reasons, "; ", fn {{host, port}, {code, _}} -> "#{host}:#{port} - #{code}" end)
          _ ->
            reason |> inspect() |> Logger.error()
            "check logs"
        end
        {:stop, {:start, "Kafka: " <> msg}}
    end
  end

  @impl true
  def handle_subscribe(:producer, _opts, from, %State{batch_timeout: batch_timeout} = state) do
    timer = Process.send_after(self(), :timeout, batch_timeout)
    {:manual, %{state | timer: timer, source: from}}
  end

  @impl true
  def handle_events(messages, _from, %State{brod: brod, batch_size: batch_size, timer: timer} = state) do
    payload = Enum.filter(messages, fn %Message{topic: topic} -> topic end)
    result = payload
      |> Enum.group_by(fn %Message{topic: topic} -> topic end)
      |> Task.async_stream(fn {topic, messages} -> produce(topic, messages, brod) end, timeout: :infinity)
      |> Enum.into([])

    case Keyword.fetch(result, :exit) do
      {:ok, {reason, _messages}} ->
        {:stop, reason, state}
      :error ->
        n = length(payload)
        if n > 0, do: Logger.info("Produced #{n} messages")

        messages
          |> Enum.group_by(fn %Message{from: from} -> from end)
          |> Enum.each(fn {from, messages} -> GenStage.call(from, {:ack, messages}, :infinity) end)

        if length(messages) < batch_size do
          {:noreply, [], state}
        else
          if timer, do: Process.cancel_timer(timer)
          send(self(), :timeout)
          {:noreply, [], %{state | timer: nil}}
        end
    end
  end

  @impl true
  def handle_info(:timeout, %State{batch_size: batch_size, batch_timeout: batch_timeout, source: source} = state) do
    GenStage.ask(source, batch_size)
    timer = Process.send_after(self(), :timeout, batch_timeout)
    {:noreply, [], %{state | timer: timer}}
  end

  @impl true
  def terminate(reason, _state), do: Logger.info("Stop #{inspect(reason)}")

  @spec produce(binary(), [Message.t()], pid()) :: :ok
  defp produce(topic, messages, brod) do
    brod_messages = Enum.map(messages, fn %Message{key: key, value: value, metadata: meta} ->
      headers = Enum.into(meta, [], fn {key, value} -> {to_string(key), to_string(value)} end)
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
