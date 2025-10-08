defmodule KafkaPipe.Connector.Source.Kafka do
  @behaviour KafkaPipe.Connector.Source
  @behaviour :brod_group_member
  use TypedStruct

  alias KafkaPipe.Connector.{Source, Message}
  alias __MODULE__.Config
  require Logger
  import Record

  defrecord :brod_received_assignment, extract(:brod_received_assignment, from_lib: "brod/include/brod.hrl")
  defrecord :kafka_message, extract(:kafka_message, from_lib: "brod/include/brod.hrl")

  typedstruct module: State do
    field :dirty_messages?, boolean(), default: false
    field :group_coord, pid(), enforce: true
    field :brod_id, atom(), enforce: true
    field :endpoints, [{String.t(), pos_integer()}], enforce: true
    field :fetch_max_bytes, pos_integer(), enforce: true
    field :offsets, %{{String.t(), non_neg_integer()} => non_neg_integer()}, default: %{}
    field :gen_id, integer()
    field :group_caller, reference()
  end

  @spec start_link([Source.start_opt()]) :: GenServer.on_start() | {:error, {:start, reason}} when reason: map() | any()
  def start_link(opts), do: Source.start_link(__MODULE__, opts)

  @impl true
  def init(name, cfg) do
    case Config.new(cfg) do
      {:ok, %Config{topics: topics, endpoints: endpoints, fetch_max_bytes: fetch_max_bytes}} ->
        brod_endpoints = Enum.map(endpoints, fn endpoint ->
          [host, port] = String.split(endpoint, ":", parts: 2)
          {port, _} = Integer.parse(port)
          {host, port}
        end)

        brod_id = :"#{name}.brod"
        {:ok, _} = :brod.start_link_client(brod_endpoints, brod_id)
        {:ok, group_coord} = :brod_group_coordinator.start_link(brod_id, Atom.to_string(name), topics, [], __MODULE__, self())
        {:ok, %State{group_coord: group_coord, brod_id: brod_id, endpoints: brod_endpoints, fetch_max_bytes: fetch_max_bytes}}
      {:error, errors} -> {:error, {:start, errors}}
    end
  end

  @impl true
  def handle_poll(count, %State{brod_id: brod_id, offsets: offsets, fetch_max_bytes: fetch_max_bytes, group_caller: nil} = state) do
    messages = offsets
      |> Enum.map(fn {{topic, partition}, offset} ->
        Task.async(fn -> {topic, partition,:brod.fetch(brod_id, topic, partition, offset, %{max_bytes: fetch_max_bytes})} end)
      end)
      |> Task.yield_many(timeout: 1_000, on_timeout: :ignore)
      |> Stream.map(fn
        {_task, {:ok, {topic, part, {:ok, {_hw_offset, brod_msgs}}}}} ->
          {topic, part, Enum.take(brod_msgs, count)}
        {_task, {:ok, {_, _, {:error, reason}}}} ->
          if reason != :offset_out_of_range, do: Logger.error("Fetch error: #{inspect(reason)}")
          nil
        {_task, nil} -> nil
        {_task, {_, _, {:error, reason}}} ->
          Logger.error("Fetch error: #{inspect(reason)}")
          nil
        {_task, {:exit, reason}} ->
          Logger.error("Fetch error (exit): #{inspect(reason)}")
          nil
      end)
      |> Stream.reject(&is_nil/1)
      |> Enum.flat_map(fn {topic, part, brod_messages} ->
        for kafka_message(key: key, value: value, offset: offset, headers: headers, ts: ts) <- brod_messages do
          headers = [{"partition", part}, {"offset", offset} | headers]
          headers = if ts == :undefined, do: headers, else: [{"ts_ms", ts} | headers]

          %Message{
            topic: topic,
            key: key,
            value: value,
            metadata: Map.new(headers)
          }
        end
      end)

    {:ok, messages, %State{state | dirty_messages?: messages != []}}
  end

  @impl true
  def handle_poll(_count, state), do: {:ok, [], state}

  @impl true
  def handle_ack(messages, %State{group_coord: group_coord, gen_id: gen_id, group_caller: group_caller} = state) do
    offsets = Enum.reduce(messages, %{}, fn %Message{topic: topic, metadata: %{"partition" => partition, "offset" => offset}}, acc ->
      offset = offset + 1
      Map.update(acc, {topic, partition}, offset, fn old -> max(old, offset) end)
    end)

    Enum.each(offsets, fn {{topic, partition}, offset} ->
      offset = offset - 1
      :brod_group_coordinator.ack(group_coord, gen_id, topic, partition, offset)
      Logger.info("Ack (topic: #{topic}, partition: #{partition}, offset: #{offset})")
    end)

    if group_caller, do: GenServer.reply({group_coord, group_caller}, :ok)
    %State{state | offsets: offsets, dirty_messages?: false, group_caller: nil}
  end

  @impl true
  def handle_call(:revoke, {_, ref}, %State{dirty_messages?: true} = state), do: {:noreply, %State{state | group_caller: ref}}

  @impl true
  def handle_call(:revoke, _from, %State{dirty_messages?: false} = state), do: {:reply, :ok, state}

  @impl true
  def handle_info({:set_assignments, gen_id, assignments}, %State{endpoints: endpoints} = state) do
    offsets = for brod_received_assignment(topic: topic, partition: partition, begin_offset: begin_offset) <- assignments, reduce: %{} do
      acc ->
        offset = if is_integer(begin_offset) do
          begin_offset
        else
          {:ok, offset} = :brod.resolve_offset(endpoints, topic, partition, :earliest)
          offset
        end
        Map.put(acc, {topic, partition}, offset)
    end

    {:noreply, %State{state | gen_id: gen_id, offsets: offsets}}
  end

  @impl true
  def handle_info({:EXIT, pid, reason}, %State{group_coord: pid} = state), do: {:stop, reason, state}

  @impl true
  def handle_info({:EXIT, _, _}, state), do: {:noreply, state}

  @impl true
  def assignments_received(pid, _member_id, generation_id, assignments) do
    send(pid, {:set_assignments, generation_id, assignments})
    :ok
  end

  @impl true
  def assignments_revoked(pid) do
    :ok = GenServer.call(pid, :revoke, :infinity)
    send(pid, {:set_assignments, nil, []})
    :ok
  end

  @impl true
  @dialyzer {:nowarn_function,  get_committed_offsets: 2}
  def get_committed_offsets(_pid, _list), do: raise "not implemented"
end
