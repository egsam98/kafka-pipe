defmodule KafkaPipe.Connector.Sink.ClickHouse do
  @behaviour KafkaPipe.Connector.Sink
  use TypedStruct

  alias KafkaPipe.Connector.{Sink, Message}
  alias __MODULE__.Config

  @query_meta """
    SELECT name, type
    FROM system.columns
    WHERE database = {$0:String} AND table = {$1:String} AND default_expression = ''
  """

  @spec start_link([Sink.start_opt()]) :: GenServer.on_start() | {:error, {:start, reason}} when reason: map() | any()
  def start_link(opts), do: Sink.start_link(__MODULE__, opts)

  typedstruct module: State do
    field :conn, pid(), enforce: true
    field :db, :string, enforce: true
    field :offsets, %{{String.t(), non_neg_integer()} => non_neg_integer()}, default: %{}
  end

  @impl true
  def init(_name, config) do
    case Config.new(config) do
      {:ok, %Config{hostname: hostname, port: port, database: db, batch: batch} = cfg} ->
        {:ok, conn} = cfg
          |> Map.from_struct()
          |> Map.update!(:scheme, &Atom.to_string/1)
          |> Keyword.new()
          |> Ch.start_link()

        if DBConnection.status(conn) == :error do
          {:error, {:start, "No connection to #{hostname}:#{port}"}}
        else
          {:ok, batch, %State{conn: conn, db: db}}
        end
      {:error, errors} -> {:error, {:start, errors}}
    end
  end

  @impl true
  def handle_messages(messages, %State{db: db, conn: conn} = state) do
    results = messages
      |> Enum.group_by(fn %Message{topic: topic} -> topic end)
      |> Task.async_stream(fn {topic, msgs} -> insert(topic, msgs, conn, db) end) # Handle errors
      |> Enum.to_list()

    case Keyword.fetch(results, :exit) do
      {:ok, {reason, _messages}} -> {:error, reason}
      :error -> {:ok, state}
    end
  end

  defp insert(nil, _messages, _conn, _db), do: :ok

  defp insert(topic, messages, conn, db) do
    %Ch.Result{rows: name_types} = Ch.query!(conn, @query_meta, [db, topic])
    {columns, types} = Enum.map_reduce(name_types, [], fn [name, type], acc -> {name, acc ++ [type]} end)

    values = Enum.map(messages, fn %Message{value: value} ->
      values_map = case Jason.decode(value) do
        {:ok, value = %{}} -> value
        {:ok, value} -> raise ArgumentError, "JSON map is required, got: #{inspect(value)}"
        {:error, %Jason.DecodeError{data: data} = error} ->
          raise ArgumentError, "#{error.__struct__}: #{Exception.message(error)}: #{data}}"
      end
      Enum.map(columns, & Map.get(values_map, &1))
    end)

    Ch.query!(conn, "INSERT INTO #{topic} (#{Enum.join(columns, ",")}) FORMAT RowBinary", values, types: types)
    :ok
  end
end
