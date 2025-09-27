defmodule KafkaPipe.Connector.Source.Postgres do
  use GenStage
  use TypedStruct

  require Logger
  import Postgrex.PgOutput.Messages
  alias KafkaPipe.Connector.{MemberDB, Message}
  alias __MODULE__.{Config, Internal}
  alias KafkaPipe.Pg.Lsn

  typedstruct module: State do
    field :name, atom(), enforce: true
    field :internal, pid(), enforce: true
    field :buffer, [Message.t()], default: []
  end

  @type start_opt :: {:name, atom()} | {:config, Config.t()}

  @spec start_link([start_opt()]) :: GenServer.on_start() | {:error, {:start, reason}}
    when reason: Postgrex.Error.t() | any()
  def start_link(opts) do
    name = Keyword.fetch!(opts, :name)
    with {:error, %Postgrex.Error{} = error} <- GenStage.start_link(__MODULE__, opts, name: name) do
      {:error, {:start, error}}
    end
  end

  @spec push(pid(), Message.t()) :: :ok
  def push(pid, message), do: GenStage.cast(pid, {:push, message})

  @spec push_stub(pid(), Lsn.t()) :: :ok
  def push_stub(pid, lsn), do: GenStage.cast(pid, {:push, %Message{from: pid, metadata: %{lsn: lsn}}})

  @impl true
  def init(opts) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    cfg = Keyword.fetch!(opts, :config)
    Logger.metadata(name: name)
    Logger.info("Start")

    MemberDB.open(name)
    commit_lsn = case MemberDB.get(name, :commit_lsn) do
      [] -> %Lsn{}
      [commit_lsn] -> commit_lsn
    end

    case Internal.start_link(commit_lsn, name: :"#{name}.pgrepl", config: cfg) do
      {:ok, pid} -> {:producer, %State{name: name, internal: pid}, buffer_size: :infinity}
      {:error, reason} -> {:stop, {:start, reason}}
    end
  end

  @impl true
  def handle_demand(demand, %State{buffer: buffer} = state) do
    messages = buffer
      |> Enum.take(-demand)
      |> Enum.reverse()
    {:noreply, messages, state}
  end

  @impl true
  def handle_cast({:push, msg}, %State{buffer: buffer} = state), do:
    {:noreply, [], %{state | buffer: [msg | buffer]}}

  @impl true
  def handle_call({:ack, messages}, _from, %State{name: name, buffer: buffer, internal: internal} = state) do
    %Message{metadata: %{lsn: lsn}} = List.last(messages)
    MemberDB.put(name, :commit_lsn, lsn)
    Internal.commit_lsn(internal, lsn)
    {:reply, :ok, [], %{state | buffer: buffer -- messages}} # TODO test
  end

  @impl true
  def handle_info({:EXIT, internal, reason}, %State{internal: internal} = state), do: {:stop, reason, state}

  @impl true
  def terminate(reason, %State{name: name}) do
    MemberDB.close(name)
    Logger.info("Stop #{inspect(reason)}")
  end

  defmodule Internal do
    use Postgrex.ReplicationConnection
    use TypedStruct

    require Logger
    import Postgrex.PgOutput.Messages

    alias KafkaPipe.Connector.Source.Postgres
    alias KafkaPipe.Pg.Lsn

    typedstruct module: Relation do
      field :namespace, binary(), enforce: true
      field :name, atom(), enforce: true
      field :columns, [tuple()], enforce: true
    end

    typedstruct module: State do
      field :step, atom(), default: :init
      field :relations, %{pos_integer() => Relation.t()}, default: %{}
      field :commit_lsn, Lsn.t(), enforce: true
      field :config, Config.t(), enforce: true
    end

    @spec start_link(Lsn.t(), [Postgres.start_opt()]) :: {:ok, pid()} | {:error, Postgrex.Error.t() | any()}
    def start_link(commit_lsn, opts) do
      pg_opts = opts
        |> Keyword.fetch!(:config)
        |> Map.from_struct()
        |> Keyword.new()
      Postgrex.ReplicationConnection.start_link(__MODULE__, {commit_lsn, opts}, pg_opts)
    end

    @spec commit_lsn(pid(), Lsn.t()) :: :ok
    def commit_lsn(pid, lsn), do: GenServer.call(pid, {:commit_lsn, lsn})

    @impl true
    def init({commit_lsn, opts}) do
      Logger.metadata(name: Keyword.fetch!(opts, :name))
      {:ok, %State{config: Keyword.fetch!(opts, :config), commit_lsn: commit_lsn}}
    end

    @impl true
    def handle_connect(%State{step: :init, config: %Config{tables: tables}} = state) do
      query = "CREATE PUBLICATION pipe FOR TABLE #{Enum.join(tables, ", ")}"
      Logger.info(query)
      {:query, query, %{state | step: :create_pub}}
    end

    @impl true
    def handle_result(result, %State{step: :create_pub} = state) when is_list(result),
      do: create_slot(state)

    @impl true
    def handle_result(result, %State{step: :create_slot} = state) when is_list(result),
      do: start_replication(state)

    @impl true
    def handle_result(%Postgrex.Error{} = error, %State{step: step} = state) do
      msg = Postgrex.Error.message(error)
      cond do
        msg =~ ~r/publication ".+" already exists/ -> create_slot(state)
        msg =~ ~r/replication slot ".+" already exists/ -> start_replication(state)
        true -> {:disconnect, %{step: step, error: error}}
      end
    end

    @impl true
    def handle_data(msg, state) when is_binary(msg), do:
      msg |> handle_message(state) |> Tuple.insert_at(0, :noreply)

    @impl true
    def handle_call({:commit_lsn, commit_lsn}, from, state) do
      GenServer.reply(from, :ok)
      {:noreply, %{state | commit_lsn: commit_lsn}}
    end

    defp create_slot(state) do
      query = "CREATE_REPLICATION_SLOT pipe LOGICAL pgoutput"
      Logger.info(query)
      {:query, query, %{state | step: :create_slot}}
    end

    defp start_replication(%State{commit_lsn: commit_lsn} = state) do
      query =
        "START_REPLICATION SLOT pipe LOGICAL #{commit_lsn} (proto_version '1', publication_names 'pipe', messages 'true')"

      Logger.info(query)
      {:stream, query, [], %{state | step: :streaming}}
    end

    @spec handle_message(binary(), State.t()) :: {iodata(), State.t()}
    defp handle_message(msg, state) when is_binary(msg), do: msg |> decode() |> handle_message(state)

    defp handle_message(msg_primary_keep_alive(reply: 0, server_wal: wal), state) do
      {:parent, pg_source} = Process.info(self(), :parent)
      Postgres.push_stub(pg_source, Lsn.from_tuple(wal))
      {[], state}
    end

    @spec handle_message(tuple(), State.t()) :: {[tuple()], State.t()}
    defp handle_message(msg_primary_keep_alive(reply: 1), %State{commit_lsn: commit_lsn} = state) do
      Logger.debug("Commit LSN: #{commit_lsn}")
      standby = commit_lsn
        |> Lsn.to_int64()
        |> then(fn lsn -> msg_standby_status_update(wal_recv: lsn, wal_flush: lsn, wal_apply: lsn, system_clock: now(), reply: 0) end)
        |> encode()

      {[standby], state}
    end

    @spec handle_message(tuple(), State.t()) :: {[tuple()], State.t()}
    defp handle_message(msg_xlog_data(data: data, end_lsn: lsn, system_clock: clock), %State{relations: relations} = state) do
      {:parent, pg_source} = Process.info(self(), :parent)
      lsn = Lsn.from_tuple(lsn)

      case data do
        msg_relation(id: rel_id, namespace: namespace, name: name, columns: columns) ->
          Postgres.push_stub(pg_source, lsn)
          relation = %Relation{namespace: namespace, name: name, columns: columns}
          {[], %{state | relations: Map.put(relations, rel_id, relation)}}

        msg_insert(relation_id: rel_id, data: data) ->
          push_data(rel_id, lsn, data, clock, pg_source, state)
          {[], state}

        msg_update(relation_id: rel_id, change_data: data) ->
          push_data(rel_id, lsn, data, clock, pg_source, state)
          {[], state}

        _ ->
          Postgres.push_stub(pg_source, lsn)
          {[], state}
      end
    end

    @spec push_data(pos_integer(), Lsn.t(), [any()], DateTime.t(), pid(), State.t()) :: :ok
    defp push_data(rel_id, lsn, data, clock, pg_source,
      %State{
        config: %Config{hostname: hostname, port: port, database: database},
        relations: relations
      }
    ) do
      %Relation{namespace: rel_namespace, name: rel_name, columns: columns} = relations[rel_id]

      {key, value} = [columns, data]
        |> Enum.zip()
        |> Enum.reduce({%{}, %{}}, fn {column(name: column, flags: flags), data}, {key, value} ->
          key = if Enum.member?(flags, :key), do: Map.put(key, column, data), else: key
          value = Map.put(value, column, data)
          {key, value}
        end)

      message = %Message{
        topic: rel_namespace <> "." <> rel_name,
        from: pg_source,
        key: Jason.encode_to_iodata!(key),
        value: Jason.encode_to_iodata!(value),
        metadata: %{
          lsn: lsn,
          version: KafkaPipe.version(),
          ts_ms: DateTime.to_unix(clock, :millisecond),
          host: hostname,
          port: port,
          database: database,
          relation: rel_name,
        },
      }
      Postgres.push(pg_source, message)
    end
  end
end
