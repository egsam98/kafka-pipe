defmodule KafkaPipe.Connector.Source.Postgres.Internal do
  use Postgrex.ReplicationConnection
  use TypedStruct

  alias KafkaPipe.Connector.Message
  alias KafkaPipe.Connector.Source.{Postgres, Postgres.Config}
  alias KafkaPipe.Pg.Lsn
  require Logger
  import Postgrex.PgOutput.Messages

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
  def handle_connect(%State{step: :init, config: %Config{tables: tables, publication: pub}} = state) do
    query = "CREATE PUBLICATION #{pub} FOR TABLE #{Enum.join(tables, ", ")}"
    Logger.info(query)
    {:query, query, %{state | step: :create_pub}}
  end

  @impl true
  def handle_result(res, %State{step: :create_pub, config: %Config{publication: pub}} = state) when is_list(res) do
    Logger.info("Publication \"#{pub}\" has been created")
    create_slot(state)
  end

  @impl true
  def handle_result(res, %State{step: :create_slot, config: %Config{slot: slot}} = state) when is_list(res) do
    Logger.info("Slot \"#{slot}\" has been created")
    start_replication(state)
  end

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
  def handle_data(msg, state) when is_binary(msg), do: msg
    |> handle_message(state)
    |> Tuple.insert_at(0, :noreply)

  @impl true
  def handle_call({:commit_lsn, commit_lsn}, from, state) do
    GenServer.reply(from, :ok)
    {:noreply, %{state | commit_lsn: commit_lsn}}
  end

  defp create_slot(%State{config: %Config{slot: slot}} = state) do
    query = "CREATE_REPLICATION_SLOT #{slot} LOGICAL pgoutput"
    {:query, query, %{state | step: :create_slot}}
  end

  defp start_replication(%State{commit_lsn: commit_lsn, config: %Config{slot: slot, publication: pub}} = state) do
    query = "START_REPLICATION SLOT #{slot} LOGICAL #{commit_lsn} (proto_version '1', publication_names '#{pub}', messages 'true')"
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
  defp push_data(rel_id, lsn, row, clock, pg_source,
    %State{
      config: %Config{hostname: hostname, port: port, database: database},
      relations: relations
    }
  ) do
    %Relation{namespace: rel_namespace, name: rel_name, columns: columns} = relations[rel_id]

    {key, value} = [columns, row]
      |> Enum.zip()
      |> Enum.reduce({%{}, %{}}, fn {column(name: column, type: type, flags: flags), raw_data}, {key, value} ->
        data = Postgrex.PgOutput.decode_value(raw_data, type)
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
