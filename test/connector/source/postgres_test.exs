defmodule Test.Connector.Source.Postgres do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.MemberDB
  alias KafkaPipe.Pg.Lsn
  alias KafkaPipe.Connector.Message
  alias KafkaPipe.Connector.Source.{Postgres, Postgres.State, Postgres.Internal}
  import Test.Rand
  import Mimic

  describe "init/2" do
    @name rand(:atom)
    @start_lsn %Lsn{file: 0, offset: 25}

    setup do
      expect MemberDB, :open, fn @name -> :ok end
      {:ok, %{config: %{tables: ["test"]}}}
    end

    test "ok without state", %{config: cfg} do
      expect MemberDB, :get, fn @name, :commit_lsn -> [] end
      pid = rand(:pid)
      expect Internal, :start_link, fn [name: _, config: _, start_lsn: %Lsn{file: 0, offset: 0}] ->
        {:ok, pid}
      end

      assert {:ok, %State{name: @name, internal: pid}} == Postgres.init(@name, cfg)
    end

    test "ok with state", %{config: cfg} do
      expect MemberDB, :get, fn @name, :commit_lsn -> [@start_lsn] end
      pid = rand(:pid)
      expect Internal, :start_link, fn [name: _, config: _, start_lsn: @start_lsn] ->
        {:ok, pid}
      end

      assert {:ok, %State{name: @name, internal: pid}} == Postgres.init(@name, cfg)
    end

    test "invalid config" do
      assert {:error, {:start, reason}} = Postgres.init(@name, %{})
      assert is_map(reason)
    end

    test "internal failed", %{config: cfg} do
      reason = :crash
      expect MemberDB, :get, fn @name, :commit_lsn -> [] end
      expect Internal, :start_link, fn _ -> {:error, reason} end

      assert {:error, {:start, reason}} == Postgres.init(@name, cfg)
    end
  end

  test "handle_demand/2" do
    state = %State{internal: nil, name: nil, buffer: Enum.to_list(5..1//-1)}
    assert {:ok, messages, new_state} = Postgres.handle_demand(3, state)
    assert state == new_state
    assert messages == [1, 2, 3]
  end

  test "handle_cast(:push)/2" do
    state = %State{internal: nil, name: nil, buffer: msgs([2, 1])}
    assert {:noreply, [], %State{buffer: buffer}} = Postgres.handle_cast({:push, msg(3)}, state)
    assert buffer == msgs([3, 2, 1])
  end

  test "handle_ack/2" do
    name = rand(:atom)
    pid = rand(:pid)
    state = %State{internal: pid, name: name, buffer: msgs([3, 2, 1])}

    expect MemberDB, :put, fn ^name, :commit_lsn, lsn ->
      assert lsn == %Lsn{file: 0, offset: 2}
    end
    expect Internal, :commit_lsn, fn ^pid, lsn ->
      assert lsn == %Lsn{file: 0, offset: 2}
    end

    assert %State{buffer: buffer} = Postgres.handle_ack(msgs([1, 2]), state)
    assert buffer == msgs([3])
  end

  test "terminate/2" do
    name = rand(:atom)
    expect MemberDB, :close, fn ^name -> :ok end
    Postgres.terminate(:normal, %State{internal: self(), name: name})
  end

  # Simplify messages assert by offsets in metadata
  defp msgs(offsets), do: Enum.map(offsets, fn offset ->
    %Message{metadata: %{lsn: %Lsn{file: 0, offset: offset}}}
  end)

  defp msg(offset), do: %Message{metadata: %{lsn: %Lsn{file: 0, offset: offset}}}
end

defmodule Test.Connector.Source.Postgres.Config do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.Source.Postgres.Config

  test "ok" do
    assert {:ok, %Config{
      hostname: "0.0.0.0",
      port: 5432,
      database: "postgres",
      username: "postgres",
      password: "postgres",
      tables: ["test"],
      publication: "kafka_pipe",
      slot: "kafka_pipe",
      timestamp_format: :millisecond
    }} == Config.new(%{hostname: "0.0.0.0", tables: ["test"]})
  end

  test "blank fields" do
    for value <- [nil, " "] do
      data = :fields
        |> Config.__schema__()
        |> Enum.map(& {&1, value})
        |> Map.new()
      assert {:error, %{tables: ["can't be blank"]}} == Config.new(data)
    end
  end

  test "negative port number" do
    assert {:error, %{port: ["must be greater than 0"]}} == Config.new(%{port: -1, tables: ["test"]})
  end

  test "blank tables" do
    for tables <- [[], [nil], [" "]] do
      assert {:error, %{tables: ["should have at least 1 item(s)"]}} == Config.new(%{tables: tables})
    end
  end

  test "timestamp format" do
    for format <- [:rfc3339, :second, :millisecond] do
      assert {:ok, _} = Config.new(%{tables: ["test"], timestamp_format: format})
    end
    assert {:error, %{timestamp_format: ["is invalid"]}} ==
      Config.new(%{tables: ["test"], timestamp_format: :ok})
  end
end

defmodule Test.Connector.Source.Postgres.Internal do
  use ExUnit.Case, async: true

  alias Testcontainers.{Container, PostgresContainer, ContainerBuilder}
  alias KafkaPipe.Connector.Message
  alias KafkaPipe.Connector.Source.Postgres.{Config, Internal, Internal.State}
  alias KafkaPipe.Pg.Lsn
  import Test.Rand

  @version KafkaPipe.version()
  @zero_lsn %Lsn{file: 0, offset: 0}

  setup_all do
    cfg = PostgresContainer.new()
      |> ContainerBuilder.build()
      |> Container.with_cmd(~w(postgres -c wal_level=logical))
    {:ok, cont} = Testcontainers.start_container(cfg)
    ExUnit.Callbacks.on_exit(fn -> Testcontainers.stop_container(cont.container_id) end)
    conn = cont
      |> PostgresContainer.connection_parameters()
      |> then(&start_link_supervised!({Postgrex, &1}))

    Postgrex.query!(conn, "CREATE TABLE test (id int primary key, name text)")

    {:ok, cfg} = PostgresContainer.connection_parameters(cont)
      |> Map.new()
      |> Map.put(:tables, ["test"])
      |> Config.new()
    {:ok, %{conn: conn, config: cfg}}
  end

  test "validate messages", %{conn: conn, config: %Config{publication: pub} = cfg} do
    names = ["Ozzy", "Dio"]

    {:ok, pid} = Internal.start_link(name: rand(:atom), config: cfg, start_lsn: @zero_lsn)
    %Postgrex.Result{
      rows: [[true]]
    } = Postgrex.query!(conn, "SELECT exists(SELECT 1 FROM pg_publication where pubname = $1)", [pub])
    Postgrex.query!(conn, "INSERT INTO test (id, name) VALUES (1, $1)", Enum.take(names, 1))
    Postgrex.query!(conn, "UPDATE test SET name = $1 WHERE id = 1", Enum.take(names, -1))

    messages = Stream.resource(
      fn -> 0 end,
      fn counter ->
        assert_receive {:"$gen_cast", {:push, %Message{topic: topic} = msg}}
        counter = if topic, do: counter + 1, else: counter
        res = if counter == length(names), do: :halt, else: [msg]
        {res, counter}
      end,
      &Function.identity/1)
      |> Enum.to_list()

    assert length(messages) > length(names), "System messages must be pushed as well"
    refute Enum.any?(messages, fn %Message{key: key, value: value}-> is_nil(key) || is_nil(value) end),
      "All messages must have key and value filled"

    messages = messages
      |> Stream.reject(&is_nil(&1.topic))
      |> Enum.into([])
    assert length(messages) != length(names), "Expected #{length(names)} messages"

    messages
    |> Enum.filter(fn %Message{topic: topic} -> topic end)
    |> Enum.reduce([prev: nil, i: 0], fn msg, [prev: prev, i: i] ->
      assert %Message{
        topic: "public.test",
        key: key,
        value: value,
        metadata: %{
          port: port,
          version: @version,
          host: hostname,
          relation: "test",
          database: database,
          ts_ms: ts_ms,
          lsn: lsn,
        }
      } = msg

      assert %Config{port: ^port, hostname: ^hostname, database: ^database} = cfg
      assert IO.iodata_to_binary(key) == ~s({"id":1})
      assert IO.iodata_to_binary(value) == ~s({"id":1,"name":"#{Enum.at(names, i)}"})
      assert {:ok, _} = DateTime.from_unix(ts_ms, :millisecond)

      if prev do
        assert ts_ms > prev.metadata[:ts_ms]
        assert KafkaPipe.Pg.Lsn.to_int64(lsn) > KafkaPipe.Pg.Lsn.to_int64(prev.metadata[:lsn])
      end

      [prev: msg, i: i + 1]
    end)

    GenServer.stop(pid)
  end

  test "publication predefined", %{conn: conn, config: cfg} do
    pub = rand()
    Postgrex.query!(conn, "CREATE PUBLICATION #{pub} FOR TABLE test")
    cfg = %Config{cfg | publication: pub}
    Internal.start_link(name: String.to_atom(pub), config: cfg, start_lsn: @zero_lsn)
  end

  test "slot predefined", %{conn: conn, config: cfg} do
    slot = rand()
    Postgrex.query!(conn, "SELECT pg_create_logical_replication_slot($1, 'pgoutput')", [slot])
    cfg = %Config{cfg | slot: slot}
    Internal.start_link(name: String.to_atom(slot), config: cfg, start_lsn: @zero_lsn)
  end

  test "commit LSN", %{config: cfg} do
    state = %State{commit_lsn: @zero_lsn, config: cfg}
    commit_lsn = %Lsn{file: 0, offset: 1}
    assert {:noreply, %State{state | commit_lsn: commit_lsn}} ==
      Internal.handle_call({:commit_lsn, commit_lsn}, {self(), make_ref()}, state)
  end

  describe "timestamp formats" do
    setup %{format: format, conn: conn, config: cfg} do
      table = rand()
      Postgrex.query!(conn, "CREATE TABLE #{table} (time timestamp not null)")
      cfg = %Config{cfg | timestamp_format: format, tables: [table], publication: table, slot: table}
      {:ok, _} = Internal.start_link(name: rand(:atom), config: cfg, start_lsn: @zero_lsn)
      naive_ts = NaiveDateTime.new!(2000, 1, 1, 23, 59, 59, 111_110)
      Postgrex.query!(conn, "INSERT INTO #{table} (time) VALUES ('#{naive_ts}')")

      # Expect message
      %Message{value: value} =
        Stream.repeatedly(fn ->
          assert_receive {:"$gen_cast", {:push, %Message{} = msg}}
          msg
        end)
        |> Enum.find(& &1.topic == "public." <> table)

      %{"time" => ts} = Jason.decode!(value)
      {:ok, %{ts: ts, naive_ts: naive_ts}}
    end

    @tag format: :rfc3339
    test "rfc3339", %{ts: ts, naive_ts: naive_ts} do
      assert ts == naive_ts |> Calendar.strftime("%c.%f") |> String.trim_trailing("0")
    end

    @tag format: :second
    test "second", %{ts: ts, naive_ts: naive_ts} do
      seconds = naive_ts
        |> DateTime.from_naive!("Etc/UTC")
        |> DateTime.to_unix(:second)
      assert ts == seconds
    end

    @tag format: :millisecond
    test "millisecond", %{ts: ts, naive_ts: naive_ts} do
      seconds = naive_ts
        |> DateTime.from_naive!("Etc/UTC")
        |> DateTime.to_unix(:millisecond)
      assert ts == seconds
    end
  end
end
