defmodule KafkaPipe.Connector.Supervisor do
  use Parent.GenServer
  use TypedStruct

  import KafkaPipe.Connector
  require Logger

  defmodule Conn do
    use TypedStruct

    typedstruct module: Member do
      field :mod, module(), enforce: true
      field :config, %{String.t() => any()}, enforce: true
    end

    typedstruct do
      field :name, atom(), enforce: true
      field :running?, boolean(), default: false
      field :source, Member.t(), enforce: true
      field :sink, Member.t(), enforce: true
    end
  end

  @type state :: %{atom() => Conn.t()}

  @dir KafkaPipe.connector_dir()
  @restart_delay 3_000

  @spec start_link(any()) :: Supervisor.on_start()
  def start_link(_args), do:
    Parent.GenServer.start_link(__MODULE__, nil, name: __MODULE__, max_restarts: :infinity)

  @spec register(atom(), module(), Keyword.t(), module(), Keyword.t()) ::
    {:ok, Conn.t()} | {:error, :exists | map() | any()}
  def register(name, source_mod, source_opts, sink_mod, sink_opts) when is_atom(name), do:
    GenServer.call(__MODULE__, {:register, name, source_mod, source_opts, sink_mod, sink_opts})

  @spec start_child(atom()) :: {:ok, Conn.t()} | :ignore | {:error, reason}
    when reason: :not_found | {:start, String.Chars.t() | any()} | {:already_started, pid()} | any()
  def start_child(name) when is_atom(name), do: GenServer.call(__MODULE__, {:start_child, name})

  @spec stop_child(atom()) :: {:ok, Conn.t()} | {:error, :not_found | String.t() | any()}
  def stop_child(name) when is_atom(name), do: GenServer.call(__MODULE__, {:stop_child, name})

  @spec delete_child(atom()) :: {:ok, Conn.t()} | {:error, :not_found | String.t() | any()}
  def delete_child(name) when is_atom(name), do: GenServer.call(__MODULE__, {:delete_child, name})

  @spec connectors :: [Conn.t()]
  def connectors, do: GenServer.call(__MODULE__, :connectors)

  @spec member_pid(atom(), KafkaPipe.Connector.member()) :: pid() | nil
  def member_pid(name, member) do
    case Parent.Client.child_pid(__MODULE__, :"#{name}.#{member}") do
      {:ok, pid} when is_pid(pid) -> pid
      _ -> nil
    end
  end

  @impl true
  def init(nil) do
    Logger.metadata(name: __MODULE__)

    File.mkdir_p!(@dir)
    state = @dir
      |> File.ls!()
      |> Enum.reject(fn path -> Path.extname(path) in [".source", ".sink"] end)
      |> Enum.map(fn path ->
        {:ok, ref} = Path.join(@dir, path)
            |> to_charlist()
            |> :dets.open_file()
        [conn: %{name: name, running?: running?} = data] = :dets.lookup(ref, :conn)
        :ok = :dets.close(ref)

        if running?, do: send(self(), {:start_child, name})
        {name, struct!(Conn, data)}
      end)
      |> Map.new()
    {:ok, state}
  end

  @impl true
  def handle_call({:register, name, _, _, _, _}, _from, state) when is_map_key(state, name), do:
    {:reply, {:error, :exists}, state}

  @impl true
  def handle_call({:register, name, source_mod, source_cfg, sink_mod, sink_cfg}, _from, state) do
    with {:ok, source_cfg} <- Module.safe_concat(source_mod, Config).new(source_cfg),
      {:ok, sink_cfg} <- Module.safe_concat(sink_mod, Config).new(sink_cfg),
      conn <- %Conn{
        name: name,
        source: %Conn.Member{mod: source_mod, config: source_cfg},
        sink: %Conn.Member{mod: sink_mod, config: sink_cfg}
      },
      :ok <- persist_conn(name, conn)
    do
      Logger.info("Register connector \"#{name}\"")
      {:reply, {:ok, conn}, Map.put(state, name, conn)}
    else
      {:error, reason} -> {:reply, {:error, reason}, state}
    end
  end

  @impl true
  def handle_call({:start_child, name}, _from, state) when is_map_key(state, name) do
    {res, state} = do_start_child(name, state)
    {:reply, res, state}
  end

  @impl true
  def handle_call({:stop_child, name}, _from, state) when is_map_key(state, name) do
    {res, state} = do_stop_child(name, state)
    {:reply, res, state}
  end

  @impl true
  def handle_call({:delete_child, name}, _from, state) when is_map_key(state, name) do
    %{^name => %Conn{running?: running?} = conn} = state

    maybe_stop = if running?, do: do_stop_child(name, state), else: {{:ok, conn}, state}

    case maybe_stop do
      {{:ok, conn}, state} ->
        @dir
        |> Path.join(Atom.to_string(name))
        |> then(& [&1, &1 <> ".source", &1 <> ".sink"])
        |> Enum.each(fn path ->
          with {:error, reason} when reason != :enoent <- File.rm(path) do
            Logger.error("Remove file #{path}: #{reason}")
          end
        end)

        Logger.info("Delete connector \"#{name}\"")
        {:reply, {:ok, conn}, Map.delete(state, name)}
      {res, state} ->
        {:reply, res, state}
    end
  end

  @impl true
  def handle_call(req, _from, state) when elem(req, 0) in [:start_child, :stop_child, :delete], do:
    {:reply, {:error, :not_found}, state}

  @impl true
  def handle_call(:connectors, _from, state), do: {:reply, Map.values(state), state}

  @impl true
  def handle_info({:start_child, name}, state) do
    state = case do_start_child(name, state) do
      {{:ok, _conn}, state} -> state
      {{:error, reason}, %{^name => %Conn{
          source: %Conn.Member{mod: source_mod},
          sink: %Conn.Member{mod: sink_mod}
        }} = state} ->
        Logger.error("Failed to autostart connector \"#{name}\": #{inspect(reason)} " <>
          "(#{humanize(source_mod, true)} => #{humanize(sink_mod, true)})")
        Map.update!(state, name, fn conn -> %Conn{conn | running?: false } end)
    end
    {:noreply, state}
  end

  @impl true
  def handle_info({:restart, stopped_children}, state) do
    Parent.return_children(stopped_children)
    {:noreply, state}
  end

  @impl true
  def handle_stopped_children(stopped_children, state) do
    Process.send_after(self(), {:restart, stopped_children}, @restart_delay)
    names = Map.keys(stopped_children)
    Logger.info("Restarting stopped children #{inspect(names)} in #{@restart_delay}ms")
    {:noreply, state}
  end

  @impl true
  def terminate(_reason, _state),do: Parent.shutdown_all()

  @spec do_start_child(atom(), state()) :: {result, state()} when result: {:ok, Conn.t()} | {:error, any()}
  defp do_start_child(name, state) do
    %{^name => %Conn{
      source: %Conn.Member{mod: source_mod, config: source_cfg},
      sink: %Conn.Member{mod: sink_mod, config: sink_cfg},
    } = conn} = state

    source_name = :"#{name}.source"
    sink_name = :"#{name}.sink"
    source_opts = [name: source_name, config: source_cfg]
    sink_opts = [name: sink_name, config: sink_cfg, subscribe_to: [source_name]]

    source_spec = %{
      id: source_name,
      start: {source_mod, :start_link, [source_opts]},
      shutdown: 30_000,
      restart: :temporary,
      ephemeral?: true
    }
    sink_spec = %{
      id: sink_name,
      start: {sink_mod, :start_link, [sink_opts]},
      shutdown: 30_000,
      restart: :temporary,
      ephemeral?: true
    }

    with {:ok, _} <- Parent.start_child(source_spec),
      {:sink, {:ok, _}} <- {:sink, Parent.start_child(sink_spec)},
      conn <- %Conn{conn | running?: true},
      :ok <- persist_conn(name, conn)
    do
      Logger.info("Start connector \"#{name}\" (#{humanize(source_mod, true)} => #{humanize(sink_mod, true)})")
      {{:ok, conn}, %{state | name => conn}}
    else
      {:sink, res} ->
        with :error <- Parent.shutdown_child(source_name) do
          Logger.error("Failed to stop source #{source_name} after sink's #{sink_name} crash")
        end
        {res, state}
      res ->
        {res, state}
    end
  end

  @spec do_stop_child(atom(), state()) :: {result, state()} when result: {:ok, Conn.t()} | {:error, String.t()}
  defp do_stop_child(name, state) do
    %{^name => conn} = state

    with {:ok, _} <- Parent.shutdown_child(:"#{name}.sink"),
      {:ok, _} <- Parent.shutdown_child(:"#{name}.source"),
      conn <- %Conn{conn | running?: false},
      :ok <- persist_conn(name, conn)
    do
      Logger.info("Stop connector \"#{name}\"")
      {{:ok, conn}, %{state | name => conn}}
    else
      :error -> {{:error, "Failed to stop"}, state}
      {:error, reason} -> {{:error, reason}, state}
    end
  end

  @spec persist_conn(atom(), Conn.t()) :: :ok | {:error, any()}
  defp persist_conn(name, conn) do
    File.mkdir_p!(@dir)
    path = Path.join(@dir, Atom.to_string(name))

    with {:ok, file} <- :dets.open_file(name, file: to_charlist(path)),
      :ok <- :dets.insert(file, [conn: Map.from_struct(conn)]),
      :ok <- :dets.close(file)
    do
      :ok
    end
  end
end
