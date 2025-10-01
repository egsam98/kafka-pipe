defmodule KafkaPipe.Connector.Source.Postgres do
  use GenStage
  use TypedStruct

  require Logger
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

    case Internal.start_link(name: :"#{name}.pgrepl", config: cfg, start_lsn: commit_lsn) do
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
    {:reply, :ok, [], %{state | buffer: buffer -- messages}}
  end

  @impl true
  def handle_info({:EXIT, internal, reason}, %State{internal: internal} = state), do: {:stop, reason, state}

  @impl true
  def terminate(reason, %State{name: name}) do
    MemberDB.close(name)
    Logger.info("Stop #{inspect(reason)}")
  end
end
