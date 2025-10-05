defmodule KafkaPipe.Connector.Source.Postgres do
  @behaviour KafkaPipe.Connector.Source
  use TypedStruct

  alias KafkaPipe.Connector.Source
  alias KafkaPipe.Connector.{MemberDB, Message}
  alias KafkaPipe.Pg.Lsn
  alias __MODULE__.{Config, Internal}
  require Logger

  typedstruct module: State do
    field :name, atom(), enforce: true
    field :internal, pid(), enforce: true
    field :buffer, [Message.t()], default: []
  end

  @spec start_link([Source.start_opt()]) :: GenServer.on_start() | {:error, {:start, reason}}
    when reason: Postgrex.Error.t() | any()
  def start_link(opts) do
    with {:error, %Postgrex.Error{} = error} <- Source.start_link(__MODULE__, opts) do
      {:error, {:start, error}}
    end
  end

  @spec push(pid(), Message.t()) :: :ok
  def push(pid, message), do: Source.cast(pid, {:push, message})

  @impl true
  def init(name, cfg) do
    case Config.new(cfg) do
      {:ok, cfg} ->
        MemberDB.open(name)
        commit_lsn = case MemberDB.get(name, :commit_lsn) do
          [] -> %Lsn{}
          [commit_lsn] -> commit_lsn
        end

        case Internal.start_link(name: :"#{name}.pgrepl", config: cfg, start_lsn: commit_lsn) do
          {:ok, pid} -> {:ok, %State{name: name, internal: pid}}
          {:error, reason} -> {:error, {:start, reason}}
        end
      {:error, errors} -> {:error, {:start, errors}}
    end
  end

  @impl true
  def handle_demand(demand, %State{buffer: buffer} = state) do
    messages = buffer
      |> Enum.take(-demand)
      |> Enum.reverse()
    {:ok, messages, state}
  end

  @impl true
  def handle_ack(messages, %State{name: name, buffer: buffer, internal: internal} = state) do
    %Message{metadata: %{lsn: lsn}} = List.last(messages)
    MemberDB.put(name, :commit_lsn, lsn)
    Internal.commit_lsn(internal, lsn)
    {:ok, %{state | buffer: buffer -- messages}}
  end

  @impl true
  def handle_cast({:push, msg}, %State{buffer: buffer} = state), do:
    {:noreply, [], %{state | buffer: [msg | buffer]}}

  @impl true
  def handle_info({:EXIT, internal, reason}, %State{internal: internal} = state), do: {:stop, reason, state}

  @impl true
  def terminate(_reason, %State{name: name}), do: MemberDB.close(name)
end
