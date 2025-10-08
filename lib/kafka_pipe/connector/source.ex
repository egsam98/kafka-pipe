defmodule KafkaPipe.Connector.Source do
  use GenServer
  use TypedStruct

  alias KafkaPipe.Connector.Message
  require Logger

  typedstruct module: State do
    field :module, atom(), enforce: true
    field :inner, any(), enforce: true
  end

  @callback init(name :: atom(), config :: %{atom() => any()}) :: {:ok, state :: any()} | {:error, reason :: any()}

  @callback handle_poll(count :: pos_integer(), state) :: {:ok, [Message.t()], state} | {:error, reason :: any()}
  when state: any()

  @callback handle_ack(messages :: [Message.t()], state) :: state when state: any()

  @callback handle_call(request :: term(), GenServer.from(), state :: term()) ::
    {:reply, reply, new_state}
    | {:reply, reply, new_state,
      timeout() | :hibernate | {:continue, continue_arg :: term()}}
    | {:noreply, new_state}
    | {:noreply, new_state,
      timeout() | :hibernate | {:continue, continue_arg :: term()}}
    | {:stop, reason, reply, new_state}
    | {:stop, reason, new_state}
  when reply: term(), new_state: term(), reason: term()

  @callback handle_cast(request :: term(), state :: term()) ::
    {:noreply, new_state}
    | {:noreply, new_state,
      timeout() | :hibernate | {:continue, continue_arg :: term()}}
    | {:stop, reason :: term(), new_state}
  when new_state: term()

  @callback handle_info(msg :: :timeout | term(), state :: term()) ::
    {:noreply, new_state}
    | {:noreply, new_state,
      timeout() | :hibernate | {:continue, continue_arg :: term()}}
    | {:stop, reason :: term(), new_state}
  when new_state: term()

  @callback terminate(reason, state :: term()) :: term() when reason: :normal | :shutdown | {:shutdown, term()} | term()

  @optional_callbacks handle_call: 3, handle_cast: 2, handle_info: 2, terminate: 2

  @type start_opt :: {:name, atom()} | {:config, %{atom() => any()}}

  @spec start_link(module(), [start_opt()]) :: GenServer.on_start()
  def start_link(module, opts),
    do: GenServer.start_link(__MODULE__, {module, opts}, name: Keyword.fetch!(opts, :name))

  @spec poll(GenServer.server(), non_neg_integer()) :: [Message.t()]
  def poll(server, count), do: GenServer.call(server, {:poll, count})

  @spec ack(GenServer.server(), [Message.t()]) :: :ok
  def ack(server, messages), do: GenServer.call(server, {:ack, messages})

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    cfg = Keyword.fetch!(opts, :config)

    Logger.metadata(name: name)
    Logger.info("Start")

    case module.init(name, cfg) do
      {:ok, state} -> {:ok, %State{module: module, inner: state}}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_call({:poll, count}, _from, %State{module: mod, inner: inner} = state) do
    case mod.handle_poll(count, inner) do
      {:ok, messages, inner} -> {:reply, messages, %State{state | inner: inner}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  @impl true
  def handle_call({:ack, messages}, _from, %State{module: mod, inner: inner} = state) do
    inner = mod.handle_ack(messages, inner)
    {:reply, :ok, %State{state | inner: inner}}
  end

  @impl true
  def handle_call(request, from, %State{module: mod, inner: inner} = state) do
    result = mod.handle_call(request, from, inner)
    update_inner_state(result, state)
  end

  @impl true
  def handle_cast(request, %State{module: mod, inner: inner} = state) do
    result = mod.handle_cast(request, inner)
    update_inner_state(result, state)
  end

  @impl true
  def handle_info(request, %State{module: mod, inner: inner} = state) do
    result = mod.handle_info(request, inner)
    update_inner_state(result, state)
  end

  @impl true
  def terminate(reason, %State{module: mod, inner: inner}) do
    Logger.info("Stop #{inspect(reason)}")
    if function_exported?(mod, :terminate, 2), do: mod.terminate(reason, inner)
  end

  defp update_inner_state(result_tuple, state) do
    last = tuple_size(result_tuple) - 1
    put_elem(result_tuple, last, %State{state | inner: elem(result_tuple, last)})
  end
end
