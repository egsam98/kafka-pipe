defmodule KafkaPipe.Connector.Source do
  use GenStage
  use TypedStruct

  alias KafkaPipe.Connector.Message
  require Logger

  typedstruct module: State do
    field :module, atom(), enforce: true
    field :inner, any(), enforce: true
  end

  @callback init(name :: atom(), config :: %{atom() => any()}) :: {:ok, state :: any()} | {:error, reason :: any()}

  @callback handle_demand(demand :: pos_integer(), state) :: {:ok, [Message.t()], state} | {:error, reason :: any()}
  when state: any()

  @callback handle_ack(messages :: [Message.t()], state) :: {:ok, state} when state: any()

  @callback handle_cast(request :: term, state :: term) ::
    {:noreply, [event], new_state}
    | {:noreply, [event], new_state, :hibernate}
    | {:stop, reason :: term, new_state}
  when new_state: term, event: term

  @callback handle_call(request :: term, from :: GenServer.from(), state :: term) ::
    {:reply, reply, [event], new_state}
    | {:reply, reply, [event], new_state, :hibernate}
    | {:noreply, [event], new_state}
    | {:noreply, [event], new_state, :hibernate}
    | {:stop, reason, reply, new_state}
    | {:stop, reason, new_state}
  when reply: term, new_state: term, reason: term, event: term

  @callback handle_info(message :: term, state :: term) ::
    {:noreply, [event], new_state}
    | {:noreply, [event], new_state, :hibernate}
    | {:stop, reason :: term, new_state}
  when new_state: term, event: term

  @callback terminate(reason, state :: any()) :: any() when reason: :normal | :shutdown | {:shutdown, any()} | any()

  @optional_callbacks handle_call: 3, handle_cast: 2, handle_info: 2, terminate: 2

  @type start_opt :: {:name, atom()} | {:config, %{atom() => any()}}

  @spec start_link(module(), [start_opt()]) :: GenServer.on_start()
  def start_link(module, opts) do
    GenStage.start_link(__MODULE__, {module, opts}, name: Keyword.fetch!(opts, :name))
  end

  @spec ack(GenStage.stage(), [Message.t()]) :: :ok
  def ack(stage, messages), do: GenStage.call(stage, {:ack, messages}, :infinity)

  @spec ask(GenStage.from(), non_neg_integer()) :: :ok | :noconnect | :nosuspend
  defdelegate ask(from, demand), to: GenStage

  @spec cast(GenStage.stage(), any()) :: :ok
  defdelegate cast(stage, args), to: GenStage

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    cfg = Keyword.fetch!(opts, :config)

    Logger.metadata(name: name)
    Logger.info("Start")

    case module.init(name, cfg) do
      {:ok, state} -> {:producer, %State{module: module, inner: state}, buffer_size: :infinity}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_demand(demand, %State{module: mod, inner: inner} = state) do
    case mod.handle_demand(demand, inner) do
      {:ok, messages, inner} -> {:noreply, messages, %State{state | inner: inner}}
      {:error, reason} -> {:stop, reason, state}
    end
  end

  @impl true
  def handle_call({:ack, messages}, _from, %State{module: mod, inner: inner} = state) do
    {atom, inner} = mod.handle_ack(messages, inner)
    {:reply, atom, [], %State{state | inner: inner}}
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
