defmodule KafkaPipe.Connector.Sink do
  use GenStage
  use TypedStruct

  alias KafkaPipe.Connector.{Source, Message}
  alias __MODULE__.Batch
  require Logger

  @callback init(name :: atom(), config :: %{atom() => any()}) :: {:ok, Batch.t(), state :: any()} | {:error, reason :: any()}

  @callback handle_messages(messages :: [Message.t()], source :: pid(), state) :: {:ok, state} | {:error, reason :: any()}
  when state: any()

  typedstruct module: State do
    field :module, atom(), enforce: true
    field :inner, any(), enforce: true
    field :batch, Batch.t(), enforce: true
    field :source, GenStage.from()
    field :timer, reference()
  end

  @type start_opt() :: {:name, atom()}
    | {:subscribe_to, [atom() | pid() | {GenServer.server(), GenStage.subscription_options()}]}
    | {:config, %{atom() => any()}}

  @spec start_link(module(), [start_opt()]) :: GenServer.on_start()
  def start_link(module, opts) do
    GenStage.start_link(__MODULE__, {module, opts}, name: Keyword.fetch!(opts, :name))
  end

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    cfg = Keyword.fetch!(opts, :config)
    subscribe_to = Keyword.fetch!(opts, :subscribe_to)

    Logger.metadata(name: name)
    Logger.info("Start")

    case module.init(name, cfg) do
      {:ok, batch, state} ->
        state = %State{module: module, inner: state, batch: batch}
        {:consumer, state, subscribe_to: subscribe_to}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_subscribe(:producer, _opts, from, %State{batch: %Batch{timeout: timeout}} = state) do
    timer = Process.send_after(self(), :timeout, timeout)
    {:manual, %State{state | timer: timer, source: from}}
  end

  @impl true
  def handle_events(messages, {source, _}, %State{
    module: mod,
    inner: inner,
    batch: %Batch{size: size},
    timer: timer
  } = state) do
    case mod.handle_messages(messages, source, inner) do
      {:ok, inner} ->
        timer = if length(messages) >= size do
          if timer, do: Process.cancel_timer(timer)
          send(self(), :timeout)
          nil
        else
          timer
        end

        {:noreply, [], %State{state | inner: inner, timer: timer}}
      {:error, reason} ->
        {:stop, reason, state}
    end
  end

  @impl true
  def handle_info(:timeout, %State{batch: %Batch{size: size, timeout: timeout}, source: source} = state) do
    Source.ask(source, size)
    timer = Process.send_after(self(), :timeout, timeout)
    {:noreply, [], %State{state | timer: timer}}
  end

  @impl true
  def terminate(reason, _state), do: Logger.info("Stop #{inspect(reason)}")
end
