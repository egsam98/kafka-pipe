defmodule KafkaPipe.Connector.Sink do
  use GenServer
  use TypedStruct

  alias KafkaPipe.Connector.{Source, Message}
  alias __MODULE__.Batch
  require Logger

  @callback init(name :: atom(), config :: %{atom() => any()}) :: {:ok, Batch.t(), state :: any()} | {:error, reason :: any()}

  @callback handle_messages(messages :: [Message.t()], state) :: {:ok, state} | {:error, reason :: any()}
  when state: any()

  @callback terminate(reason, state :: any()) :: any() when reason: :normal | :shutdown | {:shutdown, any()} | any()

  @optional_callbacks terminate: 2

  typedstruct module: State do
    field :source, GenServer.server(), enforce: true
    field :module, atom(), enforce: true
    field :inner, any(), enforce: true
    field :batch, Batch.t(), enforce: true
  end

  @type start_opt() :: {:name, atom()} | {:source, GenServer.server()} | {:config, %{atom() => any()}}

  @spec start_link(module(), [start_opt()]) :: GenServer.on_start()
  def start_link(module, opts),
    do: GenServer.start_link(__MODULE__, {module, opts}, name: Keyword.fetch!(opts, :name))

  @impl true
  def init({module, opts}) do
    Process.flag(:trap_exit, true)
    name = Keyword.fetch!(opts, :name)
    cfg = Keyword.fetch!(opts, :config)
    source = Keyword.fetch!(opts, :source)

    Logger.metadata(name: name)
    Logger.info("Start")

    case module.init(name, cfg) do
      {:ok, %Batch{timeout: timeout} = batch, state} ->
        Process.send_after(self(), :timeout, timeout)
        state = %State{source: source, module: module, inner: state, batch: batch}
        {:ok, state}
      {:error, reason} -> {:stop, reason}
    end
  end

  @impl true
  def handle_info(:timeout, %State{
    batch: %Batch{size: size, timeout: timeout},
    source: source,
    module: mod,
    inner: inner
  } = state) do
    messages = Source.poll(source, size)

    case handle_messages(messages, source, mod, inner) do
      {:ok, inner} ->
        Process.send_after(
          self(),
           :timeout,
          (if length(messages) >= size, do: 0, else: timeout)
        )
        {:noreply, %State{state | inner: inner}}
      {:error, reason} -> {:stop, reason, state}
    end
  catch
    :exit, {reason, _} ->
      Logger.error("Failed to poll messages from #{source}: #{reason}")
      Process.send_after(self(), :timeout, timeout)
      {:noreply, state}
  end

  @impl true
  def terminate(reason, %State{module: mod, inner: inner}) do
    Logger.info("Stop #{inspect(reason)}")
    if function_exported?(mod, :terminate, 2), do: mod.terminate(reason, inner)
  end

  @spec handle_messages([Message.t()], GenServer.server(), module(), inner) :: {:ok, inner} | {:error, any()} when inner: any()
  defp handle_messages([], _source, _mod, inner), do: {:ok, inner}

  defp handle_messages(messages, source, mod, inner) do
    payload = Enum.filter(messages, fn %Message{topic: topic} -> topic end)
    cb_result = if length(payload) > 0,
      do: mod.handle_messages(payload, inner),
      else: {:ok, inner}

    with {:ok, inner} <- cb_result do
      :ok = Source.ack(source, messages)
      {:ok, inner}
    end
  end
end
