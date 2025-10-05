defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t() | nil
    field :key, iodata()
    field :value, iodata()
    field :metadata, map() | nil
  end
end

defmodule KafkaPipe.Connector.ConfigError do
  defexception [:source, :sink]

  @type t :: %__MODULE__{source: errors(), sink: errors()}
  @type errors :: %{atom() => [String.t()]}

  @impl true
  def message(%__MODULE__{source: source_errs, sink: sink_errs}) do
    "#{inspect(source_errs)}, #{sink_errs}"
  end
end
