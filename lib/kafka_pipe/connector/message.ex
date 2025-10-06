defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t()
    field :key, iodata(), default: ""
    field :value, iodata(), default: ""
    field :metadata, map()
  end

  defimpl Jason.Encoder do
    def encode(value, opts) do
      value
      |> Map.from_struct()
      |> Map.update!(:key, &IO.iodata_to_binary/1)
      |> Map.update!(:value, &IO.iodata_to_binary/1)
      |> Jason.Encode.map(opts)
    end
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
