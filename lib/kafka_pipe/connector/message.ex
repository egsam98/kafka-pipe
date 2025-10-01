defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t() | nil
    field :key, iodata()
    field :value, iodata()
    field :metadata, map() | nil
  end
end
