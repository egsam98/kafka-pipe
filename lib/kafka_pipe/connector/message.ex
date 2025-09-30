defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t()
    field :key, iodata()
    field :value, iodata()
    field :from, pid(), enforce: true
    field :metadata, map()
  end
end
