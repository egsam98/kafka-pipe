defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t()
    field :key, binary()
    field :value, binary()
    field :from, pid(), enforce: true
    field :metadata, map()
  end
end
