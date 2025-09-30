defmodule KafkaPipe.Connector.Message do
  use TypedStruct

  typedstruct do
    field :topic, String.t()
    field :key, iodata()
    field :value, iodata()
    field :from, pid(), enforce: true # TODO rm
    field :metadata, map()
  end
end
