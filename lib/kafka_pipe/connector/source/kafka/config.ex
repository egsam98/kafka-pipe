defmodule KafkaPipe.Connector.Source.Kafka.Config do
  use TypedEctoSchema
  use Ectox.{Changeset, Ctor}

  import Bitwise

  @primary_key false
  typed_embedded_schema null: false do
    field :endpoints, {:array, :string}
    field :topics, {:array, :string}
    field :fetch_max_bytes, :integer, default: 1 <<< 20 # 1 MB
  end

  @impl true
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields)
    t
    |> cast(params, fields)
    |> validate_required(fields)
    |> validate_length(:endpoints, min: 1)
    |> validate_list(:endpoints, [
      {&validate_format/4, [~r/^[^\:]+:\d{1,5}$/, [message: "must match {hostname}:{port} format"]]}
    ])
    |> validate_length(:topics, min: 1)
    |> validate_list(:topics, [
      {&validate_required/2, []}
    ])
    |> validate_number(:fetch_max_bytes, greater_than: 0)
  end
end
