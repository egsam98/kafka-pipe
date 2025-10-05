defmodule KafkaPipe.Connector.Sink.Batch do
  use TypedEctoSchema
  use Ectox.Changeset

  @primary_key false
  typed_embedded_schema null: false do
    field(:size, :integer, default: 10_000) :: pos_integer()
    field(:timeout, :integer, default: 5_000) :: pos_integer()
  end

  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields)
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_number(:size, greater_than: 0)
    |> validate_number(:timeout, greater_than: 0)
  end
end
