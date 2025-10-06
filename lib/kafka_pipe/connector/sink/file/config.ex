defmodule KafkaPipe.Connector.Sink.File.Config do
  use TypedEctoSchema
  use Ectox.{Changeset, Ctor}

  alias KafkaPipe.Connector.Sink.Batch

  @primary_key false
  typed_embedded_schema null: false do
    field :path, :string
    embeds_one :batch, Batch, on_replace: :delete, defaults_to_struct: true
  end

  @impl true
  def changeset(t, params) do
    cast(t, params, [:path])
    |> validate_required([:path])
    |> cast_embed(:batch, required: true)
  end
end
