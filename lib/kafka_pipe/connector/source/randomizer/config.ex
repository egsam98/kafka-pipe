defmodule KafkaPipe.Connector.Source.Randomizer.Config do
  use TypedEctoSchema
  use Ectox.{Ctor, Changeset}

  @primary_key false
  typed_embedded_schema do
    field :topic, :string
    field :template, :string
  end

  @impl true
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields)
    cast(t, params, fields)
    |> validate_required(fields)
  end
end
