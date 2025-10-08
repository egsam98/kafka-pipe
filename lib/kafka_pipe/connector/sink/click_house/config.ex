defmodule KafkaPipe.Connector.Sink.ClickHouse.Config do
  use TypedEctoSchema
  use Ectox.{Ctor, Changeset}

  alias KafkaPipe.Connector.Sink.Batch

  @primary_key false
  typed_embedded_schema null: false do
    field :scheme, Ecto.Enum, values: [:http, :https], default: :http
    field :hostname, :string, default: "localhost"
    field :port, :integer, default: 8123
    field :database, :string, default: "default"
    field :username, :string
    field :password, :string
    field :tables, {:array, :string}
    embeds_one :batch, Batch, on_replace: :delete, defaults_to_struct: true
  end

  @impl true
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields) -- [:batch]
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_number(:port, greater_than: 0)
    |> validate_length(:tables, min: 1)
    |> validate_list(:tables, [
      {&validate_required/2, []}
    ])
    |> cast_embed(:batch, required: true)
  end
end
