defmodule KafkaPipe.Connector.Source.Postgres.Config do
  use TypedEctoSchema
  use Ectox.{Changeset, Ctor}

  @timestamp_formats ~w(millisecond second rfc3339)a

  @type timestamp_format :: unquote(Enum.reduce(@timestamp_formats, &{:|, [], [&1, &2]}))

  @primary_key false
  typed_embedded_schema null: false do
    field :hostname, :string, default: "localhost"
    field(:port, :integer, default: 5432) :: pos_integer()
    field :database, :string, default: "postgres"
    field :username, :string, default: "postgres"
    field :password, :string, default: "postgres"
    field :tables, {:array, :string}
    field :publication, :string, default: "kafka_pipe"
    field :slot, :string, default: "kafka_pipe"
    field(:timestamp_format, Ecto.Enum, values: @timestamp_formats, null: false, default: :millisecond) :: timestamp_format()
  end

  @impl Ectox.Ctor
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields)
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_number(:port, greater_than: 0)
    |> validate_length(:tables, min: 1)
  end
end
