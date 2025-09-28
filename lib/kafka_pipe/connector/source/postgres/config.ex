defmodule KafkaPipe.Connector.Source.Postgres.Config do
  use TypedEctoSchema
  use KafkaPipe.Ecto.Ctor

  import Ecto.Changeset

  @primary_key false
  typed_embedded_schema do
    field :hostname, :string, null: false, default: "localhost"
    field(:port, :integer, default: 5432) :: pos_integer()
    field :database, :string, null: false, default: "postgres"
    field :username, :string,  null: false, default: "postgres"
    field :password, :string, null: false, default: "postgres"
    field :tables, {:array, :string}, null: false
    field :publication, :string, null: false, default: "kafka_pipe"
    field :slot, :string, null: false, default: "kafka_pipe"
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields)
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_number(:port, greater_than: 0)
    |> validate_length(:tables, min: 1)
  end
end
