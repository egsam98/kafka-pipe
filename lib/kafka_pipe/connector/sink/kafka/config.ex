defmodule KafkaPipe.Connector.Sink.Kafka.Config do
  use TypedEctoSchema
  use KafkaPipe.Ecto.Ctor

  import Ecto.Changeset

  defmodule Topic do
    use TypedEctoSchema

    @primary_key false
    typed_embedded_schema do
      field :name, :string, null: false
      field(:num_partitions, :integer, default: 1) :: pos_integer()
      field(:replication_factor, :integer, default: 1) :: pos_integer()
      field :configs, :map, default: %{}
    end

    def changeset(t, params), do:
      cast(t, params, __MODULE__.__schema__(:fields), empty_values: [nil | empty_values()])
      |> validate_required([:name])
      |> validate_number(:num_partitions, greater_than: 0)
      |> validate_number(:replication_factor, greater_than: 0)
  end

  @primary_key false
  typed_embedded_schema null: false do
    field :endpoints, {:array, :string}
    field(:batch_size, :integer, default: 10_000) :: pos_integer()
    field(:batch_timeout, :integer, default: 5_000) :: pos_integer()
    embeds_many :topics, Topic
  end

  @spec changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()
  def changeset(t, params) do
    cast(t, params, [:endpoints, :batch_size, :batch_timeout], empty_values: [nil | empty_values()])
    |> validate_required([:endpoints, :batch_size, :batch_timeout])
    |> validate_length(:endpoints, min: 1)
    |> validate_change(:endpoints, fn _, endpoints ->
      endpoints
      |> Enum.with_index()
      |> Enum.flat_map(fn {endpoint, i} -> if endpoint =~ ~r/^[^\:]+:\d{1,5}$/,
        do: [],
        else: [{:endpoints, {"#{i}nth is not {host}:{port}", validation: :format}}]
      end)
    end)
    |> validate_number(:batch_size, greater_than: 0)
    |> validate_number(:batch_timeout, greater_than: 0)
    |> cast_embed(:topics)
    |> validate_length(:topics, min: 1)
  end
end
