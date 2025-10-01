defmodule KafkaPipe.Connector.Sink.Kafka.Config do
  use TypedEctoSchema
  use Ectox.{Changeset, Ctor}

  defmodule Topic do
    use TypedEctoSchema
    use Ectox.Ctor

    @primary_key false
    typed_embedded_schema null: false do
      field :name, :string
      field(:num_partitions, :integer, default: 1) :: pos_integer()
      field(:replication_factor, :integer, default: 1) :: pos_integer()
      field :configs, :map, default: %{}
    end

    @impl Ectox.Ctor
    def changeset(t, params) do
      fields = __MODULE__.__schema__(:fields)
      cast(t, params, fields)
      |> validate_required(fields)
      |> validate_number(:num_partitions, greater_than: 0)
      |> validate_number(:replication_factor, greater_than: 0)
    end
  end

  @primary_key false
  typed_embedded_schema null: false do
    field :endpoints, {:array, :string}
    field(:batch_size, :integer, default: 10_000) :: pos_integer()
    field(:batch_timeout, :integer, default: 5_000) :: pos_integer()
    embeds_many :topics, Topic
  end

  @impl Ectox.Ctor
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields) -- [:topics]
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_length(:endpoints, min: 1)
    |> validate_list(:endpoints, [
      {&validate_format/4, [~r/^[^\:]+:\d{1,5}$/, [message: "must match {hostname}:{port} format"]]}
    ])
    |> validate_number(:batch_size, greater_than: 0)
    |> validate_number(:batch_timeout, greater_than: 0)
    |> cast_embed(:topics)
  end
end
