defmodule KafkaPipe.Connector.Sink.Kafka.Config do
  use TypedEctoSchema
  use Ectox.{Changeset, Ctor}

  alias KafkaPipe.Connector.Sink.Batch

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
    embeds_one :batch, Batch, on_replace: :delete, defaults_to_struct: true
    embeds_many :topics, Topic, on_replace: :delete
  end

  @impl Ectox.Ctor
  def changeset(t, params) do
    fields = __MODULE__.__schema__(:fields) -- [:topics, :batch]
    cast(t, params, fields)
    |> validate_required(fields)
    |> validate_length(:endpoints, min: 1)
    |> validate_list(:endpoints, [
      {&validate_format/4, [~r/^[^\:]+:\d{1,5}$/, [message: "must match {hostname}:{port} format"]]}
    ])
    |> cast_embed(:batch, required: true)
    |> cast_embed(:topics)
  end
end
