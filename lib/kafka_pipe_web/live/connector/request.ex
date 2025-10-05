defmodule KafkaPipeWeb.Live.Connector.Request do
  def decode_config(%Ecto.Changeset{changes: changes} = changeset, from_key, to_key) when is_map_key(changes, from_key) do
    case Yaml.decode(changes[from_key]) do
      {:ok, cfg} when is_map(cfg) -> Ecto.Changeset.put_change(changeset, to_key, cfg)
      {:ok, _} -> Ecto.Changeset.add_error(changeset, from_key, "YAML map required")
      {:error, %Yaml.ParsingError{message: msg}} -> Ecto.Changeset.add_error(changeset, from_key, msg)
    end
  end

  def decode_config(changeset, _from_key, _to_key), do: changeset
end

defmodule KafkaPipeWeb.Live.Connector.Request.Create do
  use TypedEctoSchema
  use Ectox.Changeset

  import KafkaPipeWeb.Live.Connector.Request

  @primary_key false
  typed_embedded_schema null: false do
    field :name, :string
    field(:source_module, Ectox.Module) :: module()
    field :source_config, :string
    field :_source_config, :map
    field(:sink_module, Ectox.Module) :: module()
    field :sink_config, :string
    field :_sink_config, :map
  end

  def changeset(params) do
    fields = __MODULE__.__schema__(:fields) -- [:_source_config, :_sink_config]
    %__MODULE__{}
    |> cast(params, fields)
    |> validate_required(fields)
    |> decode_config(:source_config, :_source_config)
    |> decode_config(:sink_config, :_sink_config)
  end
end

defmodule KafkaPipeWeb.Live.Connector.Request.Update do
  use TypedEctoSchema
  use Ectox.Changeset

  import KafkaPipeWeb.Live.Connector.Request

  @primary_key false
  typed_embedded_schema null: false do
    field :name, :string
    field :source_config, :string
    field :_source_config, :map
    field :sink_config, :string
    field :_sink_config, :map
  end

  def changeset(params) do
    fields = __MODULE__.__schema__(:fields) -- [:_source_config, :_sink_config]
    %__MODULE__{}
    |> cast(params, fields)
    |> validate_required(fields)
    |> decode_config(:source_config, :_source_config)
    |> decode_config(:sink_config, :_sink_config)
  end
end
