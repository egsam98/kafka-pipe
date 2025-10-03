defmodule KafkaPipeWeb.Live.Connector.NewConnector do
  use TypedEctoSchema
  use Ectox.Changeset

  @primary_key false
  typed_embedded_schema null: false do
    field(:name, :string) :: atom()
    field(:source_module, Ectox.Module) :: module()
    field(:source_config, :string)
    field(:sink_module, Ectox.Module) :: module()
    field(:sink_config, :string)
  end

  def changeset(params) do
    fields = __MODULE__.__schema__(:fields)
    %__MODULE__{}
    |> cast(params, fields)
    |> validate_required(fields)
    |> update_change(:name, &String.to_atom/1)
  end
end
