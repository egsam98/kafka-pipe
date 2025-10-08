defmodule KafkaPipe.Connector do
  @source_modules [
    __MODULE__.Source.Postgres,
    __MODULE__.Source.Kafka,
    __MODULE__.Source.Randomizer,
  ]

  @sink_modules [
    __MODULE__.Sink.Kafka,
    __MODULE__.Sink.File,
    __MODULE__.Sink.ClickHouse,
  ]

  @type member() :: :source | :sink

  @spec modules(member()) :: [module()]
  def modules(:source), do: @source_modules

  @spec modules(member()) :: [module()]
  def modules(:sink), do: @sink_modules

  @spec humanize(module(), boolean()) :: String.t()
  def humanize(mod, keep_member \\ false) do
    parts = Module.split(mod)
    if keep_member,
      do: parts |> Enum.take(-2) |> Enum.join("."),
      else: parts |> List.last()
  end
end
