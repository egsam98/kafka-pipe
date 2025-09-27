defmodule KafkaPipe.Connector do
  @type member() :: :source | :sink

  @spec modules(member()) :: [module()]
  def modules(member) do
    member_str = member
      |> Atom.to_string()
      |> String.capitalize()
    KafkaPipe.modules()
      |> Stream.map(& {&1, Module.split(&1)})
      |> Stream.filter(fn {_, parts} -> Enum.at(parts, -2) == member_str end)
      |> Enum.map(fn {mod, _} -> mod end)
  end

  @spec humanize(module(), boolean()) :: String.t()
  def humanize(mod, keep_member \\ false) do
    parts = Module.split(mod)
    if keep_member,
      do: parts |> Enum.take(-2) |> Enum.join("."),
      else: parts |> List.last()
  end
end
