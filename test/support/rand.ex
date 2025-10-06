defmodule Test.Rand do
  @alphabet Enum.concat([?a..?z, [?_]])
  @length 10

  def rand, do: rand(:string)

  def rand(:string), do:
    Stream.repeatedly(fn -> Enum.random(@alphabet) end)
    |> Enum.take(@length)
    |> List.to_string()

  def rand(:atom), do: rand(:string) |> String.to_atom()

  def rand(n) when is_integer(n) and n > 0, do: Enum.random(0..n)

  def rand(:pid), do: IEx.Helpers.pid(0, rand(999), rand(999))
end
