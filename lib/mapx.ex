defmodule Mapx do
  @spec from_nested_struct(map() | struct()) :: map()
  def from_nested_struct(data) when is_map(data) do
    data
    |> Map.delete(:__struct__)
    |> Map.new(fn {k, v} -> {k, from_nested_struct(v)} end)
  end

  def from_nested_struct(data) when is_list(data),
    do: Enum.map(data, &from_nested_struct/1)

  def from_nested_struct(data), do: data
end
