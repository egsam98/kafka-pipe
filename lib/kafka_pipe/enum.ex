defmodule KafkaPipe.Enum do
  @spec walk(Enum.t(), ({any(), any()} | any() -> any())) :: Enum.t()
  def walk(enumerable, fun) when is_list(enumerable) do
    enumerable
    |> Enum.reduce([], fn elem, acc ->
      elem = if is_list(elem) or is_map(elem), do: walk(elem, fun), else: fun.(elem)
      [elem | acc]
    end)
    |> Enum.reverse()
  end

  def walk(enumerable, fun) when is_map(enumerable) do
    enumerable
    |> Map.delete(:__struct__)
    |> Enum.reduce(%{}, fn {key, value}, acc ->
      value = if is_list(value) or is_map(value), do: walk(value, fun), else: fun.({key, value})
      Map.put(acc, key, value)
    end)
  end
end
