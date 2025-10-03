defmodule Ectox.Module do
  use Ecto.Type

  @impl true
  def type, do: :any

  @impl true
  def cast(string) when is_binary(string) do
    {:ok, Module.safe_concat([string])}
  rescue
    ArgumentError -> {:error, message: "Module not found"}
  end

  @impl true
  def load(_term), do: :error
  @impl true
  def dump(_term), do: :error
end
