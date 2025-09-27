defmodule KafkaPipe.NimbleOptions do
  def brod_endpoint, do: {:custom, __MODULE__, :brod_endpoint, []}

  def brod_endpoint(str) do
    with true <- is_binary(str),
      [host, port] <- String.split(str, ":"),
      {port, ""} when port > 0 <- Integer.parse(port) do
      {:ok, {host, port}}
    else
      _ -> {:error, "invalid host:port"}
    end
  end
end

defimpl String.Chars, for: NimbleOptions.ValidationError do
  def to_string(error), do: Exception.message(error)
end
