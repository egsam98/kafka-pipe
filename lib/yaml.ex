defmodule Yaml do
  defmodule ParsingError do
    defexception [:message]
    @type t :: %__MODULE__{message: String.t()}
  end

  @spec encode(any()) :: binary()
  defdelegate encode(input), to: Ymlr.Encode, as: :to_s!

  @type decode_opt :: {:map, boolean()}

  @spec decode(binary(), [decode_opt()]) :: {:ok, any()} | {:error, ParsingError.t()}
  def decode(input, opts \\ []) do
    require_map = Keyword.get(opts, :map, false)
    case YamlElixir.read_from_string(input) do
      {:ok, res} when require_map and not is_map(res) -> {:error, %ParsingError{message: "Map required"}}
      {:ok, res} -> {:ok, res}
      {:error, %YamlElixir.ParsingError{message: message}} -> {:error, %ParsingError{message: message}}
    end
  end
end
