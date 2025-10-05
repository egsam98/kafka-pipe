defmodule Yaml do
  use Ecto.ParameterizedType

  defmodule ParsingError do
    defexception [:message]
  end

  @spec encode(any()) :: binary()
  defdelegate encode(input), to: Ymlr.Encode, as: :to_s!

  @type decode_opt :: {:map, boolean()}

  @spec decode(binary(), [decode_opt()]) :: {:ok, any()} | {:error, ParsingError.t()}
  def decode(input, opts \\ []) do
    require_map = Keyword.get(opts, :map, false)
    case YamlElixir.read_from_string(input) do
      {:ok, res} when require_map and not is_map(res) -> %ParsingError{message: "Map required"}
      {:ok, res} -> {:ok, res}
      {:error, %YamlElixir.ParsingError{message: message}} -> {:error, %ParsingError{message: message}}
    end
  end

  @impl true
  def init(opts) when is_list(opts), do: opts

  @impl true
  def type(opts), do: (if Keyword.has_key?(opts, :map), do: :map, else: :any)

  @impl true
  def cast(iodata, opts) do
    Yaml.decode(iodata, opts) |> dbg()
    with {:error, %ParsingError{message: msg}} <- Yaml.decode(iodata, opts) do
      {:error, message: msg}
    end
  end

  @impl true
  def load(_value, _loader, _params), do: :error

  @impl true
  def dump(nil, _dumper, _params), do: {:ok, ""}

  @impl true
  def dump(value, _dumper, _params), do: {:ok, encode(value)}

  @impl true
  def embed_as(_format, _params), do: :dump

  # @impl true
  # def equal?(iodata1, iodata2, _params), do: IO.iodata_to_binary(iodata1) == IO.iodata_to_binary(iodata2)
end
