defimpl String.Chars, for: DBConnection.ConnectionError do
  def to_string(%DBConnection.ConnectionError{message: msg}), do: msg
end

defimpl String.Chars, for: Postgrex.Error do
  def to_string(error), do: Postgrex.Error.message(error)
end

defmodule KafkaPipe.Pg.Lsn do
  use TypedStruct

  alias KafkaPipe.Pg.Lsn

  typedstruct do
    field :file, non_neg_integer(), default: 0
    field :offset, non_neg_integer(), default: 0
  end

  def from_tuple({file, offset}), do: %Lsn{file: file, offset: offset}

  def to_int64(%Lsn{file: file, offset: offset}), do: Postgrex.PgOutput.Lsn.encode_int64({file, offset})

  defimpl String.Chars do
    def to_string(%Lsn{file: file, offset: offset}) do
      Postgrex.PgOutput.Lsn.encode_string({file, offset})
    end
  end
end
