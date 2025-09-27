defimpl String.Chars, for: PID do
  def to_string(pid), do: pid
    |> :erlang.pid_to_list()
    |> then(&"PID#{&1}")
end

defimpl Phoenix.HTML.Safe, for: PID do
  defdelegate to_iodata(args), to: String.Chars, as: :to_string
end

defimpl Phoenix.Param, for: PID do
  defdelegate to_param(args), to: String.Chars, as: :to_string
end

defimpl Jason.Encoder, for: PID do
  def encode(pid, opts), do: pid |> to_string() |> Jason.Encode.string(opts)
end
