defmodule KafkaPipe.Connector.MemberDB do
  @spec open(atom()) :: :ok
  def open(name) when is_atom(name) do
    dir = KafkaPipe.connector_dir()
    File.mkdir_p!(dir)
    file = Path.join(dir, Atom.to_string(name))
    {:ok, _} = :dets.open_file(name, file: to_charlist(file), auto_save: 1_000)
    :ok
  end

  @spec get(atom(), atom()) :: [any()] | {:error, any()}
  def get(name, key) when is_atom(name) and is_atom(key) do
    with objs when is_list(objs) <- :dets.lookup(name, key) do
      for {_, value} <- objs, do: value
    end
  end

  @spec put(atom(), atom(), [any()]) :: :ok
  def put(name, key, values) when is_atom(name) and is_atom(key) and is_list(values) , do:
    :ok = :dets.insert(name, Enum.map(values, & {key, &1}))

  @spec put(atom(), atom(), any()) :: :ok
  def put(name, key, value), do: put(name, key, [value])

  @spec close(atom()) :: :ok
  def close(name), do: :ok = :dets.close(name)
end
