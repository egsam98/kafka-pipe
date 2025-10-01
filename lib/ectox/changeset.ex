defmodule Ectox.Changeset do
  @empty_values [nil | Ecto.Changeset.empty_values()]

  defmacro __using__(_opts) do
    quote do
      import Ecto.Changeset, except: [cast: 3]
      import Ectox.Changeset
    end
  end

  @spec cast(
    Ecto.Schema.t() | Ecto.Changeset.t() | {map(), map()},
    %{binary() => any()} | %{atom => term} | :invalid,
    [atom()],
    Keyword.t()
  ) :: Ecto.Changeset.t()
  def cast(data, params, permitted, opts \\ []), do:
    Ecto.Changeset.cast(data, params, permitted, opts ++ [empty_values: @empty_values])

  @spec validate_list(Ecto.Changeset.t(), atom(), [fun() | {fun(), [any()]}]) :: Ecto.Changeset.t()
  def validate_list(%Ecto.Changeset{types: types} = changeset, field, funs_args) do
    {:array, elem_type} = types[field]
    Ecto.Changeset.validate_change(changeset, field, fn _, list ->
      Enum.flat_map(list, fn elem ->
        changeset = Ecto.Changeset.cast({%{}, %{elem: elem_type}}, %{elem: elem}, [:elem])
        %Ecto.Changeset{errors: errors} = Enum.reduce(funs_args, changeset, fn fun_args, changeset ->
          case fun_args do
            {fun, args} ->
              apply(fun, [changeset, :elem | args])
            fun ->
              apply(fun, [changeset, :elem, []])
          end
        end)

        Enum.map(errors, fn {:elem, {msg, opts}} -> {field, {msg, opts}} end)
      end)
    end)
  end

  @type error :: %{atom() => [String.t()] | error()}

  @spec errors(Ecto.Changeset.t()) :: error()
  def errors(changeset), do: Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
    Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
      opts
      |> Keyword.fetch!(String.to_existing_atom(key))
      |> to_string()
    end)
  end)
end
