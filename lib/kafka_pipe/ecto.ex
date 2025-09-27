defmodule KafkaPipe.Ecto.Ctor do
  defmacro __using__(_opts) do
    quote do
      @before_compile KafkaPipe.Ecto.Ctor.Injector
    end
  end

  defmodule Injector do
    defmacro __before_compile__(_env) do
      quote do

        @spec new(map()) :: {:ok, __MODULE__.t()} | {:error, map()}
        def new(params) do
          with changeset <- __MODULE__.changeset(%__MODULE__{}, params),
            {:error, changeset} <- Ecto.Changeset.apply_action(changeset, :new)
          do
            details = Ecto.Changeset.traverse_errors(changeset, fn {msg, opts} ->
              Regex.replace(~r"%{(\w+)}", msg, fn _, key ->
                opts
                |> Keyword.fetch!(String.to_existing_atom(key))
                |> to_string()
              end)
            end)
            {:error, details}
          end
        end

      end
    end
  end

end
