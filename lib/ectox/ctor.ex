defmodule Ectox.Ctor do
  @callback changeset(Ecto.Schema.t(), map()) :: Ecto.Changeset.t()

  defmacro __using__(_opts) do
    quote do
      @before_compile Ectox.Ctor.Injector

      @behaviour Ectox.Ctor
    end
  end

  defmodule Injector do
    defmacro __before_compile__(_env) do
      quote do

        @spec new(map()) :: {:ok, __MODULE__.t()} | {:error, map()}
        def new(params) do
          changeset = __MODULE__.changeset(%__MODULE__{}, params)
          with {:error, changeset} <- Ecto.Changeset.apply_action(changeset, :new) do
            {:error, Ectox.Changeset.errors(changeset)}
          end
        end

      end
    end
  end
end
