defmodule KafkaPipe do
  @moduledoc """
  KafkaPipe keeps the contexts that define your domain
  and business logic.

  Contexts are also responsible for managing your data, regardless
  if it comes from the database, an external API or others.
  """

  import Logger.Formatter

  @spec connector_dir() :: String.t()
  def connector_dir, do: Application.fetch_env!(:kafka_pipe, :connector_dir)

  @spec version() :: String.t()
  def version, do: Application.spec(:kafka_pipe, :vsn) || ""

  @spec modules() :: [module()]
  def modules, do: Application.spec(:kafka_pipe, :modules) || []

  @spec format_log(atom(), String.t(), Logger.Formatter.date_time_ms(), Keyword.t()) :: String.t()
  def format_log(level, msg, {date, time}, meta) do
    name = meta
      |> Keyword.get(:name)
      |> then(& if &1, do: "[#{inspect(&1)}]")
    "#{format_date(date)} #{format_time(time)} [#{level}] #{name} #{msg}\n"
  end
end
