defmodule KafkaPipeWeb.Live.Connector do
  use KafkaPipeWeb, :live_view

  alias Phoenix.LiveView.Socket
  alias KafkaPipe.Connector
  alias KafkaPipe.Connector.Supervisor.Conn

  @register_schema %{
    name: [
      type: :string,
      required: true,
      into: &String.to_atom/1
    ],
    source_module: [
      type: :string,
      required: true,
    ],
    sink_module: [
      type: :string,
      required: true,
    ]
  }

  @impl true
  def mount(_params, _session, socket) do
    source_mods = Connector.modules(:source)
      |> Enum.map(& {Connector.humanize(&1), &1})
    sink_mods = Connector.modules(:sink)
      |> Enum.map(& {Connector.humanize(&1), &1})
    connectors = Connector.Supervisor.connectors()
      |> Enum.map(&conn_view/1)

    {:ok, socket
      |> assign(version: KafkaPipe.version(), source_modules: source_mods, sink_modules: sink_mods)
      |> stream_configure(:connectors, dom_id: fn %Conn{name: name} -> Atom.to_string(name) end)
      |> stream(:connectors, connectors)
      |> allow_upload(:source_config, accept: ~w(.yaml .yml))
      |> allow_upload(:sink_config, accept: ~w(.yaml .yml))}
  end

  @impl true
  def handle_event("register", params, socket) do
    socket = case params
      |> Tarams.scrub_param()
      |> Tarams.cast(@register_schema)
    do
      {:ok, %{name: name, source_module: source_mod, sink_module: sink_mod}} ->
        do_register(socket, name, source_mod, sink_mod)
      {:error, details} ->
        msg = details
          |> Enum.map_join("; ", fn {field, errors} -> "#{field}: #{inspect(errors)}" end)
          |> then(& "Invalid parameters: #{&1}")
        put_flash(socket, :error, msg)
    end

    {:noreply, socket}
  end

  @impl true
  def handle_event("validate", _params, socket), do: {:noreply, socket}

  @impl true
  def handle_event("start", %{"name" => name}, socket) do
    name = String.to_existing_atom(name)
    socket = case Connector.Supervisor.start_child(name) do
      {:ok, conn} -> socket
        |> stream_insert(:connectors, conn_view(conn))
        |> put_flash(:info, "Connector has been started")
      {:error, :not_found} -> put_flash(socket, :error, "Connector not found")
      {:error, {:already_started, _pid}} -> put_flash(socket, :error, "Connector is running")
      {:error, {:start, reason}} -> put_flash(socket, :error, to_string(reason))
    end
    {:noreply, socket}
  end

  @impl true
  def handle_event("stop", %{"name" => name}, socket) do
    name = String.to_existing_atom(name)
    socket = case Connector.Supervisor.stop_child(name) do
      {:ok, conn} -> socket
        |> stream_insert(:connectors, conn_view(conn))
        |> put_flash(:info, "Connector has been stopped")
      {:error, :not_found} -> put_flash(socket, :error, "Connector not found")
      {:error, reason} -> put_flash(socket, :error, to_string(reason))
    end
    {:noreply, socket}
  end

  @impl true
  def handle_event("delete", %{"name" => name}, socket) do
    name = String.to_existing_atom(name)
    state = case Connector.Supervisor.delete_child(name) do
      {:ok, %Conn{name: name}} -> socket
        |> stream_delete_by_dom_id(:connectors, Atom.to_string(name))
        |> put_flash(:info, "Connector has been deleted")
      {:error, :not_found} -> put_flash(socket, :error, "Connector not found")
      {:error, reason} -> put_flash(socket, :error, to_string(reason))
    end
    {:noreply, state}
  end

  @impl true
  def handle_info(:clear_flash, socket), do: {:noreply, clear_flash(socket)}

  defp do_register(socket, name, source_mod, sink_mod) do
    source_mod = Module.safe_concat([source_mod])
    sink_mod = Module.safe_concat([sink_mod])

    with {"Source", {:ok, source_opts}} <- {"Source", read_yaml(socket, :source_config)},
      {"Sink", {:ok, sink_opts}} <- {"Sink", read_yaml(socket, :sink_config)},
      {:ok, conn} <- Connector.Supervisor.register(name, source_mod, source_opts, sink_mod, sink_opts)
    do
      socket
      |> stream_insert(:connectors, conn_view(conn))
      |> push_event("close", %{})
    else
      {:error, :exists} -> put_flash(socket, :error, "Connector exists")
      {member, {:error, %YamlElixir.ParsingError{message: message}}} ->
        put_flash(socket, :error, "#{member}: #{message}")
    end
  rescue
    ArgumentError -> put_flash(socket, :error, "Module not found")
  end

  @spec read_yaml(Socket.t(), atom()) ::
    {:ok, keyword()} | {:error, YamlElixir.FileNotFoundError.t() | YamlElixir.ParsingError.t()}
  defp read_yaml(socket, name) do
    fun = fn %{path: path}, _ ->
      {:ok, YamlElixir.read_from_file(path)}
    end
    case consume_uploaded_entries(socket, name, fun) do
      [result | _] -> result
      [] -> {:error, %YamlElixir.ParsingError{message: "file is required"}}
    end
  end

  @spec conn_view(Conn.t()) :: map()
  defp conn_view(%Conn{name: name, source: source, sink: sink} = conn) do
    %{conn | source: member_view(name, source), sink: member_view(name, sink)}
  end

  @spec member_view(atom(), Conn.Member.t()) :: map()
  defp member_view(name, %Conn.Member{mod: mod} = member), do: member
    |> Map.put(:mod, Connector.humanize(mod))
    |> Map.put(:pid, Connector.Supervisor.member_pid(name, :source))
end
