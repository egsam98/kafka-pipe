defmodule KafkaPipeWeb.Live.Connector do
  use KafkaPipeWeb, :live_view

  alias Phoenix.HTML.Form
  alias Phoenix.LiveView.Socket
  alias KafkaPipe.Connector
  alias KafkaPipe.Connector.Supervisor.Conn
  alias KafkaPipeWeb.Live.Connector.NewConnector

  @impl true
  def mount(_params, _session, socket) do
    connectors = Enum.map(Connector.Supervisor.connectors(), &conn_view/1)
    {:ok, socket
      |> assign(version: KafkaPipe.version(), form: nil)
      |> stream_configure(:connectors, dom_id: fn %Conn{name: name} -> Atom.to_string(name) end)
      |> stream(:connectors, connectors)}
  end

  @impl true
  def handle_event("new", _params, socket) do
    form = %NewConnector{}
      |> Ecto.Changeset.change()
      |> to_form()
    source_mods = Connector.modules(:source)
      |> Enum.map(& {Connector.humanize(&1), &1})
    sink_mods = Connector.modules(:sink)
      |> Enum.map(& {Connector.humanize(&1), &1})
    upload_opts = [auto_upload: true, progress: &handle_file_upload/3, accept: ~w(.yaml .yml)]

    {:noreply, socket
      |> assign(
        form: form,
        source_modules: source_mods,
        sink_modules: sink_mods
      )
      |> allow_upload(:source_config_file, upload_opts)
      |> allow_upload(:sink_config_file, upload_opts)
    }
  end

  @impl true
  def handle_event("cancel_new", _params, socket), do: {:noreply, assign(socket, form: nil)}

  @impl true
  def handle_event("create", %{"new_connector" => new_conn_params}, socket) do
    changeset = new_conn_params
      |> NewConnector.changeset()
      |> Map.put(:action, :create)
    {changeset, source_cfg} = parse_config(changeset, :source_module, :source_config)
    {changeset, sink_cfg} = parse_config(changeset, :sink_module, :sink_config)

    socket = case Ecto.Changeset.apply_action(changeset, :create) do
      {:ok, %NewConnector{name: name, source_module: source_mod, sink_module: sink_mod}} ->
        case Connector.Supervisor.register(name, source_mod, source_cfg, sink_mod, sink_cfg) do
          {:ok, conn} -> socket
            |> stream_insert(:connectors, conn_view(conn))
            |> assign(:form, nil)
          {:error, :exists} ->
            form = changeset
              |> Ecto.Changeset.add_error(:name, "Connector exists")
              |> to_form()
            assign(socket, form: form)
        end
      {:error, changeset} ->
        assign(socket, form: to_form(changeset))
    end

    {:noreply, socket}
  end

  # Required for file upload progress
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

  @spec handle_file_upload(atom(), Phoenix.LiveView.UploadEntry.t(), Socket.t()) :: {:noreply, Socket.t()}
  defp handle_file_upload(name, entry, socket) when entry.done? do
    %Socket{assigns: %{form: %Form{source: changeset}}} = socket

    changeset = case consume_uploaded_entries(socket, name, fn %{path: path}, _ -> {:ok, File.read(path)} end) do
      [{:ok, raw}] ->
        config_field = name
          |> Atom.to_string()
          |> String.replace_trailing("_file", "")
          |> String.to_existing_atom()
        Ecto.Changeset.put_change(changeset, config_field, raw)
      [] -> Ecto.Changeset.add_error(changeset, name, "Failed to upload file")
    end
    {:noreply, assign(socket, form: to_form(changeset))}
  end

  defp handle_file_upload(_name, _entry, socket), do: {:noreply, socket}

  @spec parse_config(Ecto.Changeset.t(NewConnector.t()), atom(), atom()) :: {Ecto.Changeset.t(NewConnector.t()), struct() | nil}
  defp parse_config(changeset, mod_field, cfg_field) do
    mod_binary = Ecto.Changeset.get_change(changeset, mod_field)
    cfg_binary = Ecto.Changeset.get_change(changeset, cfg_field)

    with true <- mod_binary != nil and cfg_binary != nil,
      {:ok, cfg_raw} <- YamlElixir.read_from_string(cfg_binary),
      true <- is_map(cfg_raw) || {:error, "YAML map required"},
      {:ok, cfg} <- Module.safe_concat(mod_binary, Config).new(cfg_raw)
    do
      {changeset, cfg}
    else
      false -> {changeset, nil}
      {:error, %Ecto.Changeset{errors: errors}} ->
        changeset = errors
          |> Enum.reduce(changeset, fn {field, {msg, _opts}}, changeset ->
            Ecto.Changeset.add_error(changeset, cfg_field, "#{field}: #{msg}")
          end)
        {changeset, nil}
      {:error, reason} ->
        msg = case reason do
          %YamlElixir.ParsingError{message: msg} -> msg
          reason when is_binary(reason) -> reason
        end
        {Ecto.Changeset.add_error(changeset, cfg_field, msg), nil}
    end
  end

  @spec conn_view(Conn.t()) :: map()
  defp conn_view(%Conn{name: name, source: source, sink: sink} = conn) do
    %{conn | source: member_view(name, source), sink: member_view(name, sink)}
  end

  @spec member_view(atom(), Conn.Member.t()) :: map()
  defp member_view(name, %Conn.Member{mod: mod} = member) do
    member
    |> Map.put(:mod, Connector.humanize(mod))
    |> Map.put(:pid, Connector.Supervisor.member_pid(name, :source))
  end
end
