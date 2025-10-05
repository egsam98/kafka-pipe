defmodule KafkaPipeWeb.Live.Connector do
  use KafkaPipeWeb, :live_view

  alias Phoenix.HTML.Form
  alias Phoenix.LiveView.Socket
  alias KafkaPipe.Connector
  alias KafkaPipe.Connector.Supervisor
  alias KafkaPipe.Connector.Supervisor.Conn
  alias KafkaPipeWeb.Live.Connector.{CreateRequest, UpdateRequest}

  @impl true
  def mount(_params, _session, socket) do
    connectors = Enum.map(Supervisor.connectors(), &conn_view/1)
    {:ok, socket
      |> assign(version: KafkaPipe.version(), form: nil)
      |> stream_configure(:connectors, dom_id: fn %{name: name} -> Atom.to_string(name) end)
      |> stream(:connectors, connectors)}
  end

  @impl true
  def handle_event("new", _params, socket) do
    form = %CreateRequest{}
      |> Ecto.Changeset.change()
      |> to_form(action: :create)
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
  def handle_event("create", %{"create_request" => req}, socket) do
    changeset = CreateRequest.changeset(req)
    {changeset, source_cfg} = decode_config(changeset, :source_config)
    {changeset, sink_cfg} = decode_config(changeset, :sink_config)

    socket = case Ecto.Changeset.apply_action(changeset, :create) do
      {:ok, %CreateRequest{name: name, source_module: source_mod, sink_module: sink_mod}} ->
        case Connector.Supervisor.create(name, source_mod, source_cfg, sink_mod, sink_cfg) do
          {:ok, conn} -> socket
            |> stream_insert(:connectors, conn_view(conn))
            |> assign(:form, nil)
          {:error, :exists} ->
            form = changeset
              |> Ecto.Changeset.add_error(:name, "Connector exists")
              |> to_form(action: :create)
            assign(socket, form: form)
          {:error, %Connector.ConfigError{source: source_errors, sink: sink_errors}} ->
            form = changeset
              |> changeset_add_errors(:source_config, source_errors)
              |> changeset_add_errors(:sink_config, sink_errors)
              |> to_form(action: :create)
            assign(socket, form: form)
        end
      {:error, changeset} ->
        assign(socket, form: to_form(changeset, action: :create))
    end

    {:noreply, socket}
  end

  @impl true
  def handle_event("edit", %{"name" => name}, socket) do
    name = String.to_existing_atom(name)

    socket = case Supervisor.connector(name) do
      nil -> put_flash(socket, :error, "Connector not found")
      %Supervisor.Conn{
        source: %Conn.Member{config: source_cfg},
        sink: %Conn.Member{config: sink_cfg}
      } ->
        form = %UpdateRequest{
            name: name,
            source_config: Yaml.encode(source_cfg),
            sink_config: Yaml.encode(sink_cfg)
          }
          |> Ecto.Changeset.change()
          |> to_form(action: :update)
        upload_opts = [auto_upload: true, progress: &handle_file_upload/3, accept: ~w(.yaml .yml)]

        socket
        |> assign(form: form)
        |> allow_upload(:source_config_file, upload_opts)
        |> allow_upload(:sink_config_file, upload_opts)
    end

    {:noreply, socket}
  end

  @impl true
  def handle_event("update", %{"update_request" => req}, socket) do
    changeset = UpdateRequest.changeset(req)
    {changeset, source_cfg} = decode_config(changeset, :source_config)
    {changeset, sink_cfg} = decode_config(changeset, :sink_config)

    socket = case Ecto.Changeset.apply_action(changeset, :update) do
      {:ok, %UpdateRequest{name: name}} ->
        case Connector.Supervisor.update(name, source_cfg, sink_cfg) do
          {:ok, conn} -> socket
            |> stream_insert(:connectors, conn_view(conn))
            |> assign(:form, nil)
          {:error, :not_found} ->
            form = changeset
              |> Ecto.Changeset.add_error(:name, "Connector not found")
              |> to_form(action: :update)
            assign(socket, form: form)
          {:error, %Connector.ConfigError{source: source_errors, sink: sink_errors}} ->
            form = changeset
              |> changeset_add_errors(:source_config, source_errors)
              |> changeset_add_errors(:sink_config, sink_errors)
              |> to_form(action: :update)
            assign(socket, form: form)
        end
      {:error, changeset} ->
        assign(socket, form: to_form(changeset, action: :update))
    end

    {:noreply, socket}
  end

  @impl true
  def handle_event("cancel_modal", _params, socket), do: {:noreply, assign(socket, form: nil)}

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
    state = case Supervisor.delete_child(name) do
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
    %Socket{assigns: %{form: %Form{source: changeset, action: action}}} = socket

    changeset = case consume_uploaded_entries(socket, name, fn %{path: path}, _ -> {:ok, File.read(path)} end) do
      [{:ok, raw}] ->
        config_field = name
          |> Atom.to_string()
          |> String.replace_trailing("_file", "")
          |> String.to_existing_atom()
        Ecto.Changeset.put_change(changeset, config_field, raw)
      [] -> Ecto.Changeset.add_error(changeset, name, "Failed to upload file")
    end
    {:noreply, assign(socket, form: to_form(changeset, action: action))}
  end

  defp handle_file_upload(_name, _entry, socket), do: {:noreply, socket}

  defp decode_config(%Ecto.Changeset{changes: changes} = changeset, key) when is_map_key(changes, key) do
    case Yaml.decode(changes[key]) do
      {:ok, cfg} when is_map(cfg) -> {changeset, cfg}
      {:ok, _} -> {Ecto.Changeset.add_error(changeset, key, "YAML map required"), nil}
      {:error, %Yaml.ParsingError{message: msg}} -> {Ecto.Changeset.add_error(changeset, key, msg), nil}
    end
  end

  defp decode_config(changeset, _key), do: {changeset, nil}

  @spec conn_view(Supervisor.Conn.t()) :: map()
  defp conn_view(%Supervisor.Conn{name: name} = conn) do
    conn
    |> Map.from_struct()
    |> Map.update!(:source, &member_view(name, &1))
    |> Map.update!(:sink, &member_view(name, &1))
  end

  @spec member_view(atom(), Supervisor.Conn.Member.t()) :: map()
  defp member_view(name, %Supervisor.Conn.Member{mod: mod} = member) do
    member
    |> Map.from_struct()
    |> Map.put(:mod, Connector.humanize(mod))
    |> Map.put(:pid, Connector.Supervisor.member_pid(name, :source))
  end

  defp changeset_add_errors(changeset, field, %{} = errors) do
    Enum.reduce(errors, changeset, fn {error_field, msgs}, changeset ->
      Ecto.Changeset.add_error(changeset, field, "#{error_field}: #{inspect(msgs)}")
    end)
  end

  defp changeset_add_errors(changeset, _field, nil), do: changeset
end
