defmodule KafkaPipeWeb.Components do
  use Phoenix.Component

  alias Phoenix.LiveView.JS
  import KafkaPipeWeb.CoreComponents

  attr :name, :string, required: true
  attr :class, :string, default: "btn btn-primary btn-soft bg-none"
  attr :rest, :global
  def icon_button(assigns) do
    ~H"""
    <.button class={@class} {@rest}>
      <.icon name={@name} class="size-6" />
    </.button>
    """
  end

  attr :id, :string, required: true
  slot :inner_block
  def modal(%{id: id} = assigns) do
    assigns = assign(assigns, :sel, "#" <> id)
    ~H"""
    <dialog id={@id} class="modal" phx-mounted={JS.set_attribute({"open", true})}>
      <div class="modal-box" phx-click-away="cancel_new">
        <.icon_button name="hero-x-mark"
          class="btn btn-primary btn-soft btn-circle absolute right-2 top-2"
          phx-click="cancel_new" />
        {render_slot(@inner_block)}
      </div>
    </dialog>
    """
  end

  attr :data, :map, required: true
  def secure_inspect(assigns) do
    assigns = update(assigns, :data, &KafkaPipe.Enum.walk(&1, fn
      {key, value} -> if to_string(key) =~ ~r/pass(word)?/, do: "******", else: value
      elem -> elem
    end))

    ~H"""
    <span class="whitespace-pre">{inspect(@data, pretty: true, width: 0)}</span>
    """
  end

  attr :id, :string, required: true
  attr :field_module, Phoenix.HTML.FormField, required: true
  attr :field_config, Phoenix.HTML.FormField, required: true
  attr :modules, :list, required: true
  attr :file, Phoenix.LiveView.UploadConfig, required: true
  def member_form(%{id: id} = assigns) do
    assigns = assign(assigns, ace_id: id <> "_ace", title: String.capitalize(id))
    ~H"""
    <div class="mb-3">
      <.input field={@field_module} type="select" label={@title} options={@modules} />
      <.input field={@field_config} type="textarea" class="hidden" label={@title <> " config"} phx-hook="AceInput" phx-ace-id={@ace_id} />
      <div id={@ace_id} class="w-full mb-2 h-40" phx-update="ignore" />
      <label for={@file.ref} phx-drop-target={@file.ref} >
        <.live_file_input upload={@file} class="file-input" />
      </label>
    </div>
    """
  end
end
