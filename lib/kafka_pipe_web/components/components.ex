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
    <dialog id={@id} class="modal" phx-hook="ModalHook" phx-mounted={JS.ignore_attributes(["open"])}>
      <div class="modal-box" phx-click-away={JS.dispatch("phx:close", to: @sel)}>
        <.icon_button name="hero-x-mark"
          class="btn btn-primary btn-soft btn-circle absolute right-2 top-2"
          phx-click={JS.dispatch("phx:close", to: @sel)} />
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
end
