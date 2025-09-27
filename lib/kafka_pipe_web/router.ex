defmodule KafkaPipeWeb.Router do
  use KafkaPipeWeb, :router

  import Phoenix.LiveDashboard.Router
  import Tarams

  pipeline :browser do
    plug :accepts, ["html"]
    plug :fetch_session
    plug :fetch_live_flash
    plug :put_root_layout, html: {KafkaPipeWeb.Layouts, :root}
    plug :protect_from_forgery
    plug :put_secure_browser_headers
    plug :plug_scrub
  end

  pipeline :api do
    plug :accepts, ["json"]
  end

  # live_session :default do
  #   scope "/", KafkaPipeWeb do
  #     pipe_through :browser
  #     # live "/", Live.Connector
  #   end
  # end

  scope "/", KafkaPipeWeb do
    pipe_through :browser

    live "/", Live.Connector
    # get "/", PageController, :home
    # post "/register", PageController, :register
    live_dashboard "/dashboard", metrics: KafkaPipeWeb.Telemetry
  end

  # Other scopes may use custom stacks.
  # scope "/api", KafkaPipeWeb do
  #   pipe_through :api
  # end
end
