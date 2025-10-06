defmodule Test.Connector.Sink.Kafka do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.Message
  alias KafkaPipe.Connector.Sink.{Batch, Kafka, Kafka.Config}

  import Mimic
  import Test.Rand

  describe "init/2" do
    setup do
      endpoints = ["localhost:9092"]
      topic = rand()
      cfg = %{
        endpoints: endpoints,
        batch: %{size: 50_000, timeout: 1_000},
        topics: [%{name: topic}]
      }

      {:ok, %{config: cfg, brod: rand(:pid)}}
    end

    test "ok", %{config: %{batch: batch} = cfg, brod: brod} do
      expect :brod, :create_topics, fn _, _, _ -> :ok end
      expect :brod, :start_link_client, fn endpoints, _, _ ->
        assert endpoints == [{"localhost", 9092}]
        {:ok, brod}
      end

      assert {:ok, %Batch{size: batch[:size], timeout: batch[:timeout]}, brod} == Kafka.init(rand(:atom), cfg)
    end

    test "topic exists", %{config: %{batch: batch} = cfg, brod: brod} do
      expect :brod, :create_topics, fn _, _, _ ->
        [%{name: name} | _] = cfg.topics
        {:error, "Topic '#{name}' already exists"}
      end
      expect :brod, :start_link_client, fn _, _, _ -> {:ok, brod} end

      assert {:ok, %Batch{size: batch[:size], timeout: batch[:timeout]}, brod} == Kafka.init(rand(:atom), cfg)
    end

    test "error returned from Kafka", %{config: cfg} do
      reason = "Some error occurred"
      assert_fun = fn ->
        assert {:error, {:start, inspect(reason)}} == Kafka.init(rand(:atom), cfg)
      end

      expect :brod, :create_topics, fn _, _, _ -> {:error, reason} end
      assert_fun.()
      expect :brod, :create_topics, fn _, _, _ -> :ok end
      expect :brod, :start_link_client, fn _, _, _ -> {:error, reason} end
      assert_fun.()
    end
  end

  describe "handle_messages/3" do
    setup do
      msgs = [
        %Message{topic: "topic1"},
        %Message{topic: "topic2"}
      ]

      spawn_link(fn ->
        assert_receive {:"$gen_call", from, {:ack, ^msgs}}, 1_000
        GenStage.reply(from, :ok)
      end)

      {:ok, %{msgs: msgs}}
    end

    test "ok", %{msgs: msgs} do
      expect :brod, :produce_sync, length(msgs), fn _, topic, :hash, :undefined, _ when topic in ["topic1", "topic2"] ->
        :ok
      end
      brod = rand(:pid)
      assert {:ok, brod} == Kafka.handle_messages(msgs, brod)
    end

    test "crashed request to Kafka", %{msgs: msgs} do
      Process.flag(:trap_exit, true)
      expect :brod, :produce_sync, length(msgs), fn _, _, _, _, _ ->
        {:error, :client_down}
      end

      assert {:error, %RuntimeError{message: "client down"}} == Kafka.handle_messages(msgs, rand(:pid))
    end
  end
end

defmodule Test.Connector.Sink.Kafka.Config do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.Sink.{Batch, Kafka.Config}

  @endpoint "127.0.0.1:29092"

  test "ok" do
    assert {:ok, %Config{
      endpoints: [@endpoint],
      batch: %Batch{size: 10_000, timeout: 5_000},
      topics: [
        %Config.Topic{
          name: "test",
          num_partitions: 1,
          replication_factor: 1,
          configs: %{}
        }
      ]
    }} == Config.new(%{endpoints: [@endpoint], topics: [%{name: "test"}]})
  end

  test "blank fields" do
    cases = [
      %{value: nil, reason: %{endpoints: ["can't be blank"], topics: ["is invalid"], batch: ["can't be blank"]}},
      %{value: "", reason: %{endpoints: ["can't be blank"], topics: ["is invalid"], batch: ["is invalid"]}},
      %{value: " ", reason: %{endpoints: ["can't be blank"], topics: ["is invalid"], batch: ["is invalid"]}}
    ]
    for %{value: value, reason: reason} <- cases do
      data = :fields
        |> Config.__schema__()
        |> Enum.map(& {&1, value})
        |> Map.new()
      assert {:error, reason} == Config.new(data)
    end
  end

  test "blank endpoints" do
    for endpoints <- [[], [nil], [" "]] do
      assert {:error, %{endpoints: ["should have at least 1 item(s)"]}} == Config.new(%{endpoints: endpoints})
    end
  end

  test "invalid endpoint" do
    assert {:error, %{endpoints: ["must match {hostname}:{port} format"]}} == Config.new(%{endpoints: ["9092"]})
  end

  test "batch size == 0" do
    assert {:error, %{batch: %{size: ["must be greater than 0"]}}} == Config.new(%{
      endpoints: [@endpoint],
      batch: %{size: 0}
    })
  end

  test "batch timeout == 0" do
    assert {:error, %{batch: %{timeout: ["must be greater than 0"]}}} == Config.new(%{
      endpoints: [@endpoint],
      batch: %{timeout: 0}
    })
  end
end

defmodule Test.Connector.Sink.Kafka.Config.Topic do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.Sink.Kafka.Config.Topic

  test "blank name" do
    for value <- [nil, " "] do
      data = :fields
        |> Topic.__schema__()
        |> Enum.map(& {&1, value})
        |> Map.new()
      errors = %Topic{}
        |> Topic.changeset(data)
        |> Ectox.Changeset.errors()
      assert %{name: ["can't be blank"]} == errors
    end
  end

  test "num partitions == 0" do
    errors = %Topic{}
      |> Topic.changeset(%{name: "test", num_partitions: 0})
      |> Ectox.Changeset.errors()
    assert %{num_partitions: ["must be greater than 0"]} == errors
  end

  test "replication factor == 0" do
    errors = %Topic{}
      |> Topic.changeset(%{name: "test", replication_factor: 0})
      |> Ectox.Changeset.errors()
    assert %{replication_factor: ["must be greater than 0"]} == errors
  end
end
