defmodule Test.Connector.Sink.Kafka do
  use ExUnit.Case, async: true

  alias KafkaPipe.Connector.Message
  alias KafkaPipe.Connector.Sink.{Kafka, Kafka.Config, Kafka.State}

  import Mimic
  import Test.Rand

  describe "init/1" do
    setup do
      self = self()
      endpoints = ["localhost:9092"]
      batch_size = 50_000
      batch_timeout = 5_000
      topic = rand()
      {:ok, cfg} = Config.new(%{
        endpoints: endpoints,
        batch_size: batch_size,
        batch_timeout: batch_timeout,
        topics: [%{name: topic}]
      })

      {:ok, %{config: cfg, brod: self}}
    end

    test "ok", %{config: cfg, brod: brod} do
      expect :brod, :create_topics, fn _, _, _ -> :ok end
      expect :brod, :start_link_client, fn endpoints, _, _ ->
        assert endpoints == [{"localhost", 9092}]
        {:ok, brod}
      end

      assert {
        :consumer,
        %State{brod: brod, batch_size: cfg.batch_size, batch_timeout: cfg.batch_timeout},
        subscribe_to: [:producer]
      } == Kafka.init(name: rand(:atom), config: cfg, subscribe_to: [:producer])
    end

    test "topic exists", %{config: cfg, brod: brod} do
      expect :brod, :create_topics, fn _, _, _ ->
        [%Config.Topic{name: name} | _] = cfg.topics
        {:error, "Topic '#{name}' already exists"}
      end
      expect :brod, :start_link_client, fn _, _, _ -> {:ok, brod} end

      assert {
        :consumer,
        %State{brod: brod, batch_size: cfg.batch_size, batch_timeout: cfg.batch_timeout},
        subscribe_to: [:producer]
      } == Kafka.init(name: rand(:atom), config: cfg, subscribe_to: [:producer])
    end

    test "error returned from Kafka", %{config: cfg} do
      reason = "Some error occurred"
      assert_fun = fn ->
        assert {:stop, {:start, inspect(reason)}} == Kafka.init(name: rand(:atom), config: cfg, subscribe_to: [:producer])
      end

      expect :brod, :create_topics, fn _, _, _ -> {:error, reason} end
      assert_fun.()
      expect :brod, :create_topics, fn _, _, _ -> :ok end
      expect :brod, :start_link_client, fn _, _, _ -> {:error, reason} end
      assert_fun.()
    end
  end

  test "handle_subscribe/4" do
    from = {self(), make_ref()}
    assert {:manual, %State{timer: timer, source: ^from}} =
      Kafka.handle_subscribe(:producer, [], from, %State{brod: nil, batch_size: 0, batch_timeout: 0})
    refute is_nil(timer)
    assert_receive :timeout
  end

  describe "handle_events/3" do
    setup do
      self = self()
      timer = Process.send_after(self, :ok, 100_000)
      msgs = [
        %Message{topic: "topic1", from: self},
        %Message{topic: "topic2", from: self}
      ]

      pid = spawn_link(fn ->
        assert_receive {:"$gen_call", from, {:ack, ^msgs}}, 1_000
        GenStage.reply(from, :ok)
      end)

      {:ok, %{producer: {pid, make_ref()}, msgs: msgs, timer: timer}}
    end

    test "messages < batch size", %{producer: producer, msgs: msgs, timer: timer} do
      expect :brod, :produce_sync, length(msgs), fn _, topic, :hash, :undefined, _ when topic in ["topic1", "topic2"] ->
        :ok
      end
      state = %State{brod: self(), batch_size: length(msgs) + 1, batch_timeout: 0, timer: timer}
      assert {:noreply, [], state} == Kafka.handle_events(msgs, producer, state)
    end

    test "messages == batch size", %{producer: producer, msgs: msgs, timer: timer} do
      expect :brod, :produce_sync, length(msgs), fn _, topic, :hash, :undefined, _ when topic in ["topic1", "topic2"] ->
        :ok
      end
      state = %State{brod: self(), batch_size: length(msgs), batch_timeout: 0, timer: timer}
      assert {:noreply, [], %State{state | timer: nil}} == Kafka.handle_events(msgs, producer, state)
      assert_received :timeout
    end

    test "crashed request to Kafka", %{msgs: msgs, producer: producer} do
      Process.flag(:trap_exit, true)
      expect :brod, :produce_sync, length(msgs), fn _, _, _, _, _ ->
        {:error, :client_down}
      end
      state = %State{brod: nil, batch_size: 0, batch_timeout: 0}

      assert {:stop, %RuntimeError{message: "client down"}, state} == Kafka.handle_events(msgs, producer, state)
    end
  end

  test "handle_info(:timeout)/2" do
    batch_size = 10
    state = %State{brod: nil, batch_size: batch_size, batch_timeout: 0, source: {self(), make_ref()}}

    assert {:noreply, [], %State{timer: timer}} = Kafka.handle_info(:timeout, state)
    refute is_nil(timer)
    assert_receive {:"$gen_producer", _, {:ask, ^batch_size}}
    assert_receive :timeout
  end
end
