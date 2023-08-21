defmodule BrokerTest do
  @moduledoc """
  Test broker
  """
  use ExUnit.Case

  alias ExRocketmq.{Broker, Transport, Util.Debug, Models}

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "topic" => topic
    } =
      File.read!("./tmp/broker.json") |> Jason.decode!()

    opts = [
      broker_name: "test_broker",
      remote: [transport: Transport.Tcp.new(host: host, port: port)]
    ]

    pid = start_supervised!({Broker, opts})
    [broker: pid, topic: topic]
  end

  test "send_hearbeat", %{broker: broker} do
    assert :ok ==
             Broker.heartbeat(broker, %ExRocketmq.Models.Heartbeat{
               client_id: "test",
               producer_data_set:
                 MapSet.new([
                   %ExRocketmq.Models.ProducerData{group: "test"}
                 ]),
               consumer_data_set: MapSet.new()
             })
             |> Debug.debug()
  end

  test "sync_send_message", %{broker: broker, topic: topic} do
    msg = %Models.Letter{
      body: "Sync: Hello Elixir",
      flag: 0,
      transaction_id: "",
      batch: false,
      compress: false,
      properties: %{}
    }

    assert {:ok, _resp} =
             Broker.sync_send_message(
               broker,
               %Models.SendMsg.Request{
                 producer_group: "test",
                 topic: topic,
                 queue_id: 1,
                 sys_flag: 0,
                 born_timestamp: :os.system_time(:millisecond),
                 flag: msg.flag,
                 properties: Models.Letter.encode_properties(msg),
                 reconsume_times: 0,
                 unit_mode: false,
                 max_reconsume_times: 0,
                 batch: msg.batch,
                 default_topic: "TBW102",
                 default_topic_queue_num: 4
               },
               msg.body
             )
             |> Debug.debug()
  end

  test "one_way_send_message", %{broker: broker, topic: topic} do
    msg = %Models.Letter{
      body: "One Way: Hello Elixir",
      flag: 0,
      transaction_id: "",
      batch: false,
      compress: false,
      properties: %{}
    }

    assert :ok ==
             Broker.one_way_send_message(
               broker,
               %Models.SendMsg.Request{
                 producer_group: "test",
                 topic: topic,
                 queue_id: 1,
                 sys_flag: 0,
                 born_timestamp: :os.system_time(:millisecond),
                 flag: msg.flag,
                 properties: Models.Letter.encode_properties(msg),
                 reconsume_times: 0,
                 unit_mode: false,
                 max_reconsume_times: 0,
                 batch: msg.batch,
                 default_topic: "TBW102",
                 default_topic_queue_num: 4
               },
               msg.body
             )
  end
end
