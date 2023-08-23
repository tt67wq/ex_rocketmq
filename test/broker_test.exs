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
      "topic" => topic,
      "group" => group
    } =
      File.read!("./tmp/broker.json") |> Jason.decode!()

    opts = [
      broker_name: "test_broker",
      remote: [transport: Transport.Tcp.new(host: host, port: port)]
    ]

    pid = start_supervised!({Broker, opts})
    [broker: pid, topic: topic, group: group]
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
    msg = %Models.Message{
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
                 properties: Models.Message.encode_properties(msg),
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
    msg = %Models.Message{
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
                 properties: Models.Message.encode_properties(msg),
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

  test "pull_message", %{broker: broker, topic: topic, group: group} do
    assert {:ok, _} =
             Broker.pull_message(
               broker,
               %Models.PullMsg.Request{
                 consumer_group: group,
                 topic: topic,
                 queue_id: 1,
                 queue_offset: 692_670,
                 max_msg_nums: 5,
                 sys_flag: 0,
                 commit_offset: 0,
                 suspend_timeout_millis: 0,
                 sub_expression: "*",
                 sub_version: 0,
                 expression_type: "TAG"
               }
             )
             |> Debug.debug()
  end

  test "query_consumer_offset", %{broker: broker, topic: topic, group: group} do
    assert {:ok, _} =
             Broker.query_message(
               broker,
               %Models.QueryConsumerOffset{
                 consumer_group: group,
                 topic: topic,
                 queue_id: 1
               }
             )
             |> Debug.debug()
  end
end
