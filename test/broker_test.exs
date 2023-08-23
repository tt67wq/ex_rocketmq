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

  test "send_hearbeat", %{broker: broker, group: group, topic: topic} do
    assert :ok ==
             Broker.heartbeat(broker, %Models.Heartbeat{
               client_id: "test",
               producer_data_set:
                 MapSet.new([
                   %Models.ProducerData{group: "test"}
                 ]),
               consumer_data_set:
                 MapSet.new([
                   %Models.ConsumerData{
                     group: group,
                     consume_type: "CONSUME_ACTIVELY",
                     message_model: "Clustering",
                     consume_from_where: "CONSUME_FROM_LAST_OFFSET",
                     subscription_data_set:
                       MapSet.new([
                         %Models.Subscription{
                           class_filter_mode: true,
                           topic: topic,
                           sub_string: "*",
                           tags_set: MapSet.new([]),
                           code_set: MapSet.new([]),
                           sub_version: 0,
                           expression_type: "TAG"
                         }
                       ]),
                     unit_mode: false
                   }
                 ])
             })
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
    assert {:ok, offset} =
             Broker.query_consumer_offset(
               broker,
               %Models.QueryConsumerOffset{
                 consumer_group: group,
                 topic: topic,
                 queue_id: 1
               }
             )

    assert {:ok, _} =
             Broker.pull_message(
               broker,
               %Models.PullMsg.Request{
                 consumer_group: group,
                 topic: topic,
                 queue_id: 1,
                 queue_offset: offset - 5,
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

  test "search_offset_by_timestamp", %{broker: broker, topic: topic} do
    assert {:ok, _} =
             Broker.search_offset_by_timestamp(
               broker,
               %Models.SearchOffset{
                 topic: topic,
                 queue_id: 1,
                 timestamp: :os.system_time(:millisecond)
               }
             )
  end

  test "get_max_offset", %{broker: broker, topic: topic} do
    assert {:ok, _} =
             Broker.get_max_offset(
               broker,
               %Models.GetMaxOffset{
                 topic: topic,
                 queue_id: 1
               }
             )
  end

  test "update_consumer_offset", %{broker: broker, topic: topic, group: group} do
    assert {:ok, offset} =
             Broker.get_max_offset(
               broker,
               %Models.GetMaxOffset{
                 topic: topic,
                 queue_id: 1
               }
             )

    assert :ok ==
             Broker.update_consumer_offset(
               broker,
               %Models.UpdateConsumerOffset{
                 consumer_group: group,
                 topic: topic,
                 queue_id: 1,
                 commit_offset: offset - 10
               }
             )
  end

  test "consumer_send_msg_back", %{broker: broker, topic: topic, group: group} do
    assert :ok ==
             Broker.consumer_send_msg_back(
               broker,
               %Models.ConsumerSendMsgBack{
                 group: group,
                 offset: 296_368_101_057,
                 delay_level: 1,
                 origin_msg_id: "31302E38382E342E3237000051AF000A91F9",
                 origin_topic: topic,
                 max_reconsume_times: 3
               }
             )
  end
end
