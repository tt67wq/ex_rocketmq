defmodule ProducerTest do
  @moduledoc """
  test producer
  """
  use ExUnit.Case

  alias ExRocketmq.{Producer, Namesrvs, Transport, Util.Debug}
  alias ExRocketmq.Models.{Message}

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "topic" => topic,
      "group" => group
    } =
      File.read!("./tmp/producer.json") |> Jason.decode!()

    namesrvs_opts = [
      remotes: [
        [transport: Transport.Tcp.new(host: host, port: port)]
      ]
    ]

    namesrvs = start_supervised!({Namesrvs, namesrvs_opts})

    opts = [
      group_name: group,
      namesrvs: namesrvs
    ]

    pid = start_supervised!({Producer, opts})
    [producer: pid, topic: topic, group: group]
  end

  test "send_sync", %{producer: producer, topic: topic} do
    assert {:ok, _} =
             Producer.send_sync(producer, [%Message{topic: topic, body: "Hello from elixir"}])
  end

  test "send_multi", %{producer: producer, topic: topic} do
    assert {:ok, _} =
             Producer.send_sync(producer, [
               %Message{topic: topic, body: "Hello from elixir A"},
               %Message{topic: topic, body: "Hello from elixir B"}
             ])
  end

  test "send_oneway", %{producer: producer, topic: topic} do
    assert :ok =
             Producer.send_oneway(producer, [
               %Message{topic: topic, body: "Hello from elixir oneway"}
             ])
  end

  test "send_transaction_msg", %{producer: producer, topic: topic} do
    assert {:ok, _} =
             Producer.send_transaction_msg(
               producer,
               %Message{topic: topic, body: "Hello from elixir transaction"}
             )
             |> Debug.debug()

    # sleep 10 seconds to wait for broker notify
    Process.sleep(30_000)
  end

  # test "send_many_times", %{producer: producer, topic: topic} do
  #   send_many_times(producer, topic, 10)
  # end

  # defp send_many_times(_, _, 0), do: :ok

  # defp send_many_times(producer, topic, idx) do
  #   Producer.send_sync(producer, [%Message{topic: topic, body: "Hello from elixir, #{idx}"}])
  #   Process.sleep(1000)
  #   send_many_times(producer, topic, idx - 1)
  # end
end
