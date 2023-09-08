defmodule ConsumerTest do
  @moduledoc """
  test consumer
  """
  use ExUnit.Case

  alias ExRocketmq.{Consumer, Namesrvs, Transport}
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
      consumer_group: group,
      namesrvs: namesrvs,
      processor: %ExRocketmq.Consumer.MockProcessor{}
    ]

    pid = start_supervised!({Consumer, opts})
    [consumer: pid, topic: topic, group: group]
  end
end
