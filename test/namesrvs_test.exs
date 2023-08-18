defmodule NamesrvsTest do
  use ExUnit.Case

  alias ExRocketmq.{Namesrvs, Transport}

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "topic" => topic
    } =
      File.read!("./tmp/test.json") |> Jason.decode!()

    namesrvs_opts = [
      remotes: [
        [transport: Transport.Tcp.new(host: host, port: port)]
      ]
    ]

    namesrvs = start_supervised!({Namesrvs, namesrvs_opts})
    [namesrvs: namesrvs, topic: topic]
  end

  test "query_topic_route_info", %{namesrvs: namesrvs, topic: topic} do
    assert {:ok, _res} = Namesrvs.query_topic_route_info(namesrvs, topic)
  end

  test "get_broker_cluster_info", %{namesrvs: namesrvs} do
    assert {:ok, _res} = Namesrvs.get_broker_cluster_info(namesrvs)
  end
end
