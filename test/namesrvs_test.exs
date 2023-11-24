defmodule NamesrvsTest do
  @moduledoc false
  use ExUnit.Case

  alias ExRocketmq.{Namesrvs, Transport}

  setup_all do
    configs = Application.get_all_env(:ex_rocketmq)

    {host, port} = configs[:namesrvs]
    %{group: _group, topic: topic} = configs[:consumer]

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
