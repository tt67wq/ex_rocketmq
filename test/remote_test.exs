defmodule RemoteTest do
  @moduledoc false
  use ExUnit.Case

  alias ExRocketmq.Protocol.Request
  alias ExRocketmq.Remote
  alias ExRocketmq.Remote.Packet
  alias ExRocketmq.Transport

  require Packet
  require Request

  @req_get_broker_cluster_info Request.req_get_broker_cluster_info()
  @timeout 5000

  setup_all do
    configs = Application.get_all_env(:ex_rocketmq)

    {host, port} = configs[:namesrvs]
    %{topic: topic} = configs[:consumer]

    r =
      start_supervised!({Remote, transport: Transport.Tcp.new(host: host, port: port, timeout: @timeout)})

    [remote: r, topic: topic]
  end

  test "rpc", %{remote: r} do
    assert {:ok, _} =
             Remote.rpc(
               r,
               Packet.packet(
                 code: @req_get_broker_cluster_info,
                 language: 8,
                 ext_fields: %{}
               )
             )
  end
end
