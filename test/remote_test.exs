defmodule RemoteTest do
  use ExUnit.Case

  alias ExRocketmq.{Remote, Transport, Remote.Packet, Protocol.Request}
  require Packet
  require Request

  @req_get_broker_cluster_info Request.req_get_broker_cluster_info()
  @timeout 5000

  setup_all do
    configs = Application.get_all_env(:ex_rocketmq)

    {host, port} = configs[:namesrvs]
    %{group: group, topic: topic} = configs[:consumer]

    r =
      start_supervised!(
        {Remote, transport: Transport.Tcp.new(host: host, port: port, timeout: @timeout)}
      )

    [remote: r, topic: topic]
  end

  test "rpc", %{remote: r, topic: topic} do
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
