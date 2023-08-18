defmodule RemoteTest do
  use ExUnit.Case

  alias ExRocketmq.{Remote, Transport, Remote.Message, Protocol.Request}
  require Message
  require Request

  @req_get_broker_cluster_info Request.req_get_broker_cluster_info()

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "timeout" => timeout,
      "topic" => topic
    } =
      File.read!("./tmp/test.json") |> Jason.decode!()

    t = Transport.Tcp.new(host: host, port: port, timeout: timeout)
    r = ExRocketmq.Remote.new(name: :demo, transport: t)

    start_supervised!({Remote, remote: r})
    [remote: r, topic: topic]
  end

  test "rpc", %{remote: r, topic: topic} do
    assert {:ok, _} =
             Remote.rpc(
               r,
               Message.message(
                 code: @req_get_broker_cluster_info,
                 language: 8,
                 ext_fields: %{}
               )
             )
  end
end
