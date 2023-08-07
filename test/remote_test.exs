defmodule RemoteTest do
  use ExUnit.Case

  alias ExRocketmq.{Remote, Transport, Message, Protocol.Request}
  require Message

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "timeout" => timeout,
      "topic" => topic
    } =
      File.read!("./tmp/test.json") |> Jason.decode!()

    t = Transport.Tcp.new(host: host, port: port, timeout: timeout)
    r = ExRocketmq.Remote.new(name: :test_remote, transport: t)

    start_supervised!({Remote, remote: r})
    [remote: r, topic: topic]
  end

  test "rpc", %{remote: r, topic: topic} do
    assert {:ok, _res} =
             Remote.rpc(
               r,
               Message.message(
                 code: Request.req_get_routeinfo_by_topic(),
                 language: 8,
                 ext_fields: %{"topic" => topic}
               )
             )
  end
end
