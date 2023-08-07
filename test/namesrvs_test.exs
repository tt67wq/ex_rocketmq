defmodule NamesrvsTest do
  use ExUnit.Case

  alias ExRocketmq.{Namesrvs, Transport, Message}
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
    namesrvs = ExRocketmq.Namesrvs.new(remote: r)

    start_supervised!({Namesrvs, namesrvs: namesrvs})
    [namesrvs: namesrvs, topic: topic]
  end

  test "query_topic_route_info", %{namesrvs: namesrvs, topic: topic} do
    assert {:ok, _res} = Namesrvs.query_topic_route_info(namesrvs, topic)
  end
end
