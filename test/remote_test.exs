defmodule RemoteTest do
  use ExUnit.Case

  alias ExRocketmq.{Remote, Transport}

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "timeout" => timeout
    } =
      File.read!("./tmp/test.json") |> Jason.decode!()

    t = Transport.Tcp.new(host: host, port: port, timeout: timeout)
    r = ExRocketmq.Remote.new(name: :test_remote, transport: t)

    start_supervised!({Remote, remote: r})
    [remote: r]
  end
end
