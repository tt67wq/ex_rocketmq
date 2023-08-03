defmodule TransportTest do
  use ExUnit.Case

  alias ExRocketmq.Transport.Tcp

  setup_all do
    %{
      "host" => host,
      "port" => port,
      "timeout" => timeout
    } =
      File.read!("./tmp/test.json")
      |> Jason.decode!()

    t = Tcp.new(host: host, port: port, timeout: timeout)

    start_supervised!({Tcp, transport: t})
    [transport: t]
  end
end
