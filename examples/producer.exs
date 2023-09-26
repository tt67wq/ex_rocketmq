Mix.install([
  {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
])

defmodule DemoProducer do
  @moduledoc """
  producer task
  """

  use Task

  alias ExRocketmq.Producer
  alias ExRocketmq.Models.Message

  @topic "POETRY"

  def start_link(opts) do
    Task.start_link(__MODULE__, :run, [opts])
  end

  defp get_msg() do
    File.read!("tmp/poetry.txt")
    |> String.split("\n")
    |> Enum.random()
  end

  def run(opts) do
    Producer.send_sync(:producer, [
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()},
      %Message{topic: @topic, body: get_msg()}
    ])

    Process.sleep(1000)
    run(opts)
  end
end

alias ExRocketmq.{Producer, Namesrvs, Transport}

Supervisor.start_link(
  [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {
      Producer,
      group_name: "GID_WANQIANG",
      namesrvs: :namesrvs,
      opts: [
        name: :producer
      ]
    },
    {
      DemoProducer,
      []
    }
  ],
  strategy: :one_for_one
)

Process.sleep(:infinity)
