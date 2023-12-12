# ExRocketmq

ExRocketmq is an Elixir client for Apache RocketMQ, a distributed messaging and streaming platform. 
With ExRocketmq, you can easily produce and consume messages from RocketMQ clusters, as well as manage topics and subscriptions. 
It provides a simple and flexible API for working with RocketMQ, and is designed to be easy to use and integrate into your Elixir applications.

NOTE: **This project is still under development, and is not ready for production use yet.**


## Installation
ExRocketmq can be installed by adding :ex_rocketmq to your list of dependencies in mix.exs:
```Elixir
def deps do
  [
    {:ex_rocketmq, "~> 0.1.0"}
  ]
end
```

## Usage

### producer
```Elixir
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

  @msgs "豫章故郡，洪都新府。
星分翼轸，地接衡庐。
襟三江而带五湖，控蛮荆而引瓯越。
物华天宝，龙光射牛斗之墟；人杰地灵，徐孺下陈蕃之榻。
雄州雾列，俊采星驰。"
  def start_link(opts) do
    Task.start_link(__MODULE__, :run, [opts])
  end

  defp get_msg() do
    @msgs
    |> String.split("\n")
    |> Enum.random()
  end


  def run(opts) do
    get_msg()
    |> Enum.chunk_every(2)
    |> Enum.each(fn msgs ->
      to_emit =
        msgs
        |> Enum.map(fn msg ->
          %Message{topic: @topic, body: msg}
        end)

      Producer.send_sync(:producer, to_emit)
    end)

    Process.sleep(5000)
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
      group_name: "GID_POETRY",
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

```

### consumer
```Elixir
Mix.install([
  {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
])

alias ExRocketmq.{Consumer, Namesrvs, Transport, Models.MsgSelector}

defmodule MyProcessor do
  @moduledoc """
  A mock processor for testing.
  """

  alias ExRocketmq.{
    Consumer.Processor,
    Models.MessageExt,
    Typespecs
  }

  @behaviour Processor

  defstruct []

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
          Processor.consume_result() | {:error, term()}
  def process(_, topic, msgs) do
    msgs
    |> Enum.map(fn msg -> IO.puts("#{topic}: #{msg.queue_offset} -- #{msg.message.body}") end)

    :success
  end
end

Supervisor.start_link(
  [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {Consumer,
     consumer_group: "GID_POETRY",
     namesrvs: :namesrvs,
     processor: MyProcessor.new(),
     subscriptions: %{"POETRY" => MsgSelector.new(:tag, "*")},
     opts: [
       name: :consumer
     ]}
  ],
  strategy: :one_for_one
)

Process.sleep(:infinity)
```

More examples can be found in [examples](https://github.com/tt67wq/ex_rocketmq/tree/master/examples)

