# {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
alias ExRocketmq.Consumer
alias ExRocketmq.Models.MsgSelector
alias ExRocketmq.Namesrvs
alias ExRocketmq.Transport

Mix.install([
  {:ex_rocketmq, path: "../ex_rocketmq"}
])

defmodule MyProcessor do
  @moduledoc """
  A mock processor for testing.
  """

  @behaviour Processor

  alias ExRocketmq.Consumer.Processor
  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Typespecs

  defstruct []

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
          Processor.consume_result() | {:error, term()}
  def process(_, topic, msgs) do
    Enum.each(msgs, fn msg -> IO.puts("#{topic}: #{msg.queue_offset} -- #{msg.message.body}") end)
    :success
  end
end

Supervisor.start_link(
  [
    {Namesrvs,
     remotes: [
       [transport: Transport.Tcp.new(host: "test.rocket-mq.net", port: 31_120)]
     ],
     opts: [
       name: :namesrvs
     ]},
    {Consumer,
     consumer_group: "GID_POETRY",
     namesrvs: :namesrvs,
     processor: MyProcessor.new(),
     subscriptions: %{"POETRY" => MsgSelector.new(:tag, "*")},
     model: :broadcast,
     opts: [
       name: :consumer
     ]}
  ],
  strategy: :one_for_one
)

Process.sleep(:infinity)
