Mix.install([
  # {:ex_rocketmq, github: "tt67wq/ex_rocketmq", branch: "master"}
  {:ex_rocketmq, path: "../ex_rocketmq"}
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
    |> Enum.reduce(%{}, fn msg, acc ->
      IO.puts("#{topic}: #{msg.queue_offset} -- #{msg.message.body}")

      if :random.uniform(10) < 2 do
        Map.put(acc, msg.msg_id, 1)
      else
        acc
      end
    end)
    |> case do
      %{} ->
        :success

      delay_level_map ->
        {:retry_later, delay_level_map}
    end
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
