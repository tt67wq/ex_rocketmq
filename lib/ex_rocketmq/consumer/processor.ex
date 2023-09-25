defmodule ExRocketmq.Consumer.Processor do
  @moduledoc """
  The processor is called when messages are received from the broker.
  """

  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Typespecs

  @type t :: struct()
  @type concurrent_consume_result ::
          :success | {:retry_later, Typespecs.delay_level()}
  @type orderly_consume_result :: :commit | :rollback | :suspend

  @type consume_result :: concurrent_consume_result() | orderly_consume_result()

  @callback process(processort :: t(), topic :: Typespecs.topic(), msgs :: [MessageExt.t()]) ::
              consume_result() | {:error, term()}

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) :: consume_result() | {:error, term()}
  def process(m, topic, msgs), do: delegate(m, :process, [topic, msgs])
end

defmodule ExRocketmq.Consumer.MockProcessor do
  @moduledoc """
  A mock processor for testing.
  """

  alias ExRocketmq.{
    Consumer.Processor,
    Models.MessageExt
  }

  @behaviour Processor

  defstruct []

  @type t :: %__MODULE__{}

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
          Processor.consume_result() | {:error, term()}
  def process(_, topic, msgs) do
    msgs
    |> Enum.map(fn msg -> IO.puts("#{topic}: #{msg.queue_offset} -- #{msg.message.body}") end)

    :success
  end
end
