defmodule ExRocketmq.Consumer.MockProcessor do
  @moduledoc false
  @behaviour ExRocketmq.Consumer.Processor

  alias ExRocketmq.Consumer.Processor
  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Typespecs

  defstruct []

  @type t :: %__MODULE__{}

  @spec new() :: t()
  def new, do: %__MODULE__{}

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) ::
          Processor.consume_result() | {:error, term()}
  def process(_, _topic, msgs) do
    Enum.each(msgs, fn msg -> IO.inspect(msg) end)
    :success
  end
end
