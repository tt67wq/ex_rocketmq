defmodule ExRocketmq.Consumer.MockProcessor do
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
  def process(_, _topic, msgs) do
    msgs
    |> Enum.each(fn msg ->
      IO.inspect(msg)
    end)

    :success
  end
end
