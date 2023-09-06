defmodule ExRocketmq.Consumer.Processor do
  @moduledoc """
  The processor is called when messages are received from the broker.
  """

  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Typespecs

  @type t :: struct()

  @callback process(processort :: t(), topic :: Typespecs.topic(), msgs :: [MessageExt.t()]) ::
              :ok | {:error, term()}

  defp delegate(%module{} = m, func, args),
    do: apply(module, func, [m | args])

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) :: :ok | {:error, term()}
  def process(m, topic, msgs), do: delegate(m, :process, [topic, msgs])
end
