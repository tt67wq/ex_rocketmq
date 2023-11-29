defmodule ExRocketmq.Consumer.Processor do
  @moduledoc """
  The processor is called when messages are received from the broker.
  """

  alias ExRocketmq.Models.MessageExt
  alias ExRocketmq.Typespecs

  @type t :: struct()

  @typedoc """
  concurrent consume result must be one of:
    - :success
    - {:retry_later, %{msg_id => delay_level}}

  rocketmq provide 18 delay_level: 1s 5s 10s 30s 1m 2m 3m 4m 5m 6m 7m 8m 9m 10m 20m 30m 1h 2h
  delay_level=1 means 1s, delay_level=2 means 5s and so on.
  """
  @type concurrent_consume_result ::
          :success | {:retry_later, %{Typespecs.msg_id() => Typespecs.delay_level()}}
  @typedoc """
  orderly consume result must be one of:
    - :success
    - {:suspend, suspend_milliseconds, [msg_id]}
  """
  @type orderly_consume_result ::
          :success | {:suspend, non_neg_integer(), [Typespecs.msg_id()]}

  @type consume_result :: concurrent_consume_result() | orderly_consume_result()

  @callback process(processort :: t(), topic :: Typespecs.topic(), msgs :: [MessageExt.t()]) ::
              consume_result() | {:error, term()}

  defp delegate(%module{} = m, func, args), do: apply(module, func, [m | args])

  @spec process(t(), Typespecs.topic(), [MessageExt.t()]) :: consume_result() | {:error, term()}
  def process(m, topic, msgs), do: delegate(m, :process, [topic, msgs])
end
