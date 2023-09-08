defmodule ExRocketmq.Models.MessageQueue do
  @moduledoc """
  The mq info model of producer
  """
  alias ExRocketmq.{Typespecs}

  defstruct topic: "",
            broker_name: "",
            queue_id: 0

  @type t :: %__MODULE__{
          topic: Typespecs.topic(),
          broker_name: Typespecs.broker_name(),
          queue_id: non_neg_integer()
        }

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(t) do
    %{
      "topic" => t.topic,
      "brokerName" => t.broker_name,
      "queueId" => t.queue_id
    }
  end

  @spec equal?(t(), t()) :: boolean()
  def equal?(a, b) do
    a.topic == b.topic &&
      a.broker_name == b.broker_name &&
      a.queue_id == b.queue_id
  end
end
