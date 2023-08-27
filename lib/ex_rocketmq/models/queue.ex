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
end
