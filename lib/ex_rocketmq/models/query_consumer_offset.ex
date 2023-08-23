defmodule ExRocketmq.Models.QueryConsumerOffset do
  @moduledoc """
  query consumer offset model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct [
    :consumer_group,
    :topic,
    :queue_id
  ]

  @type t :: %__MODULE__{
          consumer_group: String.t(),
          topic: String.t(),
          queue_id: non_neg_integer()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "consumerGroup" => t.consumer_group,
      "topic" => t.topic,
      "queueId" => "#{t.queue_id}"
    }
  end
end
