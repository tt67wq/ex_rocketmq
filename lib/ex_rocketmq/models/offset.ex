defmodule ExRocketmq.Models.QueryConsumerOffset do
  @moduledoc """
  query consumer offset model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct consumer_group: "",
            topic: "",
            queue_id: 0

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

defmodule ExRocketmq.Models.SearchOffset do
  @moduledoc """
  search offset model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct topic: "",
            queue_id: 0,
            timestamp: 0

  @type t :: %__MODULE__{
          topic: String.t(),
          queue_id: non_neg_integer(),
          timestamp: non_neg_integer()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "topic" => t.topic,
      "queueId" => "#{t.queue_id}",
      "timestamp" => "#{t.timestamp}"
    }
  end
end

defmodule ExRocketmq.Models.GetMaxOffset do
  @moduledoc """
  get max offset model
  """
  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct topic: "",
            queue_id: 0

  @type t :: %__MODULE__{
          topic: String.t(),
          queue_id: non_neg_integer()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "topic" => t.topic,
      "queueId" => "#{t.queue_id}"
    }
  end
end

defmodule ExRocketmq.Models.UpdateConsumerOffset do
  @moduledoc """
  update consumer offset model
  """

  alias ExRocketmq.{Remote.ExtFields}

  @behaviour ExtFields

  defstruct [
    :consumer_group,
    :topic,
    :queue_id,
    :commit_offset
  ]

  @type t :: %__MODULE__{
          consumer_group: String.t(),
          topic: String.t(),
          queue_id: non_neg_integer(),
          commit_offset: non_neg_integer()
        }

  @impl ExtFields
  def to_map(t) do
    %{
      "consumerGroup" => t.consumer_group,
      "topic" => t.topic,
      "queueId" => "#{t.queue_id}",
      "commitOffset" => "#{t.commit_offset}"
    }
  end
end
