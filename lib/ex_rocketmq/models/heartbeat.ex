defmodule ExRocketmq.Models.Heartbeat do
  @moduledoc """
  heartbeat data send to broker
  """

  defstruct client_id: "",
            producer_data_set: MapSet.new(),
            consumer_data_set: MapSet.new()

  @type t :: %__MODULE__{
          client_id: String.t(),
          producer_data_set: MapSet.t(ExRocketmq.Models.ProducerData.t()),
          consumer_data_set: MapSet.t(ExRocketmq.Models.ConsumerData.t())
        }

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(h) do
    %{
      "clientID" => h.client_id,
      "producerDataSet" =>
        h.producer_data_set |> Enum.map(&ExRocketmq.Models.ProducerData.to_map/1),
      "consumerDataSet" =>
        h.consumer_data_set |> Enum.map(&ExRocketmq.Models.ConsumerData.to_map/1)
    }
  end

  @spec encode(t()) :: {:ok, binary()} | {:error, any()}
  def encode(heartbeat) do
    heartbeat
    |> to_map()
    |> Jason.encode()
  end
end

defmodule ExRocketmq.Models.ProducerData do
  @moduledoc false

  defstruct group: ""

  @type t :: %__MODULE__{
          group: String.t()
        }

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(t) do
    %{
      "groupName" => t.group
    }
  end
end

defmodule ExRocketmq.Models.ConsumerData do
  @moduledoc false

  defstruct [
    :group,
    :consume_type,
    :message_model,
    :consume_from_where,
    :subscription_data_set,
    :unit_mode
  ]

  @type t :: %__MODULE__{
          group: String.t(),
          consume_type: String.t(),
          message_model: String.t(),
          consume_from_where: String.t(),
          subscription_data_set: [ExRocketmq.Models.Subscription.t()],
          unit_mode: boolean()
        }

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(t) do
    %{
      "groupName" => t.group,
      "consumeType" => t.consume_type,
      "messageModel" => t.message_model,
      "consumeFromWhere" => t.consume_from_where,
      "subscriptionDataSet" =>
        t.subscription_data_set |> Enum.map(&ExRocketmq.Models.Subscription.to_map/1),
      "unitMode" => t.unit_mode
    }
  end
end
