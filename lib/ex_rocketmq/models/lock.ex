defmodule ExRocketmq.Models.Lock do
  defmodule Req do
    @moduledoc """
    lock mq req
    """

    alias ExRocketmq.Models.{MessageQueue}

    defstruct consumer_group: "",
              client_id: "",
              mq: MapSet.new()

    @type t :: %__MODULE__{
            consumer_group: String.t(),
            client_id: String.t(),
            mq: MapSet.t(MessageQueue.t())
          }

    @spec to_map(t()) :: %{String.t() => any()}
    def to_map(t) do
      %{
        "consumerGroup" => t.consumer_group,
        "clientId" => t.client_id,
        "mqSet" => t.mq |> Enum.map(&MessageQueue.to_map/1)
      }
    end

    @spec encode(t()) :: binary()
    def encode(t) do
      t
      |> to_map()
      |> Jason.encode!()
    end
  end

  defmodule Resp do
    @moduledoc """
    lock mq resp
    """

    alias ExRocketmq.Models.{MessageQueue}

    defstruct [
      :mqs
    ]

    @type t :: %__MODULE__{
            mqs: [MessageQueue.t()]
          }

    @spec decode(binary()) :: t()
    def decode(body) do
      body
      |> Jason.decode!()
      |> Map.get("lockOKMQSet")
      |> Enum.map(fn %{"brokerName" => broker_name, "queueId" => queue_id, "topic" => topic} ->
        %MessageQueue{
          topic: topic,
          broker_name: broker_name,
          queue_id: queue_id
        }
      end)
      |> then(fn mqs ->
        %__MODULE__{
          mqs: mqs
        }
      end)
    end
  end
end
