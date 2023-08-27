defmodule ExRocketmq.Models.TopicRouteInfo do
  @moduledoc """
  The topic route info model of rocketmq

  %{
     "brokerDatas" => [
       %{
         "brokerAddrs" => %{"0" => "10.88.4.78:20911", "1" => "10.88.4.144:20911"},
         "brokerName" => "broker-0",
         "cluster" => "d2"
       }
     ],
     "queueDatas" => [
       %{
         "brokerName" => "broker-0",
         "perm" => 6,
         "readQueueNums" => 2,
         "topicSynFlag" => 0,
         "writeQueueNums" => 2
       }
     ]
   }
  """
  alias ExRocketmq.Models.{BrokerData, QueueData}

  @type t :: %__MODULE__{
          broker_datas: [BrokerData.t()],
          queue_datas: [QueueData.t()]
        }

  defstruct [:broker_datas, :queue_datas]

  @spec from_json(String.t()) :: t()
  def from_json(json) do
    json
    |> Jason.decode!()
    |> from_map()
  end

  def from_map(%{"brokerDatas" => broker_datas, "queueDatas" => queue_datas}) do
    %__MODULE__{
      broker_datas: broker_datas |> Enum.map(&BrokerData.from_map/1),
      queue_datas: queue_datas |> Enum.map(&QueueData.from_map/1)
    }
  end
end

defmodule ExRocketmq.Models.BrokerData do
  @moduledoc """
  The broker data model of rocketmq

  %{
     "brokerAddrs" => %{"0" => "10.88.4.78:20911", "1" => "10.88.4.144:20911"},
     "brokerName" => "broker-0",
     "cluster" => "d2"
   }
  """
  @type t :: %__MODULE__{
          broker_addrs: %{String.t() => String.t()},
          broker_name: String.t(),
          cluster: String.t()
        }

  defstruct [:broker_addrs, :broker_name, :cluster]

  def from_map(%{"brokerAddrs" => broker_addrs, "brokerName" => broker_name, "cluster" => cluster}) do
    %__MODULE__{
      broker_addrs: broker_addrs,
      broker_name: broker_name,
      cluster: cluster
    }
  end
end
