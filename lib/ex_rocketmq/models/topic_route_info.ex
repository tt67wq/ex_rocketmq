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
          broker_addrs: map(),
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

defmodule ExRocketmq.Models.QueueData do
  @moduledoc """
  The queue data model of rocketmq
  %{
     "brokerName" => "broker-0",
     "perm" => 6,
     "readQueueNums" => 2,
     "topicSynFlag" => 0,
     "writeQueueNums" => 2
   }
  """
  @type t :: %__MODULE__{
          broker_name: String.t(),
          perm: integer(),
          read_queue_nums: integer(),
          topic_syn_flag: integer(),
          write_queue_nums: integer()
        }

  defstruct [:broker_name, :perm, :read_queue_nums, :topic_syn_flag, :write_queue_nums]

  def from_map(%{
        "brokerName" => broker_name,
        "perm" => perm,
        "readQueueNums" => read_queue_nums,
        "topicSynFlag" => topic_syn_flag,
        "writeQueueNums" => write_queue_nums
      }) do
    %__MODULE__{
      broker_name: broker_name,
      perm: perm,
      read_queue_nums: read_queue_nums,
      topic_syn_flag: topic_syn_flag,
      write_queue_nums: write_queue_nums
    }
  end
end
