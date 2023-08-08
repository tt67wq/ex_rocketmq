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

  @derive [
    {
      Nestru.PreDecoder,
      translate: %{"brokerDatas" => :broker_datas, "queueDatas" => :queue_datas}
    },
    {Nestru.Decoder, hint: %{broker_datas: [BrokerData], queue_datas: [QueueData]}}
  ]
  defstruct [:broker_datas, :queue_datas]
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

  @derive [
    {Nestru.PreDecoder,
     translate: %{
       "brokerAddrs" => :broker_addrs,
       "brokerName" => :broker_name
     }},
    Nestru.Decoder
  ]
  defstruct [:broker_addrs, :broker_name, :cluster]
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

  @derive [
    {Nestru.PreDecoder,
     translate: %{
       "brokerName" => :broker_name,
       "readQueueNums" => :read_queue_nums,
       "topicSynFlag" => :topic_syn_flag,
       "writeQueueNums" => :write_queue_nums
     }},
    Nestru.Decoder
  ]
  defstruct [:broker_name, :perm, :read_queue_nums, :topic_syn_flag, :write_queue_nums]
end
