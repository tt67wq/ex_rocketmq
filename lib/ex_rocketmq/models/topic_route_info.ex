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

  defstruct broker_datas: [], queue_datas: []

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
  alias ExRocketmq.Typespecs

  @type t :: %__MODULE__{
          broker_addrs: %{String.t() => String.t()},
          broker_name: Typespecs.broker_name(),
          cluster: String.t()
        }

  defstruct broker_addrs: %{}, broker_name: "", cluster: ""

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

  alias ExRocketmq.{Typespecs, Models.MessageQueue}

  @type t :: %__MODULE__{
          broker_name: String.t(),
          perm: integer(),
          read_queue_nums: integer(),
          topic_syn_flag: integer(),
          write_queue_nums: integer()
        }

  defstruct broker_name: "", perm: 0, read_queue_nums: 0, topic_syn_flag: 0, write_queue_nums: 0

  # @perm_priority Bitwise.bsl(1, 3)
  @perm_read Bitwise.bsl(1, 2)
  @perm_write Bitwise.bsl(1, 1)

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

  @spec writeable?(t()) :: boolean()
  defp writeable?(queue) do
    queue.perm
    |> Bitwise.&&&(@perm_write)
    |> Kernel.==(@perm_write)
  end

  @spec readable?(t()) :: boolean()
  def readable?(queue) do
    queue.perm
    |> Bitwise.&&&(@perm_read)
    |> Kernel.==(@perm_read)
  end

  @spec to_publish_queues(t(), Typespecs.topic()) :: list(MessageQueue.t())
  def to_publish_queues(queue, topic) do
    queue
    |> writeable?()
    |> if do
      0..(queue.write_queue_nums - 1)
      |> Enum.map(fn queue_id ->
        %MessageQueue{
          topic: topic,
          broker_name: queue.broker_name,
          queue_id: queue_id
        }
      end)
    else
      []
    end
  end

  @spec to_consume_queues(t(), Typespecs.topic()) :: list(MessageQueue.t())
  def to_consume_queues(queue, topic) do
    queue
    |> readable?()
    |> if do
      0..(queue.read_queue_nums - 1)
      |> Enum.map(fn queue_id ->
        %MessageQueue{
          topic: topic,
          broker_name: queue.broker_name,
          queue_id: queue_id
        }
      end)
    else
      []
    end
  end
end
