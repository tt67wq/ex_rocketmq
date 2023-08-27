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

  # @perm_priority Bitwise.bsl(1, 3)
  @perm_read Bitwise.bsl(1, 2)
  @perm_write Bitwise.bsl(1, 1)

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

  @spec writeable?(t()) :: boolean()
  def writeable?(queue) do
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
end

defmodule ExRocketmq.Models.MessageQueue do
  @moduledoc """
  The mq info model of producer
  """
  alias ExRocketmq.{Typespecs, Models}

  defstruct [
    :topic,
    :broker_name,
    :queue_id
  ]

  @type t :: %__MODULE__{
          topic: Typespecs.topic(),
          broker_name: String.t(),
          queue_id: non_neg_integer()
        }

  @spec from_queue_data(Models.QueueData.t(), Typespecs.topic()) :: [t()]
  def from_queue_data(queue_data, topic) do
    queue_data
    |> Models.QueueData.writeable?()
    |> if do
      0..(queue_data.write_queue_nums - 1)
      |> Enum.map(fn queue_id ->
        %__MODULE__{
          topic: topic,
          broker_name: queue_data.broker_name,
          queue_id: queue_id
        }
      end)
    else
      []
    end
  end

  @spec to_map(t()) :: %{String.t() => any()}
  def to_map(t) do
    %{
      "topic" => t.topic,
      "brokerName" => t.broker_name,
      "queueId" => t.queue_id
    }
  end
end
