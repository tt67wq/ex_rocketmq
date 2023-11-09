defmodule ExRocketmq.ProcessQueue.State do
  @moduledoc false

  alias ExRocketmq.{
    Typespecs,
    Consumer,
    Processor
  }

  alias ExRocketmq.Models.{
    BrokerData
  }

  defstruct client_id: "",
            group_name: "",
            topic: "",
            queue_id: 0,
            buff_manager: nil,
            buff: nil,
            broker_data: nil,
            processor: nil,
            consume_batch_size: 32,
            trace_enable: false,
            max_reconsume_times: 3

  @type t :: %__MODULE__{
          client_id: String.t(),
          group_name: Typespecs.group_name(),
          topic: Typespecs.topic(),
          queue_id: non_neg_integer(),
          buff_manager: Consumer.BuffManager.t(),
          buff: Consumer.Buff.t(),
          broker_data: BrokerData.t(),
          processor: Processor.t(),
          consume_batch_size: non_neg_integer(),
          trace_enable: boolean(),
          max_reconsume_times: non_neg_integer()
        }
end
