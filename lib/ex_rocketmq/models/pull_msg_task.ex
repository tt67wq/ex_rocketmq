defmodule ExRocketmq.Models.ConsumeState do
  @moduledoc false

  alias ExRocketmq.Typespecs

  alias ExRocketmq.Models.{
    BrokerData,
    MessageQueue,
    Subscription
  }

  defstruct task_id: "",
            client_id: "",
            topic: "",
            group_name: "",
            registry: nil,
            broker_dynamic_supervisor: nil,
            broker_data: nil,
            mq: nil,
            consume_from_where: :last_offset,
            consume_timestamp: 0,
            subscription: nil,
            next_offset: 0,
            commit_offset_enable: true,
            commit_offset: 0,
            post_subscription_when_pull: false,
            pull_batch_size: 32,
            consume_batch_size: 16,
            processor: nil,
            lock_ttl_ms: 0,
            max_reconsume_times: 16

  @type t :: %__MODULE__{
          task_id: String.t(),
          client_id: String.t(),
          topic: Typespecs.topic(),
          group_name: Typespecs.group_name(),
          registry: atom(),
          broker_dynamic_supervisor: pid(),
          broker_data: BrokerData.t(),
          mq: MessageQueue.t(),
          consume_from_where: :last_offset | :first_offset | :timestamp,
          consume_timestamp: non_neg_integer(),
          subscription: Subscription.t(),
          next_offset: integer(),
          commit_offset_enable: boolean(),
          commit_offset: non_neg_integer(),
          post_subscription_when_pull: boolean(),
          pull_batch_size: non_neg_integer(),
          consume_batch_size: non_neg_integer(),
          processor: ExRocketmq.Consumer.Processor.t(),
          lock_ttl_ms: integer(),
          max_reconsume_times: integer()
        }
end
